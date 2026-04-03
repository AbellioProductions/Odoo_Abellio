import threading
import odoo
from datetime import datetime, timedelta, time
import pytz
import logging
from odoo import models, fields, api
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

class MesOperationAssignWizard(models.TransientModel):
    _name = 'mes.operation.assign.wizard'
    _description = 'Operation Assign Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    workcenter_id = fields.Many2one('mrp.workcenter')
    machine_id = fields.Many2one(related='workcenter_id.machine_settings_id')
    
    report_id = fields.Many2one(
        'mes.production.report', 
        domain="[('machine_id', '=', machine_id)]",
        required=True
    )

    def action_confirm(self):
        self.operation_id.write({
            'op_type': 'job',
            'report_id': self.report_id.id,
            'job_number': self.report_id.name
        })


class MesOperationSplitWizard(models.TransientModel):
    _name = 'mes.operation.split.wizard'
    _description = 'Operation Split Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    split_dt = fields.Datetime(required=True)

    @api.constrains('split_dt')
    def _check_split_dt(self):
        for rec in self:
            op = rec.operation_id
            if rec.split_dt <= op.start_dt or (op.end_dt and rec.split_dt >= op.end_dt):
                raise ValidationError("Split time must be within the operation interval.")

    def action_confirm(self):
        op = self.operation_id
        op.copy({
            'start_dt': self.split_dt,
            'end_dt': op.end_dt,
            'op_type': op.op_type,
            'report_id': op.report_id.id,
            'job_number': op.job_number
        })
        op.end_dt = self.split_dt


class MesHistPerformanceWiz(models.TransientModel):
    _name = 'mes.hist.performance.wiz'
    _description = 'Historical Shift Generator'

    start_date = fields.Datetime(string='Start DateTime', required=True)
    end_date = fields.Datetime(string='End DateTime', required=True, default=fields.Datetime.now)
    machine_ids = fields.Many2many('mrp.workcenter', string='Machines', required=True)

    def action_generate(self):
        db_name = self.env.cr.dbname
        uid = self.env.uid
        context = self.env.context.copy()
        
        s_date = self.start_date
        e_date = self.end_date
        mac_ids = self.machine_ids.ids

        thread = threading.Thread(
            target=self._run_in_background,
            args=(db_name, uid, context, s_date, e_date, mac_ids)
        )
        thread.start()

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Background Process Started',
                'message': 'Historical shift generation is running in the background. Documents will appear gradually. You can close this window.',
                'sticky': False,
                'type': 'success',
            }
        }

    @api.model
    def _run_in_background(self, db_name, uid, context, start_date, end_date, machine_ids):
        registry = odoo.registry(db_name)
        with registry.cursor() as cr:
            env = api.Environment(cr, uid, context)
            now_utc = fields.Datetime.now()
            
            try:
                q = []
                wcs = env['mrp.workcenter'].browse(machine_ids)
                
                for wc in wcs:
                    shifts = env['mes.shift'].search([('company_id', '=', wc.company_id.id)])
                    val_shifts = shifts.filtered(lambda s: not s.workcenter_ids or wc.id in s.workcenter_ids.ids)
                    
                    curr_t = start_date
                    while curr_t <= end_date:
                        for shift in val_shifts:
                            s_loc, e_loc = self._calc_window(shift, curr_t.date())
                            s_utc = self._get_utc(wc, s_loc)
                            e_utc = self._get_utc(wc, e_loc)

                            if start_date <= s_utc <= end_date:
                                q.append({
                                    'wc_id': wc.id,
                                    'shift_id': shift.id,
                                    'tgt_date': curr_t.date(),
                                    's_utc': s_utc,
                                    'e_utc': e_utc,
                                    's_loc': s_loc,
                                    'e_loc': e_loc
                                })
                        curr_t += timedelta(days=1)
                        
                q.sort(key=lambda x: x['s_utc'])

                for item in q:
                    self._process_single_shift(env, item, now_utc)
                    cr.commit()
                    env.clear()
                
            except Exception as e:
                cr.rollback()
                _logger.error(f"Failed to process historical shifts: {str(e)}")

    @api.model
    def _process_single_shift(self, env, item, now_utc):
        wc = env['mrp.workcenter'].browse(item['wc_id'])
        shift = env['mes.shift'].browse(item['shift_id'])
        
        s_utc = item['s_utc']
        e_utc = item['e_utc']
        s_loc = item['s_loc']
        e_loc = item['e_loc']
        
        tz_name = wc.company_id.tz or 'UTC'
        now_mac = pytz.utc.localize(now_utc).astimezone(pytz.timezone(tz_name)).replace(tzinfo=None)
        
        is_cur = s_utc <= now_utc < e_utc
        calc_e_utc = now_utc if is_cur else e_utc
        calc_e_loc = now_mac if is_cur else e_loc

        doc = self._prepare_doc(env, wc, shift, item['tgt_date'])
        doc._init_initial_state(s_loc, s_utc)
        doc._process_historical_events(s_loc, calc_e_loc)
        
        if not is_cur:
            doc._close_open_events(calc_e_utc)
            doc._process_telemetry_counts(s_loc, e_loc)
            if doc._is_empty_shift():
                doc.unlink()
            else:
                doc.write({'state': 'done'})

    @api.model
    def _calc_window(self, shift, target_date):
        s_t = datetime.combine(
            target_date,
            time(hour=int(shift.start_hour), minute=int((shift.start_hour % 1) * 60))
        )
        e_t = s_t + timedelta(hours=shift.duration)
        return s_t, e_t

    @api.model
    def _get_utc(self, wc, loc_naive):
        tz_name = wc.company_id.tz or 'UTC'
        tz = pytz.timezone(tz_name)
        loc_dt = tz.localize(loc_naive, is_dst=False)
        return loc_dt.astimezone(pytz.utc).replace(tzinfo=None)

    @api.model
    def _prepare_doc(self, env, wc, shift, target_date):
        domain = [
            ('machine_id', '=', wc.id),
            ('shift_id', '=', shift.id),
            ('date', '=', target_date)
        ]
        doc = env['mes.machine.performance'].search(domain, limit=1)
        
        if not doc:
            doc = env['mes.machine.performance'].create({
                'machine_id': wc.id,
                'shift_id': shift.id,
                'date': target_date
            })
        else:
            doc.alarm_ids.unlink()
            doc.running_ids.unlink()
            doc.slowing_ids.unlink()
            doc.production_ids.unlink()
            doc.rejection_ids.unlink()
            doc.write({'state': 'draft'})
            
        return doc


class MesRecalcDowntimeWiz(models.TransientModel):
    _name = 'mes.recalc.downtime.wiz'
    _description = 'Recalculate Downtimes from Telemetry'

    start_date = fields.Datetime(string='Start DateTime', required=True)
    end_date = fields.Datetime(string='End DateTime', required=True, default=fields.Datetime.now)
    machine_ids = fields.Many2many('mrp.workcenter', string='Machines', required=True)

    def action_recalc(self):
        db_name = self.env.cr.dbname
        uid = self.env.uid
        context = self.env.context.copy()
        
        s_date = self.start_date
        e_date = self.end_date
        mac_ids = self.machine_ids.ids

        thread = threading.Thread(
            target=self._run_in_background,
            args=(db_name, uid, context, s_date, e_date, mac_ids)
        )
        thread.start()

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Recalculation Started',
                'message': 'Downtime recalculation is running in the background. Alarms in existing documents will be replaced.',
                'sticky': False,
                'type': 'success',
            }
        }

    @api.model
    def _run_in_background(self, db_name, uid, context, start_date, end_date, machine_ids):
        registry = odoo.registry(db_name)
        with registry.cursor() as cr:
            env = api.Environment(cr, uid, context)
            try:
                wcs = env['mrp.workcenter'].browse(machine_ids)
                
                docs = env['mes.machine.performance'].search([
                    ('machine_id', 'in', wcs.ids),
                    ('date', '>=', start_date.date() - timedelta(days=1)),
                    ('date', '<=', end_date.date() + timedelta(days=1))
                ])
                
                valid_docs = []
                for doc in docs:
                    s_loc, e_loc = doc._get_local_shift_times()
                    s_utc = doc._get_utc_time(s_loc)
                    if start_date <= s_utc <= end_date:
                        valid_docs.append(doc)
                        
                valid_docs.sort(key=lambda d: d.date)
                ts_base = env['mes.timescale.base']
                
                for doc in valid_docs:
                    mac = doc.machine_id.machine_settings_id
                    if not mac:
                        continue
                        
                    s_loc, e_loc = doc._get_local_shift_times()
                    
                    tz_name = doc.company_id.tz or 'UTC'
                    mac_tz = pytz.timezone(tz_name)
                    now_mac = pytz.utc.localize(fields.Datetime.now()).astimezone(mac_tz).replace(tzinfo=None)
                    calc_e_loc = min(e_loc, now_mac)

                    reason_tag = mac.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')

                    with ts_base._connection() as conn:
                        with conn.cursor() as ts_cur:
                            ts_cur.execute("""
                                SELECT time, tag_name, value 
                                FROM telemetry_event 
                                WHERE machine_name = %s AND time >= %s AND time <= %s 
                                ORDER BY time ASC
                            """, (mac.name, s_loc, calc_e_loc))
                            rows = ts_cur.fetchall()

                    doc.alarm_ids.unlink()
                    alarms_to_create = []

                    for i in range(len(rows)):
                        ts_cl, tag, val = rows[i]
                        
                        if tag == reason_tag:
                            if val is None:
                                continue
                            
                            val_str = str(val).strip()
                            if not val_str:
                                continue
                                
                            try:
                                v_int = int(float(val_str))
                            except ValueError:
                                continue

                            if v_int != 0:
                                end_cl = rows[i+1][0] if i + 1 < len(rows) else calc_e_loc
                                
                                if hasattr(ts_cl, 'replace') and ts_cl.tzinfo:
                                    ts_cl = ts_cl.replace(tzinfo=None)
                                if hasattr(end_cl, 'replace') and end_cl.tzinfo:
                                    end_cl = end_cl.replace(tzinfo=None)
                                
                                ts_utc = doc._get_utc_time(ts_cl)
                                end_utc = doc._get_utc_time(end_cl)
                                
                                evt = doc._resolve_evt(mac, tag, v_int)
                                
                                if evt:
                                    alarms_to_create.append({
                                        'performance_id': doc.id,
                                        'loss_id': evt.id,
                                        'start_time': ts_utc,
                                        'end_time': end_utc,
                                    })
                            
                    if alarms_to_create:
                        env['mes.performance.alarm'].create(alarms_to_create)
                        
                    cr.commit()
                    env.clear()
                    
                
            except Exception as e:
                cr.rollback()
                _logger.error(f"!!! FATAL ERROR in _run_in_background: {str(e)}", exc_info=True)
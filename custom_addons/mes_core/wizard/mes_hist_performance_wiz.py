import threading
import odoo
from datetime import datetime, timedelta, time
import pytz
import logging
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

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
                    
                _logger.info("Historical shift generation completed successfully.")
                
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
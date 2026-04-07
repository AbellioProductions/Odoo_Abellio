import threading
import odoo
import pytz
import logging
import traceback
from datetime import datetime, timedelta, time
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
        
        _logger.info("WIZARD_INIT: Starting thread for %s machines", len(self.machine_ids))
        
        thread = threading.Thread(
            target=self._run_in_background,
            args=(db_name, uid, context, self.start_date, self.end_date, self.machine_ids.ids)
        )
        thread.start()

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Background Process Started',
                'message': 'Check server logs for WIZARD tags to monitor progress.',
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
                queue = []
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
                                queue.append({
                                    'wc_id': wc.id,
                                    'shift_id': shift.id,
                                    'tgt_date': curr_t.date(),
                                    's_utc': s_utc, 'e_utc': e_utc,
                                    's_loc': s_loc, 'e_loc': e_loc
                                })
                        curr_t += timedelta(days=1)
                        
                queue.sort(key=lambda x: x['s_utc'])
                
                _logger.info("WIZARD_QUEUE: Built queue with %s shift windows to process", len(queue))

                for item in queue:
                    self._process_single_shift_fsm(env, item, now_utc)
                    cr.commit() 
                    env.clear()  
                    
                _logger.info("WIZARD_COMPLETE: All items processed successfully")
                    
            except Exception as e:
                cr.rollback()
                error_msg = f"WIZARD_FAULT: Critical failure in background thread!\nError: {str(e)}\n{traceback.format_exc()}"
                _logger.error(error_msg)

    @api.model
    def _process_single_shift_fsm(self, env, item, now_utc):
        wc = env['mrp.workcenter'].browse(item['wc_id'])
        shift = env['mes.shift'].browse(item['shift_id'])
        
        _logger.info("WIZARD_DOC_START: WC=%s | Shift=%s | Date=%s", wc.name, shift.name, item['tgt_date'])
        
        s_utc, e_utc = item['s_utc'], item['e_utc']
        s_loc, e_loc = item['s_loc'], item['e_loc']
        
        is_cur = s_utc <= now_utc < e_utc
        calc_e_utc = now_utc if is_cur else e_utc
        calc_e_loc = self._get_local(wc, calc_e_utc)

        doc = self._prepare_doc(env, wc, shift, item['tgt_date'])
        _logger.info("WIZARD_DOC_PREP: Doc ID=%s created/found", doc.id)

        perf_model = env['mes.machine.performance']
        mac = wc.machine_settings_id

        with env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT time, tag_name, value 
                    FROM telemetry_event 
                    WHERE machine_name = %s AND time <= %s 
                    ORDER BY time DESC LIMIT 1
                """, (mac.name, s_loc.strftime('%Y-%m-%d %H:%M:%S.%f')))
                baseline = cur.fetchone()

                cur.execute("""
                    SELECT time, tag_name, value 
                    FROM telemetry_event 
                    WHERE machine_name = %s AND time > %s AND time <= %s 
                    ORDER BY time ASC
                """, (mac.name, s_loc.strftime('%Y-%m-%d %H:%M:%S.%f'), calc_e_loc.strftime('%Y-%m-%d %H:%M:%S.%f')))
                events = cur.fetchall()

        _logger.info("WIZARD_FETCH: Found %s events for window %s -> %s", len(events), s_loc, calc_e_loc)

        active_state = None

        if baseline:
            _, tag, val = baseline
            tgt_model, evt_id = perf_model.classify_fsm_transition(wc, tag, val)
            if tgt_model and evt_id:
                active_state = env[tgt_model].create({
                    'performance_id': doc.id,
                    'loss_id': evt_id,
                    'start_time': s_utc
                })

        valid_events = 0
        for row in events:
            ts_raw, tag, val = row
            evt_utc = self._get_utc(wc, ts_raw)

            tgt_model, evt_id = perf_model.classify_fsm_transition(wc, tag, val)
            if not tgt_model or not evt_id:
                continue

            valid_events += 1

            if active_state and active_state._name == tgt_model and active_state.loss_id.id == evt_id:
                continue

            if active_state:
                active_state.write({'end_time': evt_utc})

            active_state = env[tgt_model].create({
                'performance_id': doc.id,
                'loss_id': evt_id,
                'start_time': evt_utc
            })

        _logger.info("WIZARD_FSM: Processed %s valid state transitions", valid_events)

        if not is_cur:
            if active_state:
                active_state.write({'end_time': calc_e_utc})
            
            self._process_shift_counts(env, doc, wc, s_loc, e_loc)

            # ВРЕМЕННО ОТКЛЮЧАЕМ УДАЛЕНИЕ ПУСТЫХ ДОКУМЕНТОВ ДЛЯ ДЕБАГА
            doc.write({'state': 'done'})
            _logger.info("WIZARD_SAVE: Doc %s locked successfully (Unlink disabled)", doc.id)

    def _process_shift_counts(self, env, doc, wc, s_loc, e_loc):
        mac = wc.machine_settings_id
        with env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                raw_counts = mac._fetch_waste_stats_raw(cur, s_loc, e_loc)
                
        prod_vals, rej_vals = [], []
        prod_count_id = wc.production_count_id
        
        for ct in mac.count_tag_ids:
            if not ct.count_id:
                continue
                
            tag_data = raw_counts.get(ct.tag_name, {'sum': 0.0, 'cum': 0.0})
            qty = tag_data.get('cum') if ct.is_cumulative else tag_data.get('sum')
            
            if qty <= 0:
                continue
                
            val_dict = {'performance_id': doc.id, 'qty': qty, 'reason_id': ct.count_id.id}
            
            if prod_count_id and ct.count_id.id == prod_count_id.id:
                prod_vals.append(val_dict)
            else:
                rej_vals.append(val_dict)
                
        if prod_vals:
            env['mes.performance.production'].create(prod_vals)
        if rej_vals:
            env['mes.performance.rejection'].create(rej_vals)

    def _is_empty_doc(self, doc):
        r_sum = sum(doc.running_ids.mapped('duration'))
        p_sum = sum(doc.production_ids.mapped('qty'))
        return r_sum <= 0 and p_sum <= 0

    @api.model
    def _calc_window(self, shift, target_date):
        s_t = datetime.combine(target_date, time(hour=int(shift.start_hour), minute=int((shift.start_hour % 1) * 60)))
        e_t = s_t + timedelta(hours=shift.duration)
        return s_t, e_t

    @api.model
    def _get_utc(self, wc, loc_val):
        if not loc_val: return False
        if isinstance(loc_val, str):
            dt_naive = fields.Datetime.to_datetime(loc_val.replace('T', ' ').replace('Z', '')[:19])
        else:
            dt_naive = loc_val.replace(tzinfo=None)
            
        tz = pytz.timezone(wc.company_id.tz or 'UTC')
        return tz.localize(dt_naive, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

    @api.model
    def _get_local(self, wc, utc_val):
        if not utc_val: return False
        if isinstance(utc_val, str):
            dt_naive = fields.Datetime.to_datetime(utc_val.replace('T', ' ').replace('Z', '')[:19])
        else:
            dt_naive = utc_val.replace(tzinfo=None)
            
        tz = pytz.timezone(wc.company_id.tz or 'UTC')
        return pytz.utc.localize(dt_naive).astimezone(tz).replace(tzinfo=None)

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
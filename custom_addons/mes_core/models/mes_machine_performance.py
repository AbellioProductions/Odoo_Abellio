import pytz
import logging
from datetime import datetime, timedelta, time
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

class MesMachinePerformance(models.Model):
    _name = 'mes.machine.performance'
    _description = 'Machine Performance Data (OEE)'
    _order = 'date desc, shift_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Doc ID', default='New', readonly=True, copy=False)
    date = fields.Date(string='Date', required=True, default=fields.Date.context_today)
    shift_id = fields.Many2one('mes.shift', string='Shift', required=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    state = fields.Selection([('draft', 'Draft'), ('done', 'Locked')], string='Status', default='draft', tracking=True)

    alarm_ids = fields.One2many('mes.performance.alarm', 'performance_id', string='Alarms')
    running_ids = fields.One2many('mes.performance.running', 'performance_id', string='Running Logs')
    slowing_ids = fields.One2many('mes.performance.slowing', 'performance_id', string='Slowing Logs')
    rejection_ids = fields.One2many('mes.performance.rejection', 'performance_id', string='Rejections')
    production_ids = fields.One2many('mes.performance.production', 'performance_id', string='Production Output')

    _sql_constraints = [
        ('uniq_report', 'unique(machine_id, date, shift_id)', 'Report for this shift already exists!')
    ]

    @api.model_create_multi
    def create(self, vals_list):
        machine_ids = [v.get('machine_id') for v in vals_list if v.get('machine_id')]
        machines = {m.id: m.name for m in self.env['mrp.workcenter'].browse(machine_ids)}

        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                date = vals.get('date')
                machine_id = vals.get('machine_id')
                machine_name = machines.get(machine_id, str(machine_id))
                vals['name'] = f"PERF/{date}/{machine_name}"
        
        return super().create(vals_list)

    @api.model
    def cron_process_pending_events(self):
        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', '!=', False)])
        for wc in workcenters:
            try:
                self._sync_machine_fsm(wc)
            except Exception as e:
                _logger.error("CRON FSM FAULT | WC: %s | Err: %s", wc.name, str(e))

    def _sync_machine_fsm(self, wc):
        self.env.flush_all()
        open_states = []
        
        for model in ['mes.performance.running', 'mes.performance.alarm', 'mes.performance.slowing']:
            records = self.env[model].search([
                ('performance_id.machine_id', '=', wc.id),
                ('end_time', '=', False)
            ])
            open_states.extend(records)

        active_state = None
        if open_states:
            open_states.sort(key=lambda x: x.start_time, reverse=True)
            active_state = open_states[0]
            
            for orphan in open_states[1:]:
                orphan.write({'end_time': active_state.start_time})

        last_utc_ts = active_state.start_time if active_state else (fields.Datetime.now() - timedelta(days=1))
        
        tz_name = wc.company_id.tz or 'UTC'
        local_tz = pytz.timezone(tz_name)
        last_local_ts = pytz.utc.localize(last_utc_ts).astimezone(local_tz).replace(tzinfo=None)
        
        mac = wc.machine_settings_id
        
        last_reason_val = 0
        if wc.telemetry_state_logic == 'states':
            with self.env['mes.timescale.base']._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT value FROM telemetry_event 
                        WHERE machine_name = %s AND tag_name = 'OEE.nStopRootReason' AND time <= %s
                        ORDER BY time DESC LIMIT 1
                    """, (mac.name, last_local_ts.strftime('%Y-%m-%d %H:%M:%S.%f')))
                    res = cur.fetchone()
                    if res: last_reason_val = res[0]

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT time, tag_name, value 
                    FROM telemetry_event 
                    WHERE machine_name = %s AND time > %s
                    ORDER BY time ASC LIMIT 5000
                """, (mac.name, last_local_ts.strftime('%Y-%m-%d %H:%M:%S.%f')))
                rows = cur.fetchall()

        if not rows:
            return

        for row in rows:
            ts_raw, tag, val = row
            
            if isinstance(ts_raw, str):
                evt_dt = fields.Datetime.to_datetime(ts_raw.replace('T', ' ').replace('Z', '')[:19])
            else:
                evt_dt = ts_raw.replace(tzinfo=None)
                
            evt_utc = local_tz.localize(evt_dt).astimezone(pytz.utc).replace(tzinfo=None)
            
            if active_state and evt_utc <= active_state.start_time:
                continue

            tgt_model = None
            evt_id = None

            if wc.telemetry_state_logic == 'states':
                if tag == 'OEE.nStopRootReason':
                    last_reason_val = val
                    if active_state and active_state._name == 'mes.performance.alarm':
                        evt = self._resolve_event(mac, tag, int(val) if val is not None else 0)
                        if evt: active_state.write({'loss_id': evt.id})
                    continue
                
                if tag != 'OEE.nMachineState':
                    continue
                    
                plc_val = int(val) if val is not None else 0
                if plc_val == 1:
                    tgt_model = 'mes.performance.alarm'
                    evt = self._resolve_event(mac, 'OEE.nStopRootReason', int(last_reason_val) if last_reason_val is not None else 0)
                    evt_id = evt.id if evt else None
                elif plc_val in [3, 5] and active_state and active_state._name == 'mes.performance.alarm':
                    evt = self._resolve_event(mac, 'OEE.nStopRootReason', int(last_reason_val) if last_reason_val is not None else 0)
                    if evt and evt.id == active_state.loss_id.id:
                        continue 
                    tgt_model, evt_id = self.classify_fsm_transition(wc, tag, val)
                else:
                    tgt_model, evt_id = self.classify_fsm_transition(wc, tag, val)
            else:
                tgt_model, evt_id = self.classify_fsm_transition(wc, tag, val)

            if not tgt_model or not evt_id:
                continue

            active_model = active_state._name if active_state else None
            active_loss_id = active_state.loss_id.id if active_state else None

            if active_model == tgt_model and active_loss_id == evt_id:
                continue

            if active_model == 'mes.performance.alarm' and tgt_model != 'mes.performance.running':
                continue

            if active_state:
                active_state.write({'end_time': evt_utc})

            perf_doc = self._get_or_create_doc(wc, evt_utc)
            if not perf_doc:
                continue

            active_state = self.env[tgt_model].create({
                'performance_id': perf_doc.id,
                'loss_id': evt_id,
                'start_time': evt_utc
            })

    @api.model
    def classify_fsm_transition(self, wc, tag, val):
        plc_val = int(val) if val is not None else 0
        mac = wc.machine_settings_id
        
        evt = self._resolve_event(mac, tag, plc_val)
        if not evt:
            return None, None

        is_run = False
        if wc.runtime_event_id:
            run_sig = mac.event_tag_ids.filtered(lambda x: x.event_id == wc.runtime_event_id)
            if run_sig:
                is_run = (run_sig[0].tag_name == tag and run_sig[0].plc_value == plc_val)
            else:
                is_run = (wc.runtime_event_id.default_event_tag_type == tag and wc.runtime_event_id.default_plc_value == plc_val)
        
        if is_run:
            return 'mes.performance.running', evt.id

        stop_tag = mac.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
        if tag == stop_tag or tag == 'OEE.nStopRootReason':
            return 'mes.performance.alarm', evt.id

        return 'mes.performance.slowing', evt.id

    @api.model
    def _resolve_event(self, mac, tag, val):
        sig = self.env['mes.signal.event'].search([('machine_id', '=', mac.id), ('tag_name', '=', tag), ('plc_value', '=', val)], limit=1)
        if sig: 
            return sig.event_id
            
        evt = self.env['mes.event'].search([('default_event_tag_type', '=', tag), ('default_plc_value', '=', val)], limit=1)
        if evt: 
            return evt
            
        grp = self.env['mes.event'].search([('name', '=', 'Unknown'), ('parent_id', '=', False)], limit=1)
        if not grp: 
            grp = self.env['mes.event'].create({'name': 'Unknown'})
            
        return self.env['mes.event'].create({
            'name': f'Unknown {tag} Code {val}',
            'parent_id': grp.id, 
            'default_event_tag_type': tag, 
            'default_plc_value': val
        })

    @api.model
    def cron_manage_shifts(self):
        now_utc = fields.Datetime.now()
        draft_docs = self.search([('state', '=', 'draft')], order='date asc', limit=50)

        for doc in draft_docs:
            try:
                shift_start_loc, shift_end_loc = doc._get_local_shift_times()
                shift_end_utc = doc._get_utc_time(shift_end_loc)
                
                closure_threshold_utc = shift_end_utc + timedelta(minutes=5)

                if now_utc >= closure_threshold_utc:
                    doc._close_shift_and_calculate_totals(shift_start_loc, shift_end_loc, shift_end_utc)
                    self.env.cr.commit()
            except Exception as exc:
                self.env.cr.rollback()
                _logger.error("SHIFT_CLOSE_FAIL | Doc: %s | Err: %s", doc.id, str(exc))

    def _close_shift_and_calculate_totals(self, shift_start_loc, shift_end_loc, shift_end_utc):
        wc = self.machine_id
        mac_settings = wc.machine_settings_id
        if not mac_settings:
            self.write({'state': 'done'})
            return

        self.running_ids.unlink()
        self.alarm_ids.unlink()
        self.slowing_ids.unlink()
        self.production_ids.unlink()
        self.rejection_ids.unlink()

        shift_start_utc = self._get_utc_time(shift_start_loc)
        start_str = shift_start_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_str = shift_end_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tag_name, value FROM (
                        SELECT tag_name, value, row_number() OVER (PARTITION BY tag_name ORDER BY time DESC) as rn
                        FROM telemetry_event
                        WHERE machine_name = %s AND time <= %s
                    ) sub WHERE rn = 1
                """, (mac_settings.name, start_str))
                current_tags = {r[0]: r[1] for r in cur.fetchall()}

                cur.execute("""
                    SELECT time, tag_name, value FROM telemetry_event
                    WHERE machine_name = %s AND time > %s AND time <= %s
                    ORDER BY time ASC
                """, (mac_settings.name, start_str, end_str))
                shift_events = cur.fetchall()

        current_model = None
        current_loss_id = None
        current_start_utc = shift_start_utc
        last_alarm_rsn = None

        if wc.telemetry_state_logic == 'states':
            st_val = current_tags.get('OEE.nMachineState')
            rsn_val = current_tags.get('OEE.nStopRootReason', 0)
            if st_val is not None and int(float(st_val)) == 1:
                last_alarm_rsn = int(float(rsn_val))

            def eval_state(tags, last_rsn):
                st = tags.get('OEE.nMachineState')
                rsn = tags.get('OEE.nStopRootReason', 0)
                if st is None: return None, None, last_rsn
                
                st = int(float(st))
                rsn = int(float(rsn))

                is_run = False
                if wc.runtime_event_id:
                    run_sig = mac_settings.event_tag_ids.filtered(lambda x: x.event_id == wc.runtime_event_id)
                    if run_sig:
                        is_run = (run_sig[0].tag_name == 'OEE.nMachineState' and run_sig[0].plc_value == st)
                    else:
                        is_run = (wc.runtime_event_id.default_event_tag_type == 'OEE.nMachineState' and wc.runtime_event_id.default_plc_value == st)
                
                if is_run:
                    return 'mes.performance.running', wc.runtime_event_id.id, None
                
                eff_st = st
                if st in [3, 5] and last_rsn is not None and rsn == last_rsn:
                    eff_st = 1
                
                if eff_st == 1:
                    evt = self._resolve_event(mac_settings, 'OEE.nStopRootReason', rsn)
                    return 'mes.performance.alarm', (evt.id if evt else None), rsn
                else:
                    evt = self._resolve_event(mac_settings, 'OEE.nMachineState', st)
                    return 'mes.performance.slowing', (evt.id if evt else None), last_rsn

            current_model, current_loss_id, last_alarm_rsn = eval_state(current_tags, last_alarm_rsn)

            for row in shift_events:
                ts_raw, tag, val = row
                if isinstance(ts_raw, str):
                    evt_dt = fields.Datetime.to_datetime(ts_raw.replace('T', ' ').replace('Z', '')[:19])
                else:
                    evt_dt = ts_raw.replace(tzinfo=None)
                
                tz_name = wc.company_id.tz or 'UTC'
                local_tz = pytz.timezone(tz_name)
                evt_utc = local_tz.localize(evt_dt).astimezone(pytz.utc).replace(tzinfo=None)

                current_tags[tag] = val
                tgt_model, evt_id, last_alarm_rsn = eval_state(current_tags, last_alarm_rsn)

                if tgt_model and evt_id and (tgt_model != current_model or evt_id != current_loss_id):
                    if current_model and current_loss_id and current_start_utc < evt_utc:
                        self.env[current_model].create({
                            'performance_id': self.id,
                            'loss_id': current_loss_id,
                            'start_time': current_start_utc,
                            'end_time': evt_utc
                        })
                    current_model = tgt_model
                    current_loss_id = evt_id
                    current_start_utc = evt_utc
        else:
            for row in shift_events:
                ts_raw, tag, val = row
                if isinstance(ts_raw, str):
                    evt_dt = fields.Datetime.to_datetime(ts_raw.replace('T', ' ').replace('Z', '')[:19])
                else:
                    evt_dt = ts_raw.replace(tzinfo=None)
                tz_name = wc.company_id.tz or 'UTC'
                local_tz = pytz.timezone(tz_name)
                evt_utc = local_tz.localize(evt_dt).astimezone(pytz.utc).replace(tzinfo=None)

                tgt_model, evt_id = self.classify_fsm_transition(wc, tag, val)
                if tgt_model and evt_id and (tgt_model != current_model or evt_id != current_loss_id):
                    if current_model and current_loss_id and current_start_utc < evt_utc:
                        self.env[current_model].create({
                            'performance_id': self.id,
                            'loss_id': current_loss_id,
                            'start_time': current_start_utc,
                            'end_time': evt_utc
                        })
                    current_model = tgt_model
                    current_loss_id = evt_id
                    current_start_utc = evt_utc

        if current_model and current_loss_id and current_start_utc < shift_end_utc:
            self.env[current_model].create({
                'performance_id': self.id,
                'loss_id': current_loss_id,
                'start_time': current_start_utc,
                'end_time': shift_end_utc
            })

        prod_vals = []
        rej_vals = []
        
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        t.tag_name, 
                        COALESCE(SUM(t.value), 0) as sum_val, 
                        COALESCE(MAX(t.value), 0) - COALESCE(
                            (SELECT value FROM telemetry_count start_t
                             WHERE start_t.machine_name = %s 
                               AND start_t.tag_name = t.tag_name 
                               AND start_t.time <= %s 
                             ORDER BY start_t.time DESC LIMIT 1), 
                            COALESCE(MIN(t.value), 0)
                        ) as cum_val
                    FROM telemetry_count t
                    WHERE t.machine_name = %s AND t.time > %s AND t.time <= %s
                    GROUP BY t.tag_name
                """, (mac_settings.name, start_str, mac_settings.name, start_str, end_str))
                
                for row in cur.fetchall():
                    tag_name, sum_val, cum_val = row
                    tag_cfg = mac_settings.count_tag_ids.filtered(lambda s: s.tag_name == tag_name)
                    
                    if tag_cfg:
                        qty = cum_val if tag_cfg[0].is_cumulative else sum_val
                        if qty > 0:
                            count_dict_id = tag_cfg[0].count_id
                            val_dict = {
                                'performance_id': self.id,
                                'qty': float(qty),
                                'reason_id': count_dict_id.id
                            }
                            
                            if count_dict_id == self.machine_id.production_count_id:
                                prod_vals.append(val_dict)
                            else:
                                rej_vals.append(val_dict)

        if prod_vals:
            self.env['mes.performance.production'].create(prod_vals)
        if rej_vals:
            self.env['mes.performance.rejection'].create(rej_vals)

        if self._is_empty_shift():
            self.unlink()
        else:
            self.write({'state': 'done'})

    def _is_empty_shift(self):
        has_production = bool(self.production_ids)
        has_running = bool(self.running_ids)
        return not (has_production or has_running)
    
    def _get_or_create_doc(self, wc, ts_utc):
        mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
        ts_loc = pytz.utc.localize(ts_utc).astimezone(mac_tz).replace(tzinfo=None)
        
        curr_h = ts_loc.hour + ts_loc.minute / 60.0 + ts_loc.second / 3600.0
        shifts = self.env['mes.shift'].search([('company_id', '=', wc.company_id.id)])
        val_s = [s for s in shifts if not (s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
        
        cur_s = None
        for s in val_s:
            if (s.start_hour < s.end_hour and s.start_hour <= curr_h < s.end_hour) or \
               (s.start_hour >= s.end_hour and (curr_h >= s.start_hour or curr_h < s.end_hour)):
                cur_s = s
                break

        if not cur_s:
            return None
            
        st_d = ts_loc.date()
        if cur_s.start_hour > cur_s.end_hour and curr_h < cur_s.end_hour:
            st_d -= timedelta(days=1)
            
        doc = self.search([('machine_id', '=', wc.id), ('shift_id', '=', cur_s.id), ('date', '=', st_d)], limit=1)
        
        if not doc:
            doc = self.create({'machine_id': wc.id, 'shift_id': cur_s.id, 'date': st_d})
        return doc

    def _get_local_shift_times(self):
        self.ensure_one()
        s_time = datetime.combine(
            self.date,
            time(hour=int(self.shift_id.start_hour), minute=int((self.shift_id.start_hour % 1) * 60))
        )
        e_time = s_time + timedelta(hours=self.shift_id.duration)
        return s_time, e_time

    def _get_utc_time(self, local_naive_dt):
        self.ensure_one()
        tz_name = self.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        local_dt = mac_tz.localize(local_naive_dt.replace(tzinfo=None))
        return local_dt.astimezone(pytz.utc).replace(tzinfo=None)

class MesPerformanceAlarm(models.Model):
    _name = 'mes.performance.alarm'
    _description = 'Machine Alarms'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Alarm Reason', required=True)
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceRunning(models.Model):
    _name = 'mes.performance.running'
    _description = 'Machine Runnings'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Activity Type', required=True) 
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceSlowing(models.Model):
    _name = 'mes.performance.slowing'
    _description = 'Machine Slowing Logs'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Slowing Reason') 
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceRejection(models.Model):
    _name = 'mes.performance.rejection'
    _description = 'Machine Rejections'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Rejection Reason') 

class MesPerformanceProduction(models.Model):
    _name = 'mes.performance.production'
    _description = 'Machine Production'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Count Type')

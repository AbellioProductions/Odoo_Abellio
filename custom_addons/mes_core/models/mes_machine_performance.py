from datetime import datetime, time, timedelta
import pytz
from odoo import models, fields, api

import logging

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

    alarm_ids = fields.One2many('mes.performance.alarm', 'performance_id', string='Alarms')
    running_ids = fields.One2many('mes.performance.running', 'performance_id', string='Running Logs')
    slowing_ids = fields.One2many('mes.performance.slowing', 'performance_id', string='Slowing Logs')
    rejection_ids = fields.One2many('mes.performance.rejection', 'performance_id', string='Rejections')
    production_ids = fields.One2many('mes.performance.production', 'performance_id', string='Production Output')

    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)

    state = fields.Selection([
        ('draft', 'Draft'),
        ('done', 'Locked')
    ], string='Status', default='draft', tracking=True)

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
    def cron_manage_shifts(self):
        wcs = self.env['mrp.workcenter'].search([('machine_settings_id', '!=', False)])
        now_utc = fields.Datetime.now()
        
        for wc in wcs:
            self._ensure_active_shift_doc(wc, now_utc)
            
        self._close_expired_shifts(now_utc)

    @api.model
    def register_chain_event(self, mac_name, tag, val, ts_loc):
        mac = self.env['mes.machine.settings'].search([('name', '=', mac_name)], limit=1)
        if not mac:
            return
            
        wc = self.env['mrp.workcenter'].search([('machine_settings_id', '=', mac.id)], limit=1)
        if not wc:
            return
            
        doc = self._get_or_create_doc(wc, ts_loc)
        if not doc:
            return
            
        tz_name = wc.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        local_dt = mac_tz.localize(ts_loc, is_dst=False)
        ts_utc = local_dt.astimezone(pytz.utc).replace(tzinfo=None)
            
        evt = self._resolve_evt(mac, tag, val)
        doc._append_evt(wc, evt, tag, val, ts_utc)

    def _get_or_create_doc(self, wc, ts_loc):
        curr_h = ts_loc.hour + ts_loc.minute / 60.0 + ts_loc.second / 3600.0
        shifts = self.env['mes.shift'].search([('company_id', '=', wc.company_id.id)])
        val_s = [s for s in shifts if not (s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
        
        cur_s = self._find_active_shift(val_s, curr_h)
        if not cur_s:
            return None
            
        st_d = ts_loc.date()
        if cur_s.start_hour > cur_s.end_hour and curr_h < cur_s.end_hour:
            st_d -= timedelta(days=1)
            
        doc = self.search([
            ('machine_id', '=', wc.id),
            ('shift_id', '=', cur_s.id),
            ('date', '=', st_d)
        ], limit=1)
        
        if not doc:
            doc = self.create({
                'machine_id': wc.id,
                'shift_id': cur_s.id,
                'date': st_d
            })
        return doc

    def _resolve_evt(self, mac, tag, val):
        sig = self.env['mes.signal.event'].search([
            ('machine_id', '=', mac.id),
            ('tag_name', '=', tag),
            ('plc_value', '=', val)
        ], limit=1)
        if sig:
            return sig.event_id
            
        evt = self.env['mes.event'].search([
            ('default_event_tag_type', '=', tag),
            ('default_plc_value', '=', val)
        ], limit=1)
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

    def _append_evt(self, wc, evt, tag, val, ts):
        self.ensure_one()
        opn = self._get_open_log()
        tgt_mod = False
        
        is_run = self._is_run(wc, tag, val)
        
        if is_run:
            tgt_mod = 'mes.performance.running'
        elif tag == 'OEE.nMachineState' and val == 1:
            tgt_mod = 'mes.performance.alarm'
        elif tag == 'OEE.nMachineState':
            tgt_mod = 'mes.performance.slowing'

        if tag == 'OEE.nStopRootReason':
            is_val_empty = val == 0 or val == '' or val is None
            if not is_val_empty:
                if opn and opn._name == 'mes.performance.alarm':
                    is_gen = (opn.loss_id.default_event_tag_type == 'OEE.nMachineState' and opn.loss_id.default_plc_value == 1)
                    if is_gen:
                        opn.write({'loss_id': evt.id})
                return
            else:
                if opn and opn._name == 'mes.performance.alarm':
                    is_gen = (opn.loss_id.default_event_tag_type == 'OEE.nMachineState' and opn.loss_id.default_plc_value == 1)
                    if is_gen:
                        tgt_mod = 'mes.performance.slowing'
                    else:
                        return
                else:
                    return

        if not tgt_mod:
            return

        if tgt_mod == 'mes.performance.alarm' and tag == 'OEE.nMachineState' and val == 1:
            evt_use = evt
            from datetime import timedelta
            
            last_run = self.env['mes.performance.running'].search([
                ('performance_id', '=', self.id),
                ('start_time', '<=', ts)
            ], order='start_time desc', limit=1)
            
            s_start = last_run.start_time if last_run else (ts - timedelta(hours=12))
            
            mac = wc.machine_settings_id
            rsn_tag = mac.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
            
            try:
                import pytz
                tz_name = wc.company_id.tz or 'UTC'
                mac_tz = pytz.timezone(tz_name)
                
                s_start_loc = pytz.utc.localize(s_start).astimezone(mac_tz).replace(tzinfo=None)
                ts_loc = pytz.utc.localize(ts).astimezone(mac_tz).replace(tzinfo=None)

                with self.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        
                        s_str = s_start_loc.strftime('%Y-%m-%d %H:%M:%S.%f+00')
                        e_str = ts_loc.strftime('%Y-%m-%d %H:%M:%S.%f+00')

                        cur.execute("""
                            SELECT value FROM telemetry_event 
                            WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
                            ORDER BY time DESC LIMIT 1
                        """, (mac.name, rsn_tag, s_str, e_str))
                        row = cur.fetchone()
                        
                        if row and row[0] is not None:
                            raw_v = str(row[0]).strip()
                            if raw_v:
                                try:
                                    v_int = int(float(raw_v))
                                    if v_int != 0:
                                        rsn_evt = self._resolve_evt(mac, rsn_tag, v_int)
                                        if rsn_evt:
                                            evt_use = rsn_evt
                                except ValueError:
                                    pass
            except Exception:
                pass

            if opn:
                if opn._name == tgt_mod and opn.loss_id.id == evt_use.id:
                    return
                opn.write({'end_time': ts})
                
            self.env[tgt_mod].create({
                'performance_id': self.id,
                'loss_id': evt_use.id,
                'start_time': ts
            })
            return

        if opn:
            if opn._name == tgt_mod and opn.loss_id.id == evt.id:
                return
            opn.write({'end_time': ts})
            
        self.env[tgt_mod].create({
            'performance_id': self.id,
            'loss_id': evt.id,
            'start_time': ts
        })

    def _get_open_log(self):
        self.ensure_one()
        alarm = self.env['mes.performance.alarm'].search([('performance_id', '=', self.id), ('end_time', '=', False)], limit=1)
        if alarm: return alarm
        run = self.env['mes.performance.running'].search([('performance_id', '=', self.id), ('end_time', '=', False)], limit=1)
        if run: return run
        slow = self.env['mes.performance.slowing'].search([('performance_id', '=', self.id), ('end_time', '=', False)], limit=1)
        if slow: return slow
        return None

    def _is_run(self, wc, tag, val):
        mac = wc.machine_settings_id
        run_sig = mac.event_tag_ids.filtered(lambda x: x.event_id == wc.runtime_event_id)
        if run_sig:
            return run_sig[0].tag_name == tag and run_sig[0].plc_value == val
        return wc.runtime_event_id.default_event_tag_type == tag and wc.runtime_event_id.default_plc_value == val
    
    def _ensure_active_shift_doc(self, wc, now_utc):
        tz_name = wc.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        
        curr_h = now_mac.hour + now_mac.minute / 60.0 + now_mac.second / 3600.0
        shifts = self.env['mes.shift'].search([('company_id', '=', wc.company_id.id)])
        valid_shifts = [s for s in shifts if not (s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
        
        curr_s = self._find_active_shift(valid_shifts, curr_h)
        if not curr_s:
            return
            
        start_d = now_mac.date()
        if curr_s.start_hour > curr_s.end_hour and curr_h < curr_s.end_hour:
            start_d -= timedelta(days=1)
            
        doc = self.search([
            ('machine_id', '=', wc.id),
            ('shift_id', '=', curr_s.id),
            ('date', '=', start_d)
        ], limit=1)
        
        if not doc:
            doc = self.create({
                'machine_id': wc.id,
                'shift_id': curr_s.id,
                'date': start_d,
            })
            s_loc, _ = doc._get_local_shift_times()
            s_utc = doc._get_utc_time(s_loc)
            doc._init_initial_state(s_loc, s_utc)

    def _find_active_shift(self, shifts, curr_h):
        for s in shifts:
            if s.start_hour < s.end_hour:
                if s.start_hour <= curr_h < s.end_hour:
                    return s
            else:
                if curr_h >= s.start_hour or curr_h < s.end_hour:
                    return s
        return None

    def _close_expired_shifts(self, now_utc):
        draft_docs = self.search([('state', '=', 'draft')])
        for doc in draft_docs:
            if doc._is_shift_ended(now_utc):
                doc.action_close_shift()

    def _is_shift_ended(self, now_utc):
        self.ensure_one()
        tz_name = self.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        
        s_time = datetime.combine(
            self.date,
            time(hour=int(self.shift_id.start_hour), minute=int((self.shift_id.start_hour % 1) * 60))
        )
        e_time = s_time + timedelta(hours=self.shift_id.duration)
        
        return now_mac >= e_time

    def action_close_shift(self):
        docs_to_del = self.env['mes.machine.performance']
        for doc in self:
            s_loc, e_loc = doc._get_local_shift_times()
            e_utc = doc._get_utc_time(e_loc)
            
            doc._close_open_events(e_utc)
            doc._process_telemetry_counts(s_loc, e_loc)
            
            if doc._is_empty_shift():
                docs_to_del |= doc
            else:
                doc.write({'state': 'done'})
                
        if docs_to_del:
            docs_to_del.unlink()

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
        local_dt = mac_tz.localize(local_naive_dt, is_dst=False)
        return local_dt.astimezone(pytz.utc).replace(tzinfo=None)

    def _close_open_events(self, end_time):
        domain = [('performance_id', '=', self.id), ('end_time', '=', False)]
        self.env['mes.performance.alarm'].search(domain).write({'end_time': end_time})
        self.env['mes.performance.running'].search(domain).write({'end_time': end_time})
        self.env['mes.performance.slowing'].search(domain).write({'end_time': end_time})

    def _process_telemetry_counts(self, s_time, e_time):
        mac = self.machine_id.machine_settings_id
        if not mac:
            return
            
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                raw_counts = mac._fetch_waste_stats_raw(cur, s_time, e_time)
                
        prod_vals = []
        rej_vals = []
        prod_count_id = self.machine_id.production_count_id
        
        for ct in mac.count_tag_ids:
            if not ct.count_id:
                continue
                
            tag_data = raw_counts.get(ct.tag_name, {'sum': 0.0, 'cum': 0.0})
            qty = tag_data.get('cum') if ct.is_cumulative else tag_data.get('sum')
            
            if qty <= 0:
                continue
                
            val_dict = {
                'performance_id': self.id,
                'qty': qty,
                'reason_id': ct.count_id.id
            }
            
            if prod_count_id and ct.count_id.id == prod_count_id.id:
                prod_vals.append(val_dict)
            else:
                rej_vals.append(val_dict)
                
        if prod_vals:
            self.env['mes.performance.production'].create(prod_vals)
        if rej_vals:
            self.env['mes.performance.rejection'].create(rej_vals)

    def _init_initial_state(self, s_loc, s_utc=None):
        mac = self.machine_id.machine_settings_id
        if not mac:
            return
            
        if not s_utc:
            tz_name = self.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_name)
            local_dt = mac_tz.localize(s_loc, is_dst=False)
            s_utc = local_dt.astimezone(pytz.utc).replace(tzinfo=None)

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT tag_name, value 
                    FROM telemetry_event 
                    WHERE machine_name = %s AND time <= %s 
                    ORDER BY time DESC LIMIT 1
                """, (mac.name, s_loc))
                row = cur.fetchone()
                
        if row:
            tag, val = row
            evt = self._resolve_evt(mac, tag, val)
            self._append_evt(self.machine_id, evt, tag, val, s_utc)

    def _process_historical_events(self, s_loc, e_loc):
        mac = self.machine_id.machine_settings_id
        if not mac:
            return
            
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT time, tag_name, value 
                    FROM telemetry_event 
                    WHERE machine_name = %s AND time >= %s AND time <= %s 
                    ORDER BY time ASC
                """, (mac.name, s_loc, e_loc))
                rows = cur.fetchall()
                
        tz_name = self.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        
        for row in rows:
            ts_loc, tag, val = row
            ts_cl = ts_loc.replace(tzinfo=None) if hasattr(ts_loc, 'replace') else ts_loc
            
            local_dt = mac_tz.localize(ts_cl, is_dst=False)
            ts_utc = local_dt.astimezone(pytz.utc).replace(tzinfo=None)
            
            evt = self._resolve_evt(mac, tag, val)
            self._append_evt(self.machine_id, evt, tag, val, ts_utc)

    def _is_empty_shift(self):
        self.ensure_one()
        r_sum = sum(self.running_ids.mapped('duration'))
        p_sum = sum(self.production_ids.mapped('qty'))
        return r_sum <= 0 and p_sum <= 0


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
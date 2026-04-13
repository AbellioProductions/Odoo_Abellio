import operator
import pytz
import logging
from datetime import datetime, timedelta, time
import math
from odoo import models, fields, api
from odoo.exceptions import ValidationError
from odoo.http import request
from odoo.osv import expression

_logger = logging.getLogger(__name__)

class MesShifts(models.Model):
    _name = 'mes.shift'
    _description = 'Work Shifts'
    _order = 'sequence, start_hour'
    
    sequence = fields.Integer(string="Sequence", default=10)
    name = fields.Char(string='Shift Name', required=True)
    code = fields.Char(string='Code')
    start_hour = fields.Float(string='Start Hour')
    end_hour = fields.Float(string='End Hour')
    duration = fields.Float(string='Duration (Hours)', compute='_compute_duration', store=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    workcenter_ids = fields.Many2many('mrp.workcenter', string='Machines', domain="[('company_id', '=', company_id)]")

    @api.depends('start_hour', 'end_hour')
    def _compute_duration(self):
        for s in self:
            if s.end_hour >= s.start_hour:
                s.duration = s.end_hour - s.start_hour
            else:
                s.duration = 24.0 - s.start_hour + s.end_hour

    @api.model
    def get_current_shift_window(self, wc=None):
        now_utc = fields.Datetime.now()
        tz_name = wc.company_id.tz if wc and wc.company_id.tz else 'UTC'
        mac_tz = pytz.timezone(tz_name)
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        
        curr_h = now_mac.hour + now_mac.minute / 60.0 + now_mac.second / 3600.0
        
        shifts = self.search([('company_id', '=', wc.company_id.id)])
        valid_shifts = [s for s in shifts if not (wc and s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
        
        curr_s = None
        for s in valid_shifts:
            if s.start_hour < s.end_hour:
                if s.start_hour <= curr_h < s.end_hour:
                    curr_s = s
                    break
            else:
                if curr_h >= s.start_hour or curr_h < s.end_hour:
                    curr_s = s
                    break
        
        if not curr_s:
            return None, None

        start_d = now_mac.date()
        if curr_s.start_hour > curr_s.end_hour and curr_h < curr_s.end_hour:
            start_d -= timedelta(days=1)
            
        s_time = datetime.combine(
            start_d,
            time(hour=int(curr_s.start_hour), minute=int((curr_s.start_hour % 1) * 60))
        )
        
        e_time = s_time + timedelta(hours=curr_s.duration)
        
        return s_time, e_time

class MesDefects(models.Model):
    _name = 'mes.defect'
    _description = 'QC Defect Types'
    
    name = fields.Char(string='Defect Name', required=True)
    code = fields.Char(string='Defect Code')
    description = fields.Text(string='Description')

class MesHierarchyMixin(models.AbstractModel):
    _name = 'mes.hierarchy.mixin'
    _description = 'Hierarchy Sync Mixin'

    @api.model
    def sync_batch(self, data_list):
        existing = self.search([])
        code_map = {r.code: r for r in existing if r.code}
        name_map = {r.name: r for r in existing if r.name}
        parent_cache = {r.name: r.id for r in existing if r.name}

        for item in data_list:
            name = item.get('name', '').strip()
            code = item.get('code', '').strip()
            parent_name = item.get('parent_name', '').strip()
            vals = item.get('vals', {})

            if not name:
                continue

            if name == parent_name:
                parent_name = ''

            parent_id = False
            if parent_name:
                if parent_name not in parent_cache:
                    new_parent = self.create({'name': parent_name})
                    parent_cache[parent_name] = new_parent.id
                    name_map[parent_name] = new_parent
                parent_id = parent_cache[parent_name]

            vals.update({'name': name})
            if parent_name:
                vals['parent_id'] = parent_id
            if code:
                vals['code'] = code

            rec = code_map.get(code) if code else None
            if not rec:
                rec = name_map.get(name)

            if rec:
                if parent_id and parent_id == rec.id:
                    vals.pop('parent_id', None)
                
                try:
                    with self.env.cr.savepoint():
                        rec.write(vals)
                except Exception as e:
                    vals.pop('parent_id', None)
                    rec.write(vals)

                if code and code not in code_map:
                    code_map[code] = rec
                name_map[name] = rec
                parent_cache[name] = rec.id
            else:
                new_rec = self.create(vals)
                if code:
                    code_map[code] = new_rec
                name_map[name] = new_rec
                parent_cache[name] = new_rec.id

        return True

class MesCounts(models.Model):
    _name = 'mes.counts'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Counts'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Event', required=True)
    code = fields.Char(string='Code')
    parent_id = fields.Many2one('mes.counts', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.counts', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)

    default_OPCTag = fields.Char(string='Default OPC Tag')
    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)
    is_module_count = fields.Boolean(string='Is Module Count')
    wheel = fields.Integer(string='Wheel')
    module = fields.Integer(string='Module')

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for count in self:
            if count.parent_id:
                count.complete_name = '%s / %s' % (count.parent_id.complete_name, count.name)
            else:
                count.complete_name = count.name
                
    @api.constrains('parent_id')
    def _check_hierarchy(self):
        if not self._check_recursion():
            raise ValidationError('Error! You cannot create recursive categories.')

    def get_tag_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.count'].search([
            ('count_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name
        return self.default_OPCTag
    
    def get_count_config_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.count'].search([
            ('count_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name, override.is_cumulative
        return self.default_OPCTag, self.is_cumulative

class MesEvents(models.Model):
    _name = 'mes.event'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Event'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Event Name', required=True)
    code = fields.Char(string='Code')
    parent_id = fields.Many2one('mes.event', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.event', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)

    color = fields.Char(string='Color', default='#808080')
    default_event_tag_type = fields.Selection([
        ('OEE.nMachineState', 'Machine State (OEE.nMachineState)'),
        ('OEE.nStopRootReason', 'Stop Reason (OEE.nStopRootReason)')
    ], string='Default Tag Type')
    default_plc_value = fields.Integer(string='Default PLC Value')

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for event in self:
            if event.parent_id:
                event.complete_name = '%s / %s' % (event.parent_id.complete_name, event.name)
            else:
                event.complete_name = event.name

    @api.constrains('parent_id')
    def _check_hierarchy(self):
        if not self._check_recursion():
            raise ValidationError('Error! You cannot create recursive categories.')

    def get_mapping_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('event_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name, override.plc_value
        return self.default_event_tag_type, self.default_plc_value

class MesProcess(models.Model):
    _name = 'mes.process'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Process Parameters Dictionary'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Process Name', required=True)
    code = fields.Char(string='Code')
    parent_id = fields.Many2one('mes.process', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.process', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)
    default_OPCTag = fields.Char(string='Default OPC Tag')
    related_process_ids = fields.Many2many('mes.process', 'mes_process_related_rel', 'process_id', 'related_id', string='Related Processes')

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for rec in self:
            if rec.parent_id:
                rec.complete_name = '%s / %s' % (rec.parent_id.complete_name, rec.name)
            else:
                rec.complete_name = rec.name

    def get_tag_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.process'].search([
            ('process_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name
        return self.default_OPCTag

class MesWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    maintainx_id = fields.Integer(string='MaintainX ID')
    code_imatec = fields.Char(string='Imatec Name')
    machine_settings_id = fields.Many2one('mes.machine.settings', string='Telemetry Settings')
    runtime_event_id = fields.Many2one('mes.event', string='Runtime Event')
    production_count_id = fields.Many2one('mes.counts', string='Production Count')
    refresh_frequency = fields.Integer(string='Refresh Frequency (sec)', default=60)
    ideal_capacity_per_min = fields.Float(string='Ideal Capacity (Parts/Min)', default=200.0)

    current_oee = fields.Float(string="OEE (%)", readonly=True, default=0.0)
    current_availability = fields.Float(string="Availability (%)", readonly=True, default=0.0)
    current_performance = fields.Float(string="Performance (%)", readonly=True, default=0.0)
    current_quality = fields.Float(string="Quality (%)", readonly=True, default=0.0)
    current_produced = fields.Float(string="Produced", digits=(16, 0), readonly=True, default=0)
    current_waste_losses = fields.Float(string="Waste Losses", readonly=True, default=0.0)
    current_downtime_losses = fields.Float(string="Downtime Losses", readonly=True, default=0.0)
    current_first_running_time = fields.Datetime(string="First Running Time", readonly=True)
    current_runtime_formatted = fields.Char(string="Runtime", readonly=True, default='00:00:00')
    current_top_rejection = fields.Char(string="Top Rejection", readonly=True, default='None')
    current_top_alarm = fields.Char(string="Top Alarm", readonly=True, default='None')
    chart_bucket_minutes = fields.Integer(string='Chart Bucket (Min)', default=15)
    allowed_pc_ips = fields.Char(string='All Allowed PC IPs')
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    
    current_first_running_time_disp = fields.Char(
        compute='_compute_current_first_running_time_disp',
        store=False
    )

    telemetry_state_logic = fields.Selection([
        ('events', 'Through Events'),
        ('states', 'By State Flags')
    ], string='Telemetry FSM Logic', default='events', required=True)

    @api.depends('current_first_running_time')
    def _compute_current_first_running_time_disp(self):
        for rec in self:
            if rec.current_first_running_time:
                tz_name = rec.company_id.tz or 'UTC'
                mac_tz = pytz.timezone(tz_name)
                utc_dt = pytz.utc.localize(rec.current_first_running_time)
                loc_dt = utc_dt.astimezone(mac_tz)
                rec.current_first_running_time_disp = loc_dt.strftime('%d.%m.%Y %H:%M:%S')
            else:
                rec.current_first_running_time_disp = False

    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

    @api.model
    def _search(self, domain, offset=0, limit=None, order=None, **kwargs):
        if not self.env.context.get('skip_ip_filter'):
            domain = self._apply_operator_ip_filter(domain)
        return super()._search(domain, offset=offset, limit=limit, order=order, **kwargs)
    
    @api.model
    def cron_update_realtime_metrics(self):
        workcenters = self.search([
            ('machine_settings_id', '!=', False),
            ('runtime_event_id', '!=', False),
            ('production_count_id', '!=', False)
        ])
        if not workcenters:
            return

        settings_model = self.env['mes.machine.settings']
        oee_results = settings_model.get_realtime_oee_batch(workcenters)

        for wc in workcenters:
            data = oee_results.get(wc.id, {})
            if not data or 'error' in data:
                continue

            vals = {}
            if wc.current_oee != data.get('oee', 0.0): vals['current_oee'] = data.get('oee', 0.0)
            if wc.current_availability != data.get('availability', 0.0): vals['current_availability'] = data.get('availability', 0.0)
            if wc.current_performance != data.get('performance', 0.0): vals['current_performance'] = data.get('performance', 0.0)
            if wc.current_quality != data.get('quality', 0.0): vals['current_quality'] = data.get('quality', 0.0)
            if wc.current_produced != data.get('total_produced', 0): vals['current_produced'] = data.get('total_produced', 0)
            if wc.current_waste_losses != data.get('waste_losses', 0.0): vals['current_waste_losses'] = data.get('waste_losses', 0.0)
            if wc.current_downtime_losses != data.get('downtime_losses', 0.0): vals['current_downtime_losses'] = data.get('downtime_losses', 0.0)
            if wc.current_first_running_time != data.get('first_running_time'): vals['current_first_running_time'] = data.get('first_running_time')
            if wc.current_runtime_formatted != data.get('runtime_formatted', '00:00:00'): vals['current_runtime_formatted'] = data.get('runtime_formatted', '00:00:00')
            if wc.current_top_rejection != data.get('top_rejection', 'None'): vals['current_top_rejection'] = data.get('top_rejection', 'None')
            if wc.current_top_alarm != data.get('top_alarm', 'None'): vals['current_top_alarm'] = data.get('top_alarm', 'None')
            if vals:
                wc.write(vals)

    def action_force_metrics_update(self):
        self.ensure_one()
        oee_results = self.env['mes.machine.settings'].get_realtime_oee_batch(self)
        
        data = oee_results.get(self.id, {})
        if not data or 'error' in data:
            return

        self.write({
            'current_oee': data.get('oee', 0.0),
            'current_availability': data.get('availability', 0.0),
            'current_performance': data.get('performance', 0.0),
            'current_quality': data.get('quality', 0.0),
            'current_produced': data.get('total_produced', 0),
            'current_waste_losses': data.get('waste_losses', 0.0),
            'current_downtime_losses': data.get('downtime_losses', 0.0),
            'current_first_running_time': data.get('first_running_time'),
            'current_runtime_formatted': data.get('runtime_formatted', '00:00:00'),
            'current_top_rejection': data.get('top_rejection', 'None'),
            'current_top_alarm': data.get('top_alarm', 'None')
        })

    def action_open_waste_losses(self):
        self.ensure_one()
        if not self.machine_settings_id:
            return
        return self.machine_settings_id.action_open_waste_losses()

    def action_open_downtime_losses(self):
        self.ensure_one()
        if not self.machine_settings_id:
            return
        return self.machine_settings_id.action_open_downtime_losses()
    
    @api.model
    def _apply_operator_ip_filter(self, domain):
        if request and self.env.user.has_group('mes_core.group_mes_operator') and not self.env.user.has_group('mes_core.group_mes_manager'):
            client_ip = request.httprequest.headers.get('X-Forwarded-For', request.httprequest.remote_addr)
            if client_ip:
                client_ip = client_ip.split(',')[0].strip()
            else:
                client_ip = 'UNKNOWN'
                
            all_restricted_wcs = self.env['mrp.workcenter'].with_context(skip_ip_filter=True).sudo().search([('allowed_pc_ips', '!=', False)])
            allowed_wc_ids = []
            
            for wc in all_restricted_wcs:
                valid_ips = [ip.strip() for ip in wc.allowed_pc_ips.split(',')]
                if client_ip in valid_ips:
                    allowed_wc_ids.append(wc.id)
            
            ip_domain = [('id', 'in', allowed_wc_ids)]
            return expression.AND([domain or [], ip_domain])
        return domain

    @api.constrains('refresh_frequency')
    def _check_refresh_frequency(self):
        for wc in self:
            if wc.refresh_frequency < 10:
                raise ValidationError('Configuration error: Refresh frequency cannot be less than 10 seconds.')

    @api.model
    def _build_chart_payload(self, wc, s_loc, calc_e_loc, s_utc, calc_e_utc, b_min, count_id=False, proc_id=False):
        mac = wc.machine_settings_id

        def to_iso(dt):
            if not dt: return None
            if isinstance(dt, str): return dt
            return dt.replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')

        available_counts = []
        seen_c_ids = set()
        for ct in mac.count_tag_ids:
            if ct.count_id and ct.count_id.id not in seen_c_ids:
                available_counts.append({'id': ct.count_id.id, 'name': ct.count_id.name})
                seen_c_ids.add(ct.count_id.id)

        tgt_c = self.env['mes.counts'].browse(int(count_id)) if count_id else wc.production_count_id
        is_ideal_shown = bool(wc.production_count_id and tgt_c and tgt_c.id == wc.production_count_id.id)

        c_sigs = mac.count_tag_ids.filtered(lambda t: t.count_id == tgt_c)
        c_tags = list(set(c_sigs.mapped('tag_name')))
        is_cum_map = {s.tag_name: s.is_cumulative for s in c_sigs}

        available_procs = []
        for p in self.env['mes.process'].search([]):
            if p.get_tag_for_machine(mac):
                available_procs.append({'id': p.id, 'name': p.complete_name or p.name})

        tgt_p = self.env['mes.process'].browse(int(proc_id)) if proc_id else self.env['mes.process']
        p_to_fetch = tgt_p | tgt_p.related_process_ids

        b_sec = b_min * 60
        tot_s = (calc_e_loc - s_loc).total_seconds()
        n_ints = math.ceil(tot_s / b_sec) if tot_s > 0 else 1

        labels = []
        prod_data = []
        ideal_data = []
        b_idx_map = {}
        ideal_per_b = (wc.ideal_capacity_per_min or 0.0) * b_min

        for i in range(n_ints + 1):
            b_dt = s_loc + timedelta(seconds=i * b_sec)
            labels.append(to_iso(b_dt))
            prod_data.append(0.0)
            ideal_data.append(ideal_per_b if i > 0 else 0.0)
            b_idx_map[b_dt.strftime('%Y-%m-%dT%H:%M')] = i

        tl_data = mac._fetch_timeline_raw(s_utc, calc_e_utc, wc.id)
        
        all_p_data = []
        main_p_data = []

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                if c_tags:
                    raw_prod = mac._fetch_production_chart_raw(cur, c_tags, s_loc, calc_e_loc, b_min)
                    for row in raw_prod:
                        t_name = row[0]
                        b_time = row[1].replace(tzinfo=None)
                        qty = float(row[3]) if is_cum_map.get(t_name) else float(row[2])
                        key = b_time.strftime('%Y-%m-%dT%H:%M')
                        if key in b_idx_map:
                            idx = b_idx_map[key] + 1
                            if idx < len(prod_data): prod_data[idx] += qty

                for p in p_to_fetch:
                    p_tag = p.get_tag_for_machine(mac)
                    if not p_tag: continue
                    cur.execute("""
                        SELECT time, value FROM (
                            (SELECT time, value FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time < %s
                            ORDER BY time DESC LIMIT 1)
                            UNION ALL
                            (SELECT time, value FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
                            ORDER BY time ASC)
                        ) sub ORDER BY time ASC
                    """, (mac.name, p_tag, s_loc, mac.name, p_tag, s_loc, calc_e_loc))
                    
                    p_series = []
                    for row in cur.fetchall():
                        p_series.append({'x': to_iso(row[0].replace(tzinfo=None)), 'y': float(row[1])})
                    if tgt_p and p.id == tgt_p.id: main_p_data = p_series
                    all_p_data.append({'name': p.complete_name or p.name, 'data': p_series})

        return {
            'timeline': tl_data,
            'chart_duration_sec': n_ints * b_sec,
            'shift_start': labels[0],
            'available_counts': available_counts,
            'selected_count_id': tgt_c.id if tgt_c else False,
            'selected_count_name': tgt_c.name if tgt_c else 'Data',
            'available_processes': available_procs,
            'selected_process_id': tgt_p.id if tgt_p else False,
            'selected_process_name': tgt_p.complete_name if tgt_p else '',
            'chart': {
                'labels': labels,
                'production': prod_data,
                'ideal': ideal_data,
                'process': main_p_data,
                'processes': all_p_data,
                'bucket_sec': b_sec,
                'show_ideal': is_ideal_shown
            }
        }

    @api.model
    def get_live_chart_data(self, workcenter_id, selected_count_id=False, selected_process_id=False):
        wc = self.browse(workcenter_id)
        if not wc.exists() or not wc.machine_settings_id:
            return {'error': 'Machine not configured'}

        s_loc, e_loc = self.env['mes.shift'].get_current_shift_window(wc)
        if not s_loc: 
            return {'error': 'No active shift'}

        now_utc = fields.Datetime.now()
        mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
        
        now_loc = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        calc_e_loc = min(now_loc, e_loc)
        
        s_utc = mac_tz.localize(s_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        calc_e_utc = mac_tz.localize(calc_e_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        
        b_min = max(1, wc.chart_bucket_minutes)
        
        return self._build_chart_payload(wc, s_loc, calc_e_loc, s_utc, calc_e_utc, b_min, selected_count_id, selected_process_id)

    def _process_timeline_colors(self, machine, raw_timeline, state_configs):
        name_map = {}
        color_map = {}
        
        for ev in machine.event_tag_ids:
            key = (ev.tag_name, ev.plc_value)
            if key in name_map:
                name_map[key] += " / " + ev.event_id.name
            else:
                name_map[key] = ev.event_id.name
                color_map[key] = ev.event_id.color or '#808080'

        global_evts = self.env['mes.event'].search([])
        fallback_name_map = {ge.default_plc_value: ge.name for ge in global_evts}
        fallback_color_map = {ge.default_plc_value: (ge.color or '#808080') for ge in global_evts}

        result = []
        for row in raw_timeline:
            val = int(row[2]) if row[2] is not None else 0
            t_name = row[3]
            key = (t_name, val)
            
            evt_name = name_map.get(key) or fallback_name_map.get(val)
            evt_color = color_map.get(key) or fallback_color_map.get(val)
            
            if not evt_name:
                is_running = any(c['tag'] == t_name and c['val'] == val for c in state_configs)
                is_state_tag = any(c['tag'] == t_name for c in state_configs)
                
                if is_running:
                    evt_name, evt_color = 'Running', '#28a745'
                elif val == 0:
                    evt_name, evt_color = 'Ready / Cleared', '#cccccc'
                else:
                    evt_name = f'Code {val}'
                    evt_color = '#dc3545' if is_state_tag else '#ffc107'

            result.append({
                'start': row[0].strftime('%Y-%m-%dT%H:%M:%S'),
                'end': row[1].strftime('%Y-%m-%dT%H:%M:%S'),
                'duration': (row[1] - row[0]).total_seconds(),
                'color': evt_color,
                'name': evt_name
            })
        return result

class MesHistDashboardWiz(models.TransientModel):
    _name = 'mes.hist.dashboard.wiz'
    _description = 'Historical Chart Wizard'

    wc_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    s_time = fields.Datetime(string='Start Time', required=True)
    e_time = fields.Datetime(string='End Time', required=True)
    b_min = fields.Integer(string='Bucket (Min)', default=15, required=True)
    
    count_id = fields.Many2one('mes.counts', string='Production Count')
    proc_id = fields.Many2one('mes.process', string='Process Variable')

    available_count_ids = fields.Many2many('mes.counts', compute='_compute_available_tags')
    available_proc_ids = fields.Many2many('mes.process', compute='_compute_available_tags')

    @api.depends('wc_id')
    def _compute_available_tags(self):
        for wiz in self:
            if wiz.wc_id and wiz.wc_id.machine_settings_id:
                mac = wiz.wc_id.machine_settings_id
                c_ids = mac.count_tag_ids.mapped('count_id').ids
                wiz.available_count_ids = [(6, 0, c_ids)]
                
                valid_p = [p.id for p in self.env['mes.process'].search([]) if p.get_tag_for_machine(mac)]
                wiz.available_proc_ids = [(6, 0, valid_p)]
            else:
                wiz.available_count_ids = [(6, 0, [])]
                wiz.available_proc_ids = [(6, 0, [])]

    def action_update_chart(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mes.hist.dashboard.wiz',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'current',
        }

    @api.model
    def get_chart_data(self, wiz_id):
        r_id = wiz_id[0] if isinstance(wiz_id, list) else wiz_id
        wiz = self.browse(r_id)
        
        if not wiz.exists() or not wiz.wc_id or not wiz.s_time or not wiz.e_time:
            return {'error': 'Please provide Machine, Start Time, and End Time.'}

        tz_name = wiz.wc_id.company_id.tz or 'Europe/Dublin'
        local_tz = pytz.timezone(tz_name)

        s_utc = pytz.utc.localize(wiz.s_time)
        e_utc = pytz.utc.localize(wiz.e_time)

        s_time_wall = s_utc.astimezone(local_tz).replace(tzinfo=None)
        e_time_wall = e_utc.astimezone(local_tz).replace(tzinfo=None)

        res = self.env['mrp.workcenter']._build_chart_payload(
            wiz.wc_id, wiz.s_time, s_time_wall, wiz.e_time, e_time_wall, wiz.b_min, 
            wiz.count_id.id if wiz.count_id else False, 
            wiz.proc_id.id if wiz.proc_id else False
        )
        
        res['shift_start'] = s_time_wall.strftime('%Y-%m-%d %H:%M:%S')
        
        return res

class MesStreams(models.Model):
    _name = 'mes.stream'
    _description = 'Stream'

    stream_number = fields.Integer(string='Stream Number')
    machine_id = fields.Many2one('mrp.workcenter', string='Machine')
    
    def name_get(self):
        result = []
        for rec in self:
            name = f"Stream {rec.stream_number} ({rec.machine_id.name})"
            result.append((rec.id, name))
        return result

class MesWheels(models.Model):
    _name = 'mes.wheel'
    _description = 'Wheel'

    wheel_number = fields.Integer(string='Wheel Number')
    maintainx_id = fields.Integer(string='MaintainX ID')
    stream_id = fields.Many2one('mes.stream', string='Parent Stream')
    modules_amount = fields.Integer(string='Number of Modules')

class MesEmployee(models.Model):
    _inherit = 'hr.employee'

    maintainx_id = fields.Char(string='MaintainX ID')

class ResCompany(models.Model):
    _inherit = 'res.company'
    
    tz = fields.Selection(related='partner_id.tz', readonly=False, string='Timezone', required=True)

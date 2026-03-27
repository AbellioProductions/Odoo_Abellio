import operator
import logging
from datetime import datetime, timedelta
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
    code = fields.Char(string='Code', help="Code for external integration")
    start_hour = fields.Float(string='Start Hour')
    end_hour = fields.Float(string='End Hour')
    duration = fields.Float(
        string='Duration (Hours)', 
        compute='_compute_duration', 
        store=True, 
        readonly=True
    )

    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)

    @api.depends('start_hour', 'end_hour')
    def _compute_duration(self):
        for shift in self:
            if shift.end_hour >= shift.start_hour:
                shift.duration = shift.end_hour - shift.start_hour
            else:
                shift.duration = 24.0 - shift.start_hour + shift.end_hour

    @api.model
    def get_current_shift_window(self):
        now = fields.Datetime.now()
        current_hour = now.hour + now.minute / 60.0 + now.second / 3600.0
        
        shifts = self.search([])
        current_shift = None
        
        for shift in shifts:
            if shift.start_hour < shift.end_hour:
                if shift.start_hour <= current_hour < shift.end_hour:
                    current_shift = shift
                    break
            else:
                if current_hour >= shift.start_hour or current_hour < shift.end_hour:
                    current_shift = shift
                    break
        
        if not current_shift:
            return None, None

        start_date = now
        if current_shift.start_hour > current_shift.end_hour and current_hour < current_shift.end_hour:
            start_date = now - timedelta(days=1)
            
        start_time = start_date.replace(
            hour=int(current_shift.start_hour), 
            minute=int((current_shift.start_hour % 1) * 60), 
            second=0, 
            microsecond=0
        )
        
        end_time = start_time + timedelta(hours=current_shift.duration)
        return start_time, end_time

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
                    _logger.warning(f"Skipping parent for '{name}' to avoid recursion loop.")
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

    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")
    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False, 
                                   help="If true, OEE calculation will use MAX-MIN difference for this count instead of summing up values")
    is_module_count = fields.Boolean(string='Is Module Count', help="Indicates if this count is related to module production")
    wheel = fields.Integer(string='Wheel', help="Number of the wheel associated with this count")
    module = fields.Integer(string='Module', help="Number of the module associated with this count")

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

    color = fields.Char(string='Color', default='#808080', help="Color for the timeline dashboard")

    default_event_tag_type = fields.Selection([
        ('OEE.nMachineState', 'Machine State (OEE.nMachineState)'),
        ('OEE.nStopRootReason', 'Stop Reason (OEE.nStopRootReason)')
    ], string='Default Tag Type', help="Base tag source for this event")
    default_plc_value = fields.Integer(string='Default PLC Value', help="Value that triggers this event")

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
    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")

    related_process_ids = fields.Many2many(
        'mes.process', 
        'mes_process_related_rel', 
        'process_id', 
        'related_id', 
        string='Related Processes'
    )

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
    maintainx_id = fields.Integer(string='MaintainX ID', help="ID used in MaintainX system")
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")

    machine_settings_id = fields.Many2one('mes.machine.settings', string='Telemetry Settings')

    runtime_event_id = fields.Many2one('mes.event', string='Runtime Event', help="Runtime event used for OEE calculation")
    production_count_id = fields.Many2one('mes.counts', string='Production Count', help="Good parts count used for OEE calculation")
    refresh_frequency = fields.Integer(string='Refresh Frequency (sec)', default=60, help="Frequency of OEE dashboard refresh in seconds")
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

    chart_bucket_minutes = fields.Integer(string='Chart Bucket (Min)', default=15, help="Time grouping for production chart")

    allowed_pc_ips = fields.Char(
        string='All Allowed PC IPs',
        help='Specify the IP addresses of allowed computers, separated by commas (e.g., 192.168.1.50, 192.168.1.51)'
    )

    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    
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
            if wc.current_oee != data.get('oee', 0.0): 
                vals['current_oee'] = data.get('oee', 0.0)
            if wc.current_availability != data.get('availability', 0.0): 
                vals['current_availability'] = data.get('availability', 0.0)
            if wc.current_performance != data.get('performance', 0.0): 
                vals['current_performance'] = data.get('performance', 0.0)
            if wc.current_quality != data.get('quality', 0.0): 
                vals['current_quality'] = data.get('quality', 0.0)
            if wc.current_produced != data.get('total_produced', 0): 
                vals['current_produced'] = data.get('total_produced', 0)
            if wc.current_waste_losses != data.get('waste_losses', 0.0): 
                vals['current_waste_losses'] = data.get('waste_losses', 0.0)
            if wc.current_downtime_losses != data.get('downtime_losses', 0.0): 
                vals['current_downtime_losses'] = data.get('downtime_losses', 0.0)
            if wc.current_first_running_time != data.get('first_running_time'): 
                vals['current_first_running_time'] = data.get('first_running_time')
            if wc.current_runtime_formatted != data.get('runtime_formatted', '00:00:00'): 
                vals['current_runtime_formatted'] = data.get('runtime_formatted', '00:00:00')
            if wc.current_top_rejection != data.get('top_rejection', 'None'): 
                vals['current_top_rejection'] = data.get('top_rejection', 'None')
            if wc.current_top_alarm != data.get('top_alarm', 'None'): 
                vals['current_top_alarm'] = data.get('top_alarm', 'None')

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
                
            _logger.info(f"MES Security: Operator opened the dashboard from IP address: {client_ip}")
            
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
    def get_live_chart_data(self, workcenter_id, selected_count_id=False, selected_process_id=False):
        import math
        from datetime import timedelta

        wc = self.browse(workcenter_id)
        if not wc.exists() or not wc.machine_settings_id:
            return {'error': 'Machine not configured'}

        machine = wc.machine_settings_id
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window()
        
        if not start_time: 
            return {'error': 'No active shift'}

        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        available_counts = []
        seen_count_ids = set()
        for ct_tag in machine.count_tag_ids:
            if ct_tag.count_id and ct_tag.count_id.id not in seen_count_ids:
                available_counts.append({
                    'id': ct_tag.count_id.id,
                    'name': ct_tag.count_id.name
                })
                seen_count_ids.add(ct_tag.count_id.id)

        target_count = self.env['mes.counts'].browse(int(selected_count_id)) if selected_count_id else wc.production_count_id
        show_ideal_line = bool(wc.production_count_id and target_count and target_count.id == wc.production_count_id.id)

        good_signals = machine.count_tag_ids.filtered(lambda tag: tag.count_id == target_count)
        good_tags = list(set(good_signals.mapped('tag_name')))
        tag_is_cumulative = {sig.tag_name: sig.is_cumulative for sig in good_signals}

        if not good_tags and target_count and target_count.default_OPCTag:
            default_opc_tag = target_count.default_OPCTag
            good_tags = [default_opc_tag]
            tag_is_cumulative = {default_opc_tag: target_count.is_cumulative}

        event_signals = machine.event_tag_ids
        all_event_tags = list(set(event_signals.mapped('tag_name')))

        state_signals = event_signals.filtered(lambda tag: tag.event_id == wc.runtime_event_id)
        state_configs = [{'tag': sig.tag_name, 'val': sig.plc_value} for sig in state_signals]

        if not state_configs and wc.runtime_event_id and wc.runtime_event_id.default_event_tag_type:
            default_event_tag = wc.runtime_event_id.default_event_tag_type
            state_configs = [{'tag': default_event_tag, 'val': wc.runtime_event_id.default_plc_value}]

        for config in state_configs:
            if config['tag'] not in all_event_tags:
                all_event_tags.append(config['tag'])

        available_processes = []
        for proc in self.env['mes.process'].search([]):
            if proc.get_tag_for_machine(machine):
                available_processes.append({'id': proc.id, 'name': proc.complete_name or proc.name})

        target_process = self.env['mes.process'].browse(int(selected_process_id)) if selected_process_id else self.env['mes.process']
        
        processes_to_fetch = self.env['mes.process']
        if target_process:
            processes_to_fetch |= target_process
            processes_to_fetch |= target_process.related_process_ids

        bucket_min = max(1, wc.chart_bucket_minutes)
        bucket_sec = bucket_min * 60
        total_shift_sec = (calc_end_time - start_time).total_seconds()
        num_intervals = math.ceil(total_shift_sec / bucket_sec) if total_shift_sec > 0 else 1

        def format_utc_iso(dt_obj):
            return dt_obj.isoformat() + 'Z' if not dt_obj.tzinfo else dt_obj.isoformat()

        labels = []
        production_data = []
        ideal_data = []
        bucket_indices = {}
        ideal_per_bucket = (wc.ideal_capacity_per_min or 0.0) * bucket_min

        for interval_idx in range(num_intervals + 1):
            bucket_time = start_time + timedelta(seconds=interval_idx * bucket_sec)
            labels.append(format_utc_iso(bucket_time))
            production_data.append(0.0)
            ideal_data.append(ideal_per_bucket if interval_idx > 0 else 0.0)
            bucket_indices[bucket_time] = interval_idx

        timeline_data = []
        all_processes_data = []
        primary_process_data = []

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                if all_event_tags:
                    raw_timeline = machine._fetch_timeline_raw(cur, start_time, calc_end_time, all_event_tags)
                    timeline_data = self._process_timeline_colors(machine, raw_timeline, state_configs)

                if good_tags:
                    raw_production = machine._fetch_production_chart_raw(cur, good_tags, start_time, calc_end_time, bucket_min)
                    for row in raw_production:
                        tag_name = row[0]
                        bucket_ts = row[1].replace(tzinfo=None) if row[1].tzinfo else row[1]
                        produced_qty = float(row[3]) if tag_is_cumulative.get(tag_name) else float(row[2])

                        if bucket_ts in bucket_indices:
                            plot_idx = bucket_indices[bucket_ts] + 1
                            if plot_idx < len(production_data):
                                production_data[plot_idx] += produced_qty

                start_iso = format_utc_iso(start_time)
                
                for proc in processes_to_fetch:
                    proc_tag = proc.get_tag_for_machine(machine)
                    if not proc_tag:
                        continue

                    telemetry_query = """
                        SELECT time, value FROM (
                            (SELECT time, value
                            FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time < %s
                            ORDER BY time DESC LIMIT 1)
                            UNION ALL
                            (SELECT time, value
                            FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
                            ORDER BY time ASC)
                        ) sub_query ORDER BY time ASC
                    """
                    cur.execute(telemetry_query, (
                        machine.name, proc_tag, start_time,
                        machine.name, proc_tag, start_time, calc_end_time
                    ))

                    proc_series = []
                    for row in cur.fetchall():
                        pt_x = format_utc_iso(row[0])
                        proc_series.append({
                            'x': start_iso if pt_x < start_iso else pt_x,
                            'y': float(row[1])
                        })

                    if target_process and proc.id == target_process.id:
                        primary_process_data = proc_series

                    all_processes_data.append({
                        'name': proc.complete_name or proc.name,
                        'data': proc_series
                    })

        return {
            'timeline': timeline_data,
            'chart_duration_sec': num_intervals * bucket_sec,
            'shift_start': format_utc_iso(start_time),
            'available_counts': available_counts,
            'selected_count_id': target_count.id if target_count else False,
            'selected_count_name': target_count.name if target_count else 'Data',
            'available_processes': available_processes,
            'selected_process_id': target_process.id if target_process else False,
            'selected_process_name': target_process.complete_name if target_process else '',
            'chart': {
                'labels': labels,
                'production': production_data,
                'ideal': ideal_data,
                'process': primary_process_data,
                'processes': all_processes_data,
                'bucket_sec': bucket_sec,
                'show_ideal': show_ideal_line
            }
        }

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
                'start': row[0].isoformat(),
                'end': row[1].isoformat(),
                'duration': (row[1] - row[0]).total_seconds(),
                'color': evt_color,
                'name': evt_name
            })
        return result

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
    maintainx_id = fields.Integer(string='MaintainX ID', help="ID used in MaintainX system")
    stream_id = fields.Many2one('mes.stream', string='Parent Stream')
    modules_amount = fields.Integer(string='Number of Modules')

class MesEmployee(models.Model):
    _inherit = 'hr.employee'

    maintainx_id = fields.Char(string='MaintainX ID', help="User ID from MaintainX system")

class ResCompany(models.Model):
    _inherit = 'res.company'
    
    tz = fields.Selection(related='partner_id.tz', readonly=False, string='Timezone', required=True)
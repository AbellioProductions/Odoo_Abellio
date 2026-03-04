import re
import logging
from datetime import datetime, timedelta
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Connection Settings'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'mes.timescale.base']

    name = fields.Char(string='Machine Name', required=True, copy=False, tracking=True)
    ip_connection = fields.Char(string='Connection IP', tracking=True)
    ip_data = fields.Char(string='TwinCAT/Data IP', tracking=True)
    
    count_tag_ids = fields.One2many('mes.signal.count', 'machine_id', string='Counts')
    event_tag_ids = fields.One2many('mes.signal.event', 'machine_id', string='Events')
    process_tag_ids = fields.One2many('mes.signal.process', 'machine_id', string='Processes')

    _sql_constraints = [('name_uniq', 'unique (name)', 'Machine Name must be unique!')]

    def init(self):
        if hasattr(self.env['mes.timescale.db.manager'], '_init_DB'):
            self.env['mes.timescale.db.manager']._init_DB()
            self.env['mes.timescale.db.manager']._init_local_fdw()

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
        return super().unlink()

    def _get_current_shift_window(self):
        now = fields.Datetime.now()
        current_hour = now.hour + now.minute / 60.0 + now.second / 3600.0
        
        shifts = self.env['mes.shift'].search([])
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

    def get_alarm_tag_name(self, default_type='OEE.nStopRootReason'):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('event_id.default_event_tag_type', '=', default_type)
        ], limit=1)
        
        if override and override.tag_name:
            return override.tag_name
        
        return f"%{default_type}%"

    def resolve_plc_value_to_name(self, plc_value):
        self.ensure_one()
        plc_str = str(plc_value)
        
        if not plc_str.isdigit():
            return plc_str
            
        plc_int = int(plc_str)
        
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('plc_value', '=', plc_int)
        ], limit=1)
        
        if override and override.event_id:
            return override.event_id.name
            
        dict_event = self.env['mes.event'].search([
            ('default_plc_value', '=', plc_int)
        ], limit=1)
        
        if dict_event:
            return dict_event.name
            
        return plc_str
    
    def get_top_alarm_str(self, cursor, start_time, end_time):
        alarm_tag = self.get_alarm_tag_name('OEE.nStopRootReason')

        alarm_query = f"""
            WITH alarm_boundary AS (
                SELECT %s::timestamptz as time, value::text FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time < %s ORDER BY time DESC LIMIT 1
            ),
            alarm_events AS (
                SELECT time, value::text FROM alarm_boundary UNION ALL
                SELECT time, value::text FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time >= %s AND time <= %s
            ),
            alarm_durations AS (
                SELECT value as alarm_code, EXTRACT(EPOCH FROM (COALESCE(LEAD(time) OVER (ORDER BY time), %s) - time)) as duration_sec
                FROM alarm_events
            )
            SELECT alarm_code, SUM(duration_sec) as total_dur FROM alarm_durations
            WHERE alarm_code != '0' AND alarm_code != '' AND alarm_code IS NOT NULL
            GROUP BY alarm_code ORDER BY total_dur DESC LIMIT 1;
        """
        cursor.execute(alarm_query, (
            start_time, self.name, alarm_tag, start_time,
            self.name, alarm_tag, start_time, end_time, end_time
        ))
        res_al = cursor.fetchone()
        
        if res_al and res_al[0]:
            duration_min = int((res_al[1] or 0) // 60)
            alarm_name = self.resolve_plc_value_to_name(res_al[0])
            return f"{alarm_name} ({duration_min} min)"
            
        return "None"

    def get_top_rejection_str(self, cursor, start_time, end_time, good_production_count_id):
        top_rej_count = 0
        top_rej_name = "None"
        
        all_reject_counts = self.env['mes.counts'].search([('id', '!=', good_production_count_id)])
        
        for count_def in all_reject_counts:
            tag, is_cum = count_def.get_count_config_for_machine(self)
            if not tag:
                continue
                
            if is_cum:
                q_rej = "SELECT COALESCE(MAX(value) - MIN(value), 0) FROM telemetry_count WHERE machine_name=%s AND tag_name=%s AND time >= %s AND time <= %s"
            else:
                q_rej = "SELECT COALESCE(SUM(value), 0) FROM telemetry_count WHERE machine_name=%s AND tag_name=%s AND time >= %s AND time <= %s"
            
            cursor.execute(q_rej, (self.name, tag, start_time, end_time))
            val = cursor.fetchone()[0] or 0
            
            if val > top_rej_count:
                top_rej_count = val
                top_rej_name = count_def.name
                
        if top_rej_count > 0:
            return f"{top_rej_name} ({int(top_rej_count)})"
        return "None"

    def _get_planned_working_intervals(self, start_time, end_time, workcenter):
        now = fields.Datetime.now()
        calc_end_time = min(now, end_time)

        if start_time >= calc_end_time:
            return [], 0.0

        if not workcenter:
            return [(start_time, calc_end_time)], (calc_end_time - start_time).total_seconds()

        downtimes = self.env['mes.flat.downtime'].search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', calc_end_time),
            ('end_time', '>', start_time)
        ])

        _logger.info(f"For machine {workcenter.name}, found {len(downtimes)} downtimes overlapping with shift window {start_time} - {calc_end_time}")

        intervals = []
        for dt in downtimes:
            dt_s = max(dt.start_time, start_time)
            dt_e = min(dt.end_time, calc_end_time)
            _logger.info(f"For machine {workcenter.name}, processing downtime from {dt_s} to {dt_e}")

            if dt_s < dt_e:
                intervals.append([dt_s, dt_e])

        _logger.info(f"For machine {workcenter.name}, found {len(intervals)} downtime intervals after processing overlapping downtimes")


        dt_merged = []
        if intervals:
            intervals.sort(key=lambda x: x[0])
            dt_merged = [intervals[0]]
            for current in intervals[1:]:
                _logger.info(f"For machine {workcenter.name}, processing downtime from {current[0]} to {current[1]} for merging")
                last = dt_merged[-1]
                if current[0] <= last[1]:
                    dt_merged[-1] = [last[0], max(last[1], current[1])]
                else:
                    dt_merged.append(current)

        _logger.info(f"For machine {workcenter.name}, found {len(dt_merged)} merged downtime intervals after processing overlapping downtimes")

        active_intervals = []
        current_time = start_time
        for dt in dt_merged:
            if current_time < dt[0]:
                active_intervals.append((current_time, dt[0]))
            current_time = max(current_time, dt[1])

        if current_time < calc_end_time:
            active_intervals.append((current_time, calc_end_time))

        _logger.info(f"For machine {workcenter.name}, found {len(active_intervals)} active intervals after processing merged downtimes")

        total_planned_runtime_sec = sum((i[1] - i[0]).total_seconds() for i in active_intervals)

        return active_intervals, total_planned_runtime_sec

    def _fetch_shift_metrics(self, cursor, start_time, end_time, good_count_tag, is_cumulative, prod_count_id):
        prod_sql = "SELECT COALESCE(MAX(value) - MIN(value), 0)" if is_cumulative else "SELECT COALESCE(SUM(value), 0)"
        query = f"{prod_sql} FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s"
        cursor.execute(query, (self.name, good_count_tag, start_time, end_time))
        res = cursor.fetchone()
        total_produced = res[0] if res else 0

        top_alarm = self.get_top_alarm_str(cursor, start_time, end_time)
        top_rejection = self.get_top_rejection_str(cursor, start_time, end_time, prod_count_id)

        return total_produced, top_alarm, top_rejection

    def _fetch_active_runtime(self, cursor, state_tag, plc_value, active_intervals):
        _logger.info(f"Calculating active runtime for machine {self.name} with state_tag={state_tag}, plc_value={plc_value}, active_intervals={active_intervals}")
        if not active_intervals:
            return 0.0

        scan_start = active_intervals[0][0]
        scan_end = active_intervals[-1][1]

        val_list = [f"('{a_s} UTC'::timestamptz, '{a_e} UTC'::timestamptz)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        query = f"""
            WITH boundary AS (
                SELECT tag_name, time, value, 0::bigint as id
                FROM telemetry_event
                WHERE machine_name = %s AND time <= %s
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            events AS (
                SELECT time, value, id FROM boundary where value = %s AND tag_name = %s
                UNION ALL
                SELECT time, value, id FROM telemetry_event
                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
            ),
            state_durations AS (
                SELECT time as state_start, value as state,
                    COALESCE(LEAD(time) OVER (ORDER BY time ASC, id ASC), %s) as state_end
                FROM events
            ),
            running_durations AS (
                SELECT state_start, state_end FROM state_durations WHERE state = %s
            ),
            active_windows AS ( {active_cte} )
            SELECT COALESCE(SUM(
                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(r.state_end, aw.ai_end) - GREATEST(r.state_start, aw.ai_start)
                )))
            ), 0)
            FROM running_durations r
            INNER JOIN active_windows aw ON aw.ai_start < r.state_end AND aw.ai_end > r.state_start
        """
        cursor.execute(query, (
            self.name, scan_start,
            plc_value, state_tag, self.name, state_tag, scan_start, scan_end,
            scan_end,
            plc_value
        ))
        res = cursor.fetchone()
        return float(res[0]) if res else 0.0

    def _fetch_first_start_time(self, cursor, state_tag, plc_value, active_intervals):
        if not active_intervals:
            return False

        scan_start = active_intervals[0][0]
        scan_end = active_intervals[-1][1]

        _logger.info(f"Calculating first start time for machine {self.name} from start_scan={scan_start} to end_scan={scan_end} with state_tag={state_tag} and plc_value={plc_value}")
        

        val_list = [f"('{a_s} UTC'::timestamptz, '{a_e} UTC'::timestamptz)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        query = f"""
            WITH boundary AS (
                SELECT tag_name, time, value, 0::bigint as id
                FROM telemetry_event
                WHERE machine_name = %s AND time <= %s
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            events AS (
                SELECT time, value, id FROM boundary where value = %s AND tag_name = %s
                UNION ALL
                SELECT time, value, id FROM telemetry_event
                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
            ),
            state_durations AS (
                SELECT GREATEST(time, %s) as r_start,
                    COALESCE(LEAD(time) OVER (ORDER BY time ASC, id ASC), %s) as r_end,
                    value as state
                FROM events
            ),
            running_durations AS (
                SELECT r_start, r_end FROM state_durations WHERE state = %s AND r_start < r_end
            ),
            active_windows AS ( {active_cte} ),
            effective_runs AS (
                SELECT GREATEST(r.r_start, aw.ai_start) as eff_start
                FROM running_durations r
                INNER JOIN active_windows aw ON aw.ai_start < r.r_end AND aw.ai_end > r.r_start
            )
            SELECT MIN(eff_start) FROM effective_runs;
        """
        cursor.execute(query, (
            self.name, scan_start,
            plc_value, state_tag, self.name, state_tag, scan_start, scan_end,
            scan_start, scan_end,
            plc_value
        ))
        res = cursor.fetchone()
        return res[0].replace(tzinfo=None) if res and res[0] else False

    def _calculate_kpi(self, total_running_sec, total_produced, total_planned_runtime_sec, workcenter):
        
        _logger.info(f"For machine {workcenter.name}: total_running_sec={total_running_sec}, total_produced={total_produced}, total_planned_runtime_sec={total_planned_runtime_sec}")
        total_running_sec = max(0.0, total_running_sec)
        
        h, m, s = int(total_running_sec // 3600), int((total_running_sec % 3600) // 60), int(total_running_sec % 60)
        runtime_formatted = f"{h:02d}:{m:02d}:{s:02d}"

        ideal_rate_per_sec = (workcenter.ideal_capacity_per_min / 60.0) if (workcenter and workcenter.ideal_capacity_per_min > 0) else 1.0 
            
        raw_availability = total_running_sec / total_planned_runtime_sec if total_planned_runtime_sec > 0 else 0
        availability = max(0.0, min(raw_availability, 1.0))
        
        raw_performance = total_produced / (total_running_sec * ideal_rate_per_sec) if total_running_sec > 0 else 0
        performance = max(0.0, min(raw_performance, 1.0))
        
        quality = 1.0 
        oee = availability * performance * quality

        downtime_losses = max(0.0, 1.0 - raw_availability) if total_planned_runtime_sec > 0 else 0.0
        perfect_amount_for_runtime = total_running_sec * ideal_rate_per_sec

        _logger.info(f"For machine {workcenter.name}: total_produced={total_produced}, perfect_amount_for_runtime={perfect_amount_for_runtime}, total_planned_runtime_sec={total_planned_runtime_sec}")
        
        waste_losses = 1 - (total_produced / perfect_amount_for_runtime) if (total_planned_runtime_sec > 0 and perfect_amount_for_runtime > 0) else 0.0
        waste_losses = max(0.0, waste_losses)

        return {
            'availability': round(availability * 100, 2),
            'performance': round(performance * 100, 2),
            'quality': round(quality * 100, 2),
            'oee': round(oee * 100, 2),
            'waste_losses': round(waste_losses * 100, 2),
            'downtime_losses': round(downtime_losses * 100, 2),
            'total_produced': total_produced,
            'runtime_formatted': runtime_formatted,
        }

    def get_realtime_oee(self, runtime_event, production_count, workcenter=None):
        self.ensure_one()
        
        start_time, shift_end = self._get_current_shift_window()
        if not start_time or not shift_end:
            return {'error': 'Configuration error: No active shift. Calculation is not possible.'}
            
        state_tag, running_plc_value = runtime_event.get_mapping_for_machine(self)
        good_count_tag, is_cumulative = production_count.get_count_config_for_machine(self)

        if not state_tag or not good_count_tag:
            return {'error': 'Configuration error: Missing state or count tag for this machine.'}

        if not workcenter:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', self.id)], limit=1)

        active_intervals, total_planned_runtime_sec = self._get_planned_working_intervals(start_time, shift_end, workcenter)

        calc_end_time = min(fields.Datetime.now(), shift_end)

        ts_manager = self.env['mes.timescale.base']
        with ts_manager._connection() as conn:
            with conn.cursor() as cur:
                total_produced, top_alarm_str, top_rejection_str = self._fetch_shift_metrics(
                    cur, start_time, calc_end_time, good_count_tag, is_cumulative, production_count.id
                )
                
                total_running_sec = self._fetch_active_runtime(
                    cur, state_tag, running_plc_value, active_intervals
                )
                
                first_running_time = self._fetch_first_start_time(
                    cur, state_tag, running_plc_value, active_intervals
                )

        kpi_results = self._calculate_kpi(
            total_running_sec, total_produced, total_planned_runtime_sec, workcenter
        )
        
        kpi_results.update({
            'first_running_time': first_running_time,
            'top_alarm': top_alarm_str,
            'top_rejection': top_rejection_str
        })

        return kpi_results

    def action_open_waste_losses(self):
        self.ensure_one()
        return {
            'name': 'Waste Losses Details',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.waste.loss.stat',
            'view_mode': 'tree',
            'domain': [('machine_id', '=', self.id)],
            'context': {'default_machine_id': self.id},
            'target': 'new',
        }

    def action_open_downtime_losses(self):
        self.ensure_one()
        return {
            'name': 'Downtime Losses Details',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.downtime.loss.stat',
            'view_mode': 'tree',
            'domain': [('machine_id', '=', self.id)],
            'context': {'default_machine_id': self.id},
            'target': 'new',
        }

class MesSignalBase(models.AbstractModel):
    _name = 'mes.signal.base'
    _description = 'Base Signal Config'
    _inherit = ['mes.timescale.base']

    tag_name = fields.Char(string='Signal Tag', required=True)
    poll_type = fields.Selection([('cyclic', 'Cyclic'), ('on_change', 'On Change')], default='cyclic', required=True)
    poll_frequency = fields.Integer(string='Freq (ms)', default=1000)
    param_type = fields.Selection([
        ('auto', 'Auto'), ('bool', 'Boolean'), ('int', 'Integer'),
        ('double', 'Double/Real'), ('string', 'String')
    ], string='Data Type', default='auto', required=True)

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._sync(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync(rec)
        return res

    def _sync(self, rec):
        self._execute_from_file('upsert_signal.sql', (
            rec.machine_id.name, rec.tag_name, 
            rec.poll_type, rec.poll_frequency, rec.param_type, self._signal_type
        ))


class MesSignalCount(models.Model):
    _name = 'mes.signal.count'
    _inherit = 'mes.signal.base'
    _description = 'Count Signals'
    _signal_type = 'count'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    count_id = fields.Many2one('mes.counts', string='Dictionary Count', required=True)

    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)

    _sql_constraints = [
        ('tag_uniq', 'unique(machine_id, tag_name)', 'Count Tag must be unique for this machine!'),
        ('dict_uniq', 'unique(machine_id, count_id)', 'This dictionary count is already added for this machine!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

    @api.onchange('count_id')
    def _onchange_count_id(self):
        if self.count_id:
            self.is_cumulative = self.count_id.is_cumulative


class MesSignalEvent(models.Model):
    _name = 'mes.signal.event'
    _inherit = 'mes.signal.base'
    _description = 'Event Signals'
    _signal_type = 'event'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    event_id = fields.Many2one('mes.event', string='Dictionary Event', required=True)
    plc_value = fields.Integer(string='PLC Value', required=True)

    _sql_constraints = [
        ('tag_val_uniq', 'unique(machine_id, tag_name, plc_value)', 'Tag + Value pair already exists for this machine!'),
        ('dict_uniq', 'unique(machine_id, event_id)', 'This dictionary event is already configured for this machine!')
    ]

    def unlink(self):
        for rec in self:
            remaining = self.search_count([
                ('machine_id', '=', rec.machine_id.id),
                ('tag_name', '=', rec.tag_name),
                ('id', '!=', rec.id)
            ])
            if remaining == 0:
                self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()


class MesSignalProcess(models.Model):
    _name = 'mes.signal.process'
    _inherit = 'mes.signal.base'
    _description = 'Process Signals'
    _signal_type = 'process'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    process_id = fields.Many2one('mes.process', string='Dictionary Process', required=True)

    _sql_constraints = [
        ('tag_uniq', 'unique(machine_id, tag_name)', 'Process Tag must be unique for this machine!'),
        ('dict_uniq', 'unique(machine_id, process_id)', 'This dictionary process is already added for this machine!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

class MesWasteLossStat(models.TransientModel):
    _name = 'mes.waste.loss.stat'
    _description = 'Waste Losses Statistics'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine')
    name = fields.Char(string='Waste Type (Count)')
    waste_sum = fields.Float(string='Shift Total (pcs)')
    waste_per_hour = fields.Float(string='Waste per Hour (pcs/h)')

    @api.model
    def search(self, args, offset=0, limit=None, order=None, count=False):

        machine_id = None
        if args:
            for arg in args:
                if isinstance(arg, (list, tuple)) and len(arg) == 3 and arg[0] == 'machine_id' and arg[1] == '=':
                    machine_id = arg[2]
                    break
                    
        if machine_id and not self.env.context.get('skip_generation'):
            self.with_context(skip_generation=True).search([('machine_id', '=', machine_id)]).unlink()
            self._generate_stats(machine_id)
            
        return super().search(args, offset=offset, limit=limit, order=order, count=count)

    def _generate_stats(self, machine_id):
        machine = self.env['mes.machine.settings'].browse(machine_id)
        if not machine.exists(): return
        
        start_time, shift_end = machine._get_current_shift_window()
        if not start_time: return
        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        total_running_sec = 0.0
        good_tag = False
        
        if workcenter:
            if hasattr(workcenter, 'runtime_event_id') and workcenter.runtime_event_id:
                state_tag, running_plc_value = workcenter.runtime_event_id.get_mapping_for_machine(machine)
                if state_tag:
                    ts_manager = self.env['mes.timescale.base']
                    with ts_manager._connection() as conn:
                        with conn.cursor() as cur:
                            total_running_sec = machine._fetch_active_runtime(cur, state_tag, running_plc_value, active_intervals)
                            
            if hasattr(workcenter, 'production_count_id') and workcenter.production_count_id:
                good_tag, _ = workcenter.production_count_id.get_count_config_for_machine(machine)

        hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0
        
        all_counts = self.env['mes.signal.count'].search([('machine_id', '=', machine.id)])
        vals_list = []
        
        ts_manager = self.env['mes.timescale.base']
        with ts_manager._connection() as conn:
            with conn.cursor() as cur:
                for count_def in all_counts:
                    if good_tag and count_def.tag_name == good_tag:
                        continue
                        
                    tag_name = count_def.tag_name
                    is_cum = count_def.is_cumulative
                    
                    prod_sql = "SELECT COALESCE(MAX(value) - MIN(value), 0)" if is_cum else "SELECT COALESCE(SUM(value), 0)"
                    query = f"{prod_sql} FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s"
                    cur.execute(query, (machine.name, tag_name, start_time, calc_end_time))
                    res = cur.fetchone()
                    waste_sum = float(res[0] if res and res[0] else 0.0)
                    
                    if waste_sum > 0:
                        vals_list.append({
                            'machine_id': machine.id,
                            'name': count_def.count_id.name if count_def.count_id else tag_name,
                            'waste_sum': waste_sum,
                            'waste_per_hour': (waste_sum / hours_run) if hours_run > 0 else 0.0
                        })
                    
        if vals_list:
            self.with_context(skip_generation=True).create(vals_list)


class MesDowntimeLossStat(models.TransientModel):
    _name = 'mes.downtime.loss.stat'
    _description = 'Downtime Losses Statistics'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine')
    name = fields.Char(string='Event')
    frequency = fields.Integer(string='Frequency')
    freq_per_hour = fields.Float(string='Frequency per Hour')
    total_time = fields.Float(string='Total Time (min)')
    time_per_hour = fields.Float(string='Time per Hour (min/h)')

    @api.model
    def search(self, args, offset=0, limit=None, order=None, count=False):
        machine_id = None
        if args:
            for arg in args:
                if isinstance(arg, (list, tuple)) and len(arg) == 3 and arg[0] == 'machine_id' and arg[1] == '=':
                    machine_id = arg[2]
                    break
                    
        if machine_id and not self.env.context.get('skip_generation'):
            self.with_context(skip_generation=True).search([('machine_id', '=', machine_id)]).unlink()
            self._generate_stats(machine_id)
            
        return super().search(args, offset=offset, limit=limit, order=order, count=count)

    def _generate_stats(self, machine_id):
        machine = self.env['mes.machine.settings'].browse(machine_id)
        if not machine.exists(): return
        
        start_time, shift_end = machine._get_current_shift_window()
        if not start_time: return
        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        total_running_sec = 0.0
        if workcenter and hasattr(workcenter, 'runtime_event_id') and workcenter.runtime_event_id:
            state_tag, running_plc_value = workcenter.runtime_event_id.get_mapping_for_machine(machine)
            if state_tag:
                ts_manager = self.env['mes.timescale.base']
                with ts_manager._connection() as conn:
                    with conn.cursor() as cur:
                        total_running_sec = machine._fetch_active_runtime(cur, state_tag, running_plc_value, active_intervals)
                        
        hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0
        alarm_tag = machine.get_alarm_tag_name('OEE.nStopRootReason')
        
        query = f"""
            WITH alarm_boundary AS (
                SELECT time, value, 0::bigint as id FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time < %s ORDER BY time DESC LIMIT 1
            ),
            alarm_events AS (
                SELECT time, value, id FROM alarm_boundary UNION ALL
                SELECT time, value, id FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time >= %s AND time <= %s
            ),
            alarm_durations AS (
                SELECT value as alarm_code, EXTRACT(EPOCH FROM (COALESCE(LEAD(time) OVER (ORDER BY time), %s) - time)) as duration_sec
                FROM alarm_events
            )
            SELECT alarm_code, COUNT(alarm_code) as freq, SUM(duration_sec) as total_dur FROM alarm_durations
            WHERE alarm_code IS NOT NULL AND alarm_code != 0
            GROUP BY alarm_code;
        """
        
        ts_manager = self.env['mes.timescale.base']
        vals_list = []
        with ts_manager._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (
                    machine.name, alarm_tag, start_time,
                    machine.name, alarm_tag, start_time, calc_end_time, calc_end_time
                ))
                
                rows = cur.fetchall()
                for row in rows:
                    alarm_code = row[0]
                    freq = row[1] or 0
                    duration_sec = row[2] or 0.0
                    duration_min = duration_sec / 60.0
                    
                    if duration_min > 0 or freq > 0:
                        alarm_name = machine.resolve_plc_value_to_name(alarm_code)
                        vals_list.append({
                            'machine_id': machine.id,
                            'name': alarm_name,
                            'frequency': freq,
                            'freq_per_hour': (freq / hours_run) if hours_run > 0 else 0.0,
                            'total_time': duration_min,
                            'time_per_hour': (duration_min / hours_run) if hours_run > 0 else 0.0
                        })
        
        if vals_list:
            self.with_context(skip_generation=True).create(vals_list)
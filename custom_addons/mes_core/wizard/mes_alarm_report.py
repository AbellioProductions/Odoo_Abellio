from odoo import models, fields, api

class MesAlarmReportWizard(models.TransientModel):
    _name = 'mes.alarm.report.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Alarms SKD Wizard'

    event_filter_type = fields.Selection([
        ('in', 'In List'),
        ('not_in', 'Not in List')
    ], string="Event Condition", default='in', required=True)
    event_ids = fields.Many2many('mes.event', string="Events")

    row_by_event = fields.Boolean("Alarm / Event", default=True)
    col_by_event = fields.Boolean("Alarm / Event", default=False)

    show_frequency = fields.Boolean("Frequency (Count)", default=True)
    show_freq_per_hour = fields.Boolean("Frequency per Hour Run", default=True)
    show_total_time = fields.Boolean("Total Duration (min)", default=False)
    show_avg_time_per_stop = fields.Boolean("Avg Duration per Stop (min)", default=False)
    show_time_per_hour = fields.Boolean("Duration per Hour Run", default=False)

    @api.model
    def _get_limit_by_options(self):
        return [
            ('frequency', 'Frequency'),
            ('freq_per_hour', 'Frequency / Hour'),
            ('total_time', 'Total Time'),
            ('avg_time_per_stop', 'Avg Time / Stop'),
            ('time_per_hour', 'Time / Hour')
        ]

    def action_generate_report(self):
        self.env['mes.alarm.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        if not machines:
            return

        lines = []
        for machine in machines:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not workcenter:
                continue

            tz_name = workcenter.company_id.tz or 'UTC'
            shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)], order='start_hour asc')
            periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)

            state_sig = machine.event_tag_ids.filtered(lambda x: x.event_id == workcenter.runtime_event_id) if workcenter else None
            
            state_tag = state_sig[0].tag_name if state_sig else None
            running_plc_val = state_sig[0].plc_value if state_sig else 0
            alarm_tag = machine.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')

            for p_name, time_blocks in periods_dict.items():
                if not time_blocks:
                    continue

                all_active_intervals = []
                for t_s, t_e in time_blocks:
                    act_int, _ = machine._get_planned_working_intervals(t_s, t_e, workcenter)
                    all_active_intervals.extend(act_int)
                    
                all_active_intervals = self._merge_intervals(all_active_intervals)
                if not all_active_intervals: 
                    continue
                    
                with self.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        run_sec = machine._fetch_interval_stats(
                            cur, all_active_intervals, [state_tag], mode='runtime', 
                            state_tag=state_tag, state_val=running_plc_val
                        ) if state_tag else 0.0
                        hours_run = run_sec / 3600.0
                        
                        rows = machine._fetch_interval_stats(cur, all_active_intervals, [alarm_tag], mode='downtime')
                        if not rows: 
                            continue
                        
                        stats_by_event = {}
                        for row in rows:
                            t_name, a_code, freq, dur_sec = row[0], row[1], row[2], row[3]
                            matched = machine.event_tag_ids.filtered(lambda x: x.tag_name == t_name and x.plc_value == a_code)
                            if not matched: 
                                continue
                            
                            for sig in matched:
                                if not self._is_item_allowed(sig.event_id.id, self.event_ids.ids, self.event_filter_type):
                                    continue 
                                    
                                evt_name = sig.event_id.name
                                if evt_name not in stats_by_event:
                                    stats_by_event[evt_name] = {'freq': 0, 'dur': 0.0}
                                stats_by_event[evt_name]['freq'] += freq
                                stats_by_event[evt_name]['dur'] += dur_sec
                                
                        for evt_name, data in stats_by_event.items():
                            freq = data['freq']
                            dur_min = data['dur'] / 60.0
                            if freq > 0 or dur_min > 0:
                                
                                row_parts = []
                                if self.row_by_machine: row_parts.append(machine.name)
                                if self.row_by_event: row_parts.append(evt_name)
                                if self.row_by_period: row_parts.append(p_name)
                                r_label = " | ".join(row_parts) if row_parts else "All Data"

                                col_parts = []
                                if self.col_by_machine: col_parts.append(machine.name)
                                if self.col_by_event: col_parts.append(evt_name)
                                if self.col_by_period: col_parts.append(p_name)
                                c_label = " | ".join(col_parts) if col_parts else "All Data"

                                lines.append({
                                    'user_id': self.env.user.id,
                                    'machine_id': machine.id,
                                    'period_name': p_name,
                                    'event_name': evt_name,
                                    'row_group_label': r_label,
                                    'col_group_label': c_label,
                                    'frequency': freq,
                                    'freq_per_hour': (freq / hours_run) if hours_run > 0 else 0.0,
                                    'total_time': dur_min,
                                    'avg_time_per_stop': (dur_min / freq) if freq > 0 else 0.0,
                                    'time_per_hour': (dur_min / hours_run) if hours_run > 0 else 0.0
                                })
        
        if lines:
            lines.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0:
                lines = lines[:self.record_limit]
            self.env['mes.alarm.report.line'].create(lines)

        measures = []
        if self.show_frequency: measures.append('frequency')
        if self.show_freq_per_hour: measures.append('freq_per_hour')
        if self.show_total_time: measures.append('total_time')
        if self.show_avg_time_per_stop: measures.append('avg_time_per_stop')
        if self.show_time_per_hour: measures.append('time_per_hour')
        if not measures: measures.append('frequency')

        ctx = self._build_skd_context(measures)

        return {
            'name': 'Alarms Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.alarm.report.line',
            'view_mode': 'pivot,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': ctx
        }

class MesAlarmReportLine(models.Model):
    _name = 'mes.alarm.report.line'
    _description = 'Alarm Report Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")
    event_name = fields.Char(string="Alarm / Event")
    
    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    frequency = fields.Integer(string="Freq", group_operator="sum")
    freq_per_hour = fields.Float(string="Freq / Hour", group_operator="avg")
    total_time = fields.Float(string="Total Time", group_operator="sum")
    avg_time_per_stop = fields.Float(string="Avg Time/Stop", group_operator="avg")
    time_per_hour = fields.Float(string="Time / Hour", group_operator="avg")
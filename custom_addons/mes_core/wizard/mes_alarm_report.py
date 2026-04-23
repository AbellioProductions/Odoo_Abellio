from odoo import models, fields, api
from datetime import timedelta
import pytz

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

    @api.model
    def _get_uncovered_intervals(self, start_utc, end_utc, covered_windows):
        covered_windows.sort(key=lambda x: x[0])
        uncovered = []
        current = start_utc
        for c_start, c_end in covered_windows:
            if c_start > current:
                uncovered.append((current, min(c_start, end_utc)))
            current = max(current, c_end)
        if current < end_utc:
            uncovered.append((current, end_utc))
        return uncovered

    def action_generate_report(self):
        self.env['mes.alarm.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        if not machines: return

        lines = []
        now_utc = fields.Datetime.now()

        for machine in machines:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not workcenter: continue

            tz_name = workcenter.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_name)
            shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)], order='start_hour asc')
            periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)

            valid_docs = []
            docs = self.env['mes.machine.performance'].search([
                ('machine_id', '=', workcenter.id),
                ('date', '>=', (self.start_datetime - timedelta(days=1)).date()),
                ('date', '<=', (self.end_datetime + timedelta(days=1)).date())
            ])
            for doc in docs:
                s_loc, e_loc = doc._get_local_shift_times()
                d_start_utc = mac_tz.localize(s_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
                d_end_utc = mac_tz.localize(e_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
                
                if d_start_utc >= self.start_datetime and d_end_utc <= self.end_datetime:
                    valid_docs.append((d_start_utc, d_end_utc, doc))

            for p_name, time_blocks in periods_dict.items():
                if not time_blocks: continue

                stats_by_event = {}
                total_hours_run = 0.0

                for t_s, t_e in time_blocks:
                    covered_windows = []
                    
                    for d_start_utc, d_end_utc, doc in valid_docs:
                        i_start = max(t_s, d_start_utc)
                        i_end = min(t_e, d_end_utc)
                        if i_start < i_end:
                            c_end = min(i_end, now_utc)
                            if i_start < c_end:
                                covered_windows.append((i_start, c_end))
                                
                                run_logs = self.env['mes.performance.running'].search([
                                    ('performance_id', '=', doc.id),
                                    ('start_time', '>=', i_start),
                                    ('start_time', '<', i_end)
                                ])
                                total_hours_run += sum(r.duration for r in run_logs) / 60.0
                                
                                alarms = self.env['mes.performance.alarm'].search([
                                    ('performance_id', '=', doc.id),
                                    ('start_time', '>=', i_start),
                                    ('start_time', '<', i_end)
                                ])
                                
                                for al in alarms:
                                    if not al.loss_id: continue
                                    evt_name = al.loss_id.name
                                    if evt_name not in stats_by_event:
                                        stats_by_event[evt_name] = {'freq': 0, 'dur': 0.0}
                                    
                                    stats_by_event[evt_name]['freq'] += 1
                                    if al.end_time:
                                        stats_by_event[evt_name]['dur'] += al.duration * 60.0
                                    else:
                                        stats_by_event[evt_name]['dur'] += (now_utc - al.start_time).total_seconds()

                    calc_end = min(t_e, now_utc)
                    if t_s < calc_end:
                        uncovered = self._get_uncovered_intervals(t_s, calc_end, covered_windows)
                        
                        for u_start, u_end in uncovered:
                            act_int, _ = machine._get_planned_working_intervals(u_start, u_end, workcenter)
                            if not act_int: continue
                            act_int = self._merge_intervals(act_int)
                            if not act_int: continue
                            
                            run_sec = machine._fetch_interval_stats(act_int, workcenter.id, mode='runtime')
                            total_hours_run += run_sec / 3600.0
                            
                            rows = machine._fetch_interval_stats(act_int, workcenter.id, mode='downtime')
                            if rows:
                                for row in rows:
                                    loss_id, freq, dur_sec = row[0], row[1], row[2]
                                    evt = self.env['mes.event'].browse(loss_id)
                                    evt_name = evt.name
                                    if evt_name not in stats_by_event:
                                        stats_by_event[evt_name] = {'freq': 0, 'dur': 0.0}
                                    stats_by_event[evt_name]['freq'] += freq
                                    stats_by_event[evt_name]['dur'] += dur_sec

                for evt_name, data in stats_by_event.items():
                    freq = data['freq']
                    dur_min = data['dur'] / 60.0

                    if freq > 0 or dur_min > 0:
                        evt_rec = self.env['mes.event'].search([('name', '=', evt_name)], limit=1)
                        if evt_rec and not self._is_item_allowed(evt_rec.id, self.event_ids.ids, self.event_filter_type):
                            continue
                            
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
                            'freq_per_hour': (freq / total_hours_run) if total_hours_run > 0 else 0.0,
                            'total_time': dur_min,
                            'avg_time_per_stop': (dur_min / freq) if freq > 0 else 0.0,
                            'time_per_hour': (dur_min / total_hours_run) if total_hours_run > 0 else 0.0
                        })

        if lines:
            lines.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0: lines = lines[:self.record_limit]
            self.env['mes.alarm.report.line'].create(lines)

        measures = [m for m, show in [
            ('frequency', self.show_frequency), ('freq_per_hour', self.show_freq_per_hour),
            ('total_time', self.show_total_time), ('avg_time_per_stop', self.show_avg_time_per_stop),
            ('time_per_hour', self.show_time_per_hour)
        ] if show] or ['frequency']

        return {
            'name': 'Alarms Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.alarm.report.line',
            'view_mode': 'pivot,graph,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': self._build_skd_context(measures)
        }

class MesAlarmReportLine(models.Model):
    _name = 'mes.alarm.report.line'
    _description = 'Alarm Report Matrix Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")
    event_name = fields.Char(string="Event/Alarm")

    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    frequency = fields.Integer(string="Frequency", group_operator="sum")
    freq_per_hour = fields.Float(string="Freq per Hour", group_operator="avg")
    total_time = fields.Float(string="Total Duration (min)", group_operator="sum")
    avg_time_per_stop = fields.Float(string="Avg Duration per Stop", group_operator="avg")
    time_per_hour = fields.Float(string="Duration per Hour", group_operator="avg")

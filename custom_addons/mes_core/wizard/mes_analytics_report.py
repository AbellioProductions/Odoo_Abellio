from odoo import models, fields, api
import pytz

class MesAnalyticsWizard(models.TransientModel):
    _name = 'mes.analytics.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Shift Analytics Matrix Wizard'

    show_produced = fields.Boolean("Produced Qty", default=True)
    show_runtime = fields.Boolean("Runtime (h)", default=True)
    show_waste = fields.Boolean("Waste Loss (%)", default=True)
    show_downtime = fields.Boolean("Downtime Loss (%)", default=True)
    show_oee = fields.Boolean("OEE (%)", default=True)
    
    show_top_reject = fields.Boolean("Top Reject", default=True)
    show_top_alarm = fields.Boolean("Top Alarm", default=True)

    show_availability = fields.Boolean("Availability (%)", default=False)
    show_performance = fields.Boolean("Performance (%)", default=False)
    show_quality = fields.Boolean("Quality (%)", default=False)

    limit_by = fields.Selection(
        selection='_get_limit_by_options',
        default='produced',
        required=True
    )

    @api.model
    def _get_limit_by_options(self):
        return [
            ('produced', 'Produced Qty'),
            ('runtime_hours', 'Runtime (h)'),
            ('waste_losses', 'Waste Loss (%)'),
            ('downtime_losses', 'Downtime Loss (%)'),
            ('oee', 'OEE (%)'),
        ]

    def action_generate_report(self):
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        if not machines:
            return

        lines_to_create = []
        ts_mgr = self.env['mes.timescale.base']

        with ts_mgr._connection() as conn:
            with conn.cursor() as cur:
                for machine in machines:
                    workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
                    if not workcenter:
                        continue
                    
                    tz_name = workcenter.company_id.tz or 'UTC'
                    shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)], order='sequence, start_hour asc')
                    periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)

                    state_sig = machine.event_tag_ids.filtered(lambda x: x.event_id == workcenter.runtime_event_id) if workcenter else None
                    state_tag = state_sig[0].tag_name if state_sig else None
                    running_plc_val = state_sig[0].plc_value if state_sig else 0
                    
                    try:
                        alarm_tag = machine.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
                    except Exception:
                        alarm_tag = 'OEE.nStopRootReason'

                    for p_name, time_blocks in periods_dict.items():
                        if not time_blocks:
                            continue
                        
                        p_start = min(t[0] for t in time_blocks)
                        p_end = max(t[1] for t in time_blocks)

                        kpi = machine._calculate_kpi_for_window(workcenter, p_start, p_end)
                        
                        if kpi and (kpi.get('oee') or kpi.get('produced')):
                            
                            all_active_intervals = []
                            for t_s, t_e in time_blocks:
                                act_int, _ = machine._get_planned_working_intervals(t_s, t_e, workcenter)
                                all_active_intervals.extend(act_int)
                            all_active_intervals = self._merge_intervals(all_active_intervals)
                            
                            runtime_h = 0.0
                            top_alarm_str = "-"
                            top_reject_str = "-"
                            
                            if all_active_intervals:
                                run_sec = machine._fetch_interval_stats(
                                    cur, all_active_intervals, [state_tag], mode='runtime', 
                                    state_tag=state_tag, state_val=running_plc_val
                                ) if state_tag else 0.0
                                runtime_h = run_sec / 3600.0

                                rows = machine._fetch_interval_stats(cur, all_active_intervals, [alarm_tag], mode='downtime')
                                if rows:
                                    stats_by_evt = {}
                                    for row in rows:
                                        t_name, a_code, freq, dur_sec = row[0], row[1], row[2], row[3]
                                        matched = machine.event_tag_ids.filtered(lambda x: x.tag_name == t_name and x.plc_value == a_code)
                                        if matched:
                                            evt_name = matched[0].event_id.name
                                            if evt_name not in stats_by_evt:
                                                stats_by_evt[evt_name] = {'freq': 0, 'dur': 0.0}
                                            stats_by_evt[evt_name]['freq'] += freq
                                            stats_by_evt[evt_name]['dur'] += dur_sec
                                            
                                    if stats_by_evt:
                                        top_evt = max(stats_by_evt.items(), key=lambda x: x[1]['dur'])
                                        top_alarm_str = f"{top_evt[0]} ({top_evt[1]['freq']} - {top_evt[1]['dur']/60.0:.1f}m)"

                                valid_count_tags = list(machine.count_tag_ids.mapped('tag_name'))
                                if valid_count_tags:
                                    cur.execute("""
                                        SELECT tag_name, 
                                               COALESCE(SUM(value), 0) as sum_val, 
                                               COALESCE(MAX(value) - MIN(value), 0) as cum_val
                                        FROM telemetry_count 
                                        WHERE machine_name = %s AND tag_name = ANY(%s) 
                                          AND time >= %s::timestamp AT TIME ZONE 'UTC' 
                                          AND time < %s::timestamp AT TIME ZONE 'UTC'
                                        GROUP BY tag_name
                                    """, (machine.name, valid_count_tags, p_start.strftime('%Y-%m-%d %H:%M:%S'), p_end.strftime('%Y-%m-%d %H:%M:%S')))
                                    
                                    rej_stats = {}
                                    for row in cur.fetchall():
                                        t_name, sum_val, cum_val = row
                                        sig = machine.count_tag_ids.filtered(lambda s: s.tag_name == t_name)
                                        if sig:
                                            qty = cum_val if sig[0].is_cumulative else sum_val
                                            if qty > 0:
                                                c_name = sig[0].count_id.name
                                                rej_stats[c_name] = rej_stats.get(c_name, 0) + float(qty)
                                    
                                    if rej_stats:
                                        top_rej = max(rej_stats.items(), key=lambda x: x[1])
                                        qty_ph = top_rej[1] / runtime_h if runtime_h > 0 else 0.0
                                        top_reject_str = f"{top_rej[0]} ({top_rej[1]:.0f} / {qty_ph:.1f}/h)"

                            def build_label(by_mac, by_per):
                                parts = []
                                if by_mac: parts.append(machine.name)
                                if by_per: parts.append(p_name)
                                return " | ".join(parts) if parts else "All Data"

                            r_label = build_label(self.row_by_machine, self.row_by_period)
                            c_label = build_label(self.col_by_machine, self.col_by_period)

                            lines_to_create.append({
                                'user_id': self.env.user.id,
                                'machine_id': machine.id,
                                'period_name': p_name,
                                'row_group_label': r_label,
                                'col_group_label': c_label,
                                'first_running_time': kpi.get('first_running_time', False),
                                'produced': kpi.get('produced', 0),
                                'runtime_hours': runtime_h,
                                'waste_losses': kpi.get('waste_losses', 0),
                                'downtime_losses': kpi.get('downtime_losses', 0),
                                'oee': kpi.get('oee', 0),
                                'top_reject': top_reject_str,
                                'top_alarm': top_alarm_str,
                                'availability': kpi.get('availability', 0),
                                'performance': kpi.get('performance', 0),
                                'quality': kpi.get('quality', 0),
                            })

        if lines_to_create:
            lines_to_create.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0:
                lines_to_create = lines_to_create[:self.record_limit]
            self.env['mes.analytics.report.line'].create(lines_to_create)

        measures = []
        if self.show_produced: measures.append('produced')
        if self.show_runtime: measures.append('runtime_hours')
        if self.show_waste: measures.append('waste_losses')
        if self.show_downtime: measures.append('downtime_losses')
        if self.show_oee: measures.append('oee')
        if self.show_availability: measures.append('availability')
        if self.show_performance: measures.append('performance')
        if self.show_quality: measures.append('quality')
        
        if not measures:
            measures = ['produced']

        ctx = self._build_skd_context(measures)

        return {
            'name': 'Shift Analytics Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.analytics.report.line',
            'view_mode': 'tree,pivot', 
            'domain': [('user_id', '=', self.env.user.id)],
            'context': ctx
        }

class MesAnalyticsReportLine(models.Model):
    _name = 'mes.analytics.report.line'
    _description = 'Analytics Report Matrix Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")

    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    first_running_time = fields.Datetime(string="First Start")
    
    produced = fields.Float("Produced Qty", group_operator="sum")
    runtime_hours = fields.Float("Runtime (h)", group_operator="sum")
    waste_losses = fields.Float("Waste Loss (%)", group_operator="avg")
    downtime_losses = fields.Float("Downtime Loss (%)", group_operator="avg")
    oee = fields.Float("OEE (%)", group_operator="avg")
    
    top_reject = fields.Char("Top Reject")
    top_alarm = fields.Char("Top Alarm")

    availability = fields.Float("Availability (%)", group_operator="avg")
    performance = fields.Float("Performance (%)", group_operator="avg")
    quality = fields.Float("Quality (%)", group_operator="avg")
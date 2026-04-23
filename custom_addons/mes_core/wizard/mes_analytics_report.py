from odoo import models, fields, api
import pytz

class MesAnalyticsWizard(models.TransientModel):
    _name = 'mes.analytics.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Shift Analytics Matrix Wizard'

    show_produced = fields.Boolean("Produced Qty", default=True)
    show_runtime = fields.Boolean("Runtime", default=True)
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
            ('runtime_hours', 'Runtime'),
            ('waste_losses', 'Waste Loss (%)'),
            ('downtime_losses', 'Downtime Loss (%)'),
            ('oee', 'OEE (%)'),
        ]

    def action_generate_report(self):
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        if not machines: return

        lines_to_create = []

        for machine in machines:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not workcenter: continue
            
            tz_name = workcenter.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_name)
            shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)], order='sequence, start_hour asc')
            periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)

            for p_name, time_blocks in periods_dict.items():
                if not time_blocks: continue
                
                p_start = min(t[0] for t in time_blocks)
                p_end = max(t[1] for t in time_blocks)

                kpi = machine._calculate_kpi_for_window(workcenter, p_start, p_end)
                if not kpi or not (kpi.get('oee') or kpi.get('produced')): continue
                    
                all_active_intervals = []
                for t_s, t_e in time_blocks:
                    act_int, _ = machine._get_planned_working_intervals(t_s, t_e, workcenter)
                    all_active_intervals.extend(act_int)
                all_active_intervals = self._merge_intervals(all_active_intervals)
                
                runtime_h = 0.0
                runtime_fmt = "00:00:00"
                top_alarm_str = "-"
                top_reject_str = "-"
                
                if all_active_intervals:
                    run_sec = machine._fetch_interval_stats(all_active_intervals, workcenter.id, mode='runtime')
                    
                    runtime_h = run_sec / 3600.0
                    
                    h = int(run_sec // 3600)
                    m = int((run_sec % 3600) // 60)
                    s = int(run_sec % 60)
                    runtime_fmt = f"{h:02d}:{m:02d}:{s:02d}"

                    alarm_rows = machine._fetch_interval_stats(all_active_intervals, workcenter.id, mode='downtime')
                    if alarm_rows:
                        top_evt = max(alarm_rows, key=lambda x: x[2])
                        loss_name = self.env['mes.event'].browse(top_evt[0]).name
                        top_alarm_str = f"{loss_name} ({top_evt[1]} - {top_evt[2]/60.0:.1f}m)"

                    doc = self.env['mes.machine.performance'].search([
                        ('machine_id', '=', workcenter.id),
                        ('state', '=', 'done')
                    ]).filtered(lambda d: d._get_utc_time(d._get_local_shift_times()[0]) <= p_start and d._get_utc_time(d._get_local_shift_times()[1]) >= p_end)

                    rej_stats = {}
                    if doc:
                        for rej in doc.rejection_ids:
                            c_name = rej.reason_id.name
                            rej_stats[c_name] = rej_stats.get(c_name, 0) + rej.qty
                    else:
                        valid_count_tags = list(machine.count_tag_ids.mapped('tag_name'))
                        if valid_count_tags:
                            p_s_wall = pytz.utc.localize(p_start).astimezone(mac_tz).replace(tzinfo=None)
                            p_e_wall = pytz.utc.localize(p_end).astimezone(mac_tz).replace(tzinfo=None)

                            s_str = p_s_wall.strftime('%Y-%m-%d %H:%M:%S')
                            e_str = p_e_wall.strftime('%Y-%m-%d %H:%M:%S')

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
                                        WHERE t.machine_name = %s AND t.tag_name = ANY(%s) 
                                          AND t.time >= %s AND t.time < %s
                                        GROUP BY t.tag_name
                                    """, (machine.name, s_str, machine.name, valid_count_tags, s_str, e_str))
                                    
                                    for row in cur.fetchall():
                                        t_name, sum_val, cum_val = row
                                        sig = machine.count_tag_ids.filtered(lambda s: s.tag_name == t_name)
                                        if sig:
                                            qty = cum_val if sig[0].is_cumulative else sum_val
                                            if qty > 0 and sig[0].count_id != workcenter.production_count_id:
                                                c_name = sig[0].count_id.name
                                                rej_stats[c_name] = rej_stats.get(c_name, 0) + float(qty)
                    
                    if rej_stats:
                        top_rej = max(rej_stats.items(), key=lambda x: x[1])
                        qty_ph = top_rej[1] / runtime_h if runtime_h > 0 else 0.0
                        top_reject_str = f"{top_rej[0]} ({top_rej[1]:.0f} / {qty_ph:.1f}/h)"

                r_label = " | ".join(filter(None, [machine.name if self.row_by_machine else "", p_name if self.row_by_period else ""])) or "All Data"
                c_label = " | ".join(filter(None, [machine.name if self.col_by_machine else "", p_name if self.col_by_period else ""])) or "All Data"

                lines_to_create.append({
                    'user_id': self.env.user.id,
                    'machine_id': machine.id,
                    'period_name': p_name,
                    'row_group_label': r_label,
                    'col_group_label': c_label,
                    'first_running_time': kpi.get('first_running_time', False),
                    'produced': kpi.get('produced', 0),
                    'runtime_hours': runtime_h,
                    'runtime_formatted': runtime_fmt,
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
            if self.record_limit > 0: lines_to_create = lines_to_create[:self.record_limit]
            self.env['mes.analytics.report.line'].create(lines_to_create)

        measures = [m for m, show in [
            ('produced', self.show_produced), 
            ('runtime_hours', self.show_runtime),
            ('waste_losses', self.show_waste), 
            ('downtime_losses', self.show_downtime), 
            ('oee', self.show_oee),
            ('availability', self.show_availability), 
            ('performance', self.show_performance), 
            ('quality', self.show_quality)
        ] if show] or ['produced']

        return {
            'name': 'Shift Analytics Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.analytics.report.line',
            'view_mode': 'pivot,graph,tree', 
            'domain': [('user_id', '=', self.env.user.id)],
            'context': self._build_skd_context(measures)
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
    runtime_formatted = fields.Char("Runtime") 
    
    waste_losses = fields.Float("Waste Loss (%)", group_operator="avg")
    downtime_losses = fields.Float("Downtime Loss (%)", group_operator="avg")
    oee = fields.Float("OEE (%)", group_operator="avg")
    
    top_reject = fields.Char("Top Reject")
    top_alarm = fields.Char("Top Alarm")

    availability = fields.Float("Availability (%)", group_operator="avg")
    performance = fields.Float("Performance (%)", group_operator="avg")
    quality = fields.Float("Quality (%)", group_operator="avg")

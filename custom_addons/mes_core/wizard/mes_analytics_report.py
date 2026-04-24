from odoo import models, fields, api
from datetime import timedelta, time, datetime
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
    limit_by = fields.Selection(selection='_get_limit_by_options', default='produced', required=True)

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
        self._clear_previous_report_lines()
        
        machines = self._get_filtered_machines()
        if not machines:
            return

        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', 'in', machines.ids)])
        if not workcenters:
            return

        context_data = self._initialize_context_data(machines, workcenters)
        
        doc_bounds = self._fetch_document_bounds(workcenters, context_data)
        blocks_data = self._build_time_blocks(machines, doc_bounds, context_data)

        report_data = self._process_performance_metrics(blocks_data)
        report_data = self._process_timescale_metrics(report_data, blocks_data, context_data)

        self._create_report_lines(report_data, context_data)
        return self._build_action_window()

    def _clear_previous_report_lines(self):
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

    def _initialize_context_data(self, machines, workcenters):
        now_utc = fields.Datetime.now()
        start_global = self.start_datetime
        end_global = min(self.end_datetime, now_utc)

        wc_dict = {wc.id: wc for wc in workcenters}
        mac_to_wc = {m.id: wc.id for wc in workcenters for m in machines if wc.machine_settings_id.id == m.id}

        tag_info = {}
        for machine in machines:
            wc = wc_dict.get(mac_to_wc.get(machine.id))
            if not wc:
                continue
            for signal in machine.count_tag_ids:
                if signal.count_id:
                    tag_info[(machine.name, signal.tag_name)] = {
                        'is_prod': signal.count_id.id == wc.production_count_id.id,
                        'r_id': signal.count_id.id,
                        'is_cum': signal.is_cumulative
                    }

        shifts = self.env['mes.shift'].search([('company_id', 'in', workcenters.mapped('company_id').ids)])
        shift_dict = {s.id: s for s in shifts}

        return {
            'now_utc': now_utc,
            'start_global': start_global,
            'end_global': end_global,
            'wc_dict': wc_dict,
            'mac_to_wc': mac_to_wc,
            'tag_info': tag_info,
            'shift_dict': shift_dict,
            'shifts': shifts,
            'machines': machines
        }

    def _fetch_document_bounds(self, workcenters, context_data):
        start_global = context_data['start_global']
        end_global = context_data['end_global']
        
        query = """
            SELECT id, machine_id, date, shift_id 
            FROM mes_machine_performance 
            WHERE machine_id IN %s AND state = 'done' 
            AND date >= %s AND date <= %s
        """
        params = (
            tuple(workcenters.ids),
            (start_global - timedelta(days=2)).date().isoformat(),
            (end_global + timedelta(days=2)).date().isoformat()
        )
        self.env.cr.execute(query, params)
        
        doc_bounds = []
        for row in self.env.cr.dictfetchall():
            wc = context_data['wc_dict'].get(row['machine_id'])
            shift = context_data['shift_dict'].get(row['shift_id'])
            
            if not wc or not shift:
                continue
                
            tz = pytz.timezone(wc.company_id.tz or 'UTC')
            s_t = datetime.combine(row['date'], time(hour=int(shift.start_hour), minute=int((shift.start_hour % 1) * 60)))
            e_t = s_t + timedelta(hours=shift.duration)
            
            s_u = tz.localize(s_t, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            e_u = tz.localize(e_t, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            
            if e_u > start_global and s_u < end_global:
                doc_bounds.append({'id': row['id'], 'm_id': wc.machine_settings_id.id, 's': s_u, 'e': e_u})

        return sorted(doc_bounds, key=lambda x: x['s'])

    def _build_time_blocks(self, machines, doc_bounds, context_data):
        b_p_names, b_m_ids, b_doc_ids, b_t_ss, b_t_es = [], [], [], [], []
        g_p_names, g_m_names, g_t_ss, g_t_es = [], [], [], []
        doc_ids_to_fetch = set()
        doc_p_map = {}

        for machine in machines:
            wc_id = context_data['mac_to_wc'].get(machine.id)
            if not wc_id:
                continue
                
            wc = context_data['wc_dict'][wc_id]
            tz_name = wc.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_name)
            mac_shifts = context_data['shifts'].filtered(lambda x: x.company_id.id == wc.company_id.id)
            
            periods_dict = self._get_logical_periods(context_data['start_global'], context_data['end_global'], mac_shifts, tz_name)
            global_act_int, _ = machine._get_planned_working_intervals(context_data['start_global'], context_data['end_global'], wc)
            global_act_int = self._merge_intervals(global_act_int)
            
            m_docs = [d for d in doc_bounds if d['m_id'] == machine.id]
            
            for p_name, t_blocks in periods_dict.items():
                if not t_blocks:
                    continue
                    
                p_s = min(t[0] for t in t_blocks)
                p_e = min(max(t[1] for t in t_blocks), context_data['now_utc'])
                if p_s >= p_e:
                    continue

                p_docs = [d for d in m_docs if d['e'] > p_s and d['s'] < p_e]
                
                for t_s, t_e in t_blocks:
                    for g_s, g_e in global_act_int:
                        i_s = max(t_s, g_s)
                        i_e = min(t_e, g_e, context_data['now_utc'])
                        if i_s < i_e:
                            overlapping_docs = [d for d in p_docs if d['e'] > i_s and d['s'] < i_e]
                            for d in overlapping_docs:
                                b_p_names.append(p_name)
                                b_m_ids.append(machine.id)
                                b_doc_ids.append(d['id'])
                                b_t_ss.append(i_s)
                                b_t_es.append(i_e)

                if p_docs:
                    for d in p_docs:
                        doc_ids_to_fetch.add(d['id'])
                        doc_p_map[d['id']] = (machine.id, p_name)
                        
                    f_s = min(d['s'] for d in p_docs)
                    l_e = max(d['e'] for d in p_docs)
                    tails = []
                    if p_s < f_s: tails.append((p_s, f_s))
                    if l_e < p_e: tails.append((l_e, p_e))
                else:
                    tails = [(p_s, p_e)]

                for tail_s, tail_e in tails:
                    for g_s, g_e in global_act_int:
                        a_s = max(tail_s, g_s)
                        a_e = min(tail_e, g_e)
                        if a_s < a_e:
                            s_loc = pytz.utc.localize(a_s).astimezone(mac_tz).replace(tzinfo=None)
                            e_loc = pytz.utc.localize(a_e).astimezone(mac_tz).replace(tzinfo=None)
                            g_p_names.append(p_name)
                            g_m_names.append(machine.name)
                            g_t_ss.append(s_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))
                            g_t_es.append(e_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))

        return {
            'b_p_names': b_p_names, 'b_m_ids': b_m_ids, 'b_doc_ids': b_doc_ids, 'b_t_ss': b_t_ss, 'b_t_es': b_t_es,
            'g_p_names': g_p_names, 'g_m_names': g_m_names, 'g_t_ss': g_t_ss, 'g_t_es': g_t_es,
            'doc_ids_to_fetch': doc_ids_to_fetch, 'doc_p_map': doc_p_map
        }

    def _process_performance_metrics(self, blocks_data):
        report_data = {}
        now_utc = fields.Datetime.now()
        
        if not blocks_data['b_p_names']:
            return report_data

        run_query = """
            WITH blocks AS (
                SELECT unnest(%s::varchar[]) AS p_name,
                       unnest(%s::int[]) AS m_id,
                       unnest(%s::int[]) AS d_id,
                       unnest(%s::timestamp[]) AS t_s,
                       unnest(%s::timestamp[]) AS t_e
            )
            SELECT b.p_name, b.m_id, MIN(r.start_time),
                   SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(r.end_time, %s), b.t_e) - GREATEST(r.start_time, b.t_s))))
            FROM blocks b
            JOIN mes_performance_running r ON r.performance_id = b.d_id
            WHERE r.start_time < b.t_e AND COALESCE(r.end_time, %s) > b.t_s
            GROUP BY b.p_name, b.m_id
        """
        params = (
            blocks_data['b_p_names'], blocks_data['b_m_ids'], blocks_data['b_doc_ids'], blocks_data['b_t_ss'], blocks_data['b_t_es'],
            now_utc, now_utc
        )
        self.env.cr.execute(run_query, params)
        for p_name, m_id, f_s, r_sec in self.env.cr.fetchall():
            d = self._get_empty_data_node(report_data, m_id, p_name)
            d['run_sec'] += (r_sec or 0)
            if not d['f_s'] or (f_s and f_s < d['f_s']): 
                d['f_s'] = f_s

        alarm_query = """
            WITH blocks AS (
                SELECT unnest(%s::varchar[]) AS p_name,
                       unnest(%s::int[]) AS m_id,
                       unnest(%s::int[]) AS d_id,
                       unnest(%s::timestamp[]) AS t_s,
                       unnest(%s::timestamp[]) AS t_e
            )
            SELECT b.p_name, b.m_id, a.loss_id,
                   SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(a.end_time, %s), b.t_e) - GREATEST(a.start_time, b.t_s))))
            FROM blocks b
            JOIN mes_performance_alarm a ON a.performance_id = b.d_id
            WHERE a.start_time < b.t_e AND COALESCE(a.end_time, %s) > b.t_s
            GROUP BY b.p_name, b.m_id, a.loss_id
        """
        self.env.cr.execute(alarm_query, params)
        for p_name, m_id, l_id, a_sec in self.env.cr.fetchall():
            d = self._get_empty_data_node(report_data, m_id, p_name)
            d['down_sec'] += (a_sec or 0)
            d['alarms'][l_id] = d['alarms'].get(l_id, 0) + (a_sec or 0)

        if blocks_data['doc_ids_to_fetch']:
            d_tup = tuple(blocks_data['doc_ids_to_fetch'])
            self.env.cr.execute("SELECT performance_id, SUM(qty) FROM mes_performance_production WHERE performance_id IN %s GROUP BY performance_id", (d_tup,))
            for d_id, qty in self.env.cr.fetchall():
                m_id, p_name = blocks_data['doc_p_map'][d_id]
                d = self._get_empty_data_node(report_data, m_id, p_name)
                d['prod_qty'] += (qty or 0)
                
            self.env.cr.execute("SELECT performance_id, reason_id, SUM(qty) FROM mes_performance_rejection WHERE performance_id IN %s GROUP BY performance_id, reason_id", (d_tup,))
            for d_id, r_id, qty in self.env.cr.fetchall():
                m_id, p_name = blocks_data['doc_p_map'][d_id]
                d = self._get_empty_data_node(report_data, m_id, p_name)
                d['rej_qty'] += (qty or 0)
                d['rejects'][r_id] = d['rejects'].get(r_id, 0) + (qty or 0)

        return report_data

    def _process_timescale_metrics(self, report_data, blocks_data, context_data):
        valid_tags = list(set(k[1] for k in context_data['tag_info'].keys()))
        if not blocks_data['g_t_ss'] or not valid_tags:
            return report_data

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                query = """
                    WITH gaps AS (
                        SELECT p_name, m_name, start_t, end_t, ord AS gap_id
                        FROM unnest(%s::varchar[], %s::varchar[], %s::timestamp[], %s::timestamp[]) 
                        WITH ORDINALITY AS u(p_name, m_name, start_t, end_t, ord)
                    ),
                    gap_stats AS (
                        SELECT g.p_name, g.m_name, t.tag_name, g.gap_id,
                               COALESCE(SUM(t.value), 0) AS sum_val,
                               COALESCE(MAX(t.value) - MIN(t.value), 0) AS cum_val
                        FROM telemetry_count t
                        JOIN gaps g ON t.machine_name = g.m_name AND t.time >= g.start_t AND t.time < g.end_t
                        WHERE t.tag_name = ANY(%s)
                        GROUP BY g.p_name, g.m_name, t.tag_name, g.gap_id
                    )
                    SELECT p_name, m_name, tag_name, SUM(sum_val), SUM(cum_val)
                    FROM gap_stats
                    GROUP BY p_name, m_name, tag_name
                """
                params = (
                    blocks_data['g_p_names'], blocks_data['g_m_names'], blocks_data['g_t_ss'], blocks_data['g_t_es'], valid_tags
                )
                cur.execute(query, params)
                
                mac_name_to_id = {m.name: m.id for m in context_data['machines']}
                for p_name, m_name, t_name, sum_val, cum_val in cur.fetchall():
                    ti = context_data['tag_info'].get((m_name, t_name))
                    m_id = mac_name_to_id.get(m_name)
                    if ti and m_id:
                        qty = cum_val if ti['is_cum'] else sum_val
                        if qty > 0:
                            d = self._get_empty_data_node(report_data, m_id, p_name)
                            if ti['is_prod']:
                                d['prod_qty'] += float(qty)
                            else:
                                d['rej_qty'] += float(qty)
                                r_id = ti['r_id']
                                d['rejects'][r_id] = d['rejects'].get(r_id, 0) + float(qty)

        return report_data

    def _create_report_lines(self, report_data, context_data):
        lines_to_create = []
        mac_cache = {m.id: m.name for m in context_data['machines']}
        event_cache = {e.id: e.name for e in self.env['mes.event'].search([])}
        count_cache = {c.id: c.name for c in self.env['mes.counts'].search([])}

        for (m_id, p_name), d in report_data.items():
            run_h = d['run_sec'] / 3600.0
            h, m_min, s = int(d['run_sec'] // 3600), int((d['run_sec'] % 3600) // 60), int(d['run_sec'] % 60)
            runtime_fmt = f"{h:02d}:{m_min:02d}:{s:02d}"

            total_qty = d['prod_qty'] + d['rej_qty']
            total_time = d['run_sec'] + d['down_sec']
            
            waste_losses = (d['rej_qty'] / total_qty * 100.0) if total_qty > 0 else 0.0
            downtime_losses = (d['down_sec'] / total_time * 100.0) if total_time > 0 else 0.0
            availability = (d['run_sec'] / total_time * 100.0) if total_time > 0 else 0.0
            quality = (d['prod_qty'] / total_qty * 100.0) if total_qty > 0 else 0.0
            performance = 100.0 if total_qty > 0 else 0.0
            oee = (availability * quality * performance) / 10000.0
            
            top_alarm_str = "-"
            if d['alarms']:
                top_loss_id, top_dur = max(d['alarms'].items(), key=lambda x: x[1])
                evt_name = event_cache.get(top_loss_id, str(top_loss_id))
                top_alarm_str = f"{evt_name} ({top_dur/60.0:.1f}m)"

            top_reject_str = "-"
            if d['rejects']:
                top_rej_id, top_qty = max(d['rejects'].items(), key=lambda x: x[1])
                cnt_name = count_cache.get(top_rej_id, str(top_rej_id))
                qty_ph = top_qty / run_h if run_h > 0 else 0.0
                top_reject_str = f"{cnt_name} ({top_qty:.0f} / {qty_ph:.1f}/h)"

            m_name = mac_cache[m_id]
            r_label = " | ".join(filter(None, [m_name if self.row_by_machine else "", p_name if self.row_by_period else ""])) or "All Data"
            c_label = " | ".join(filter(None, [m_name if self.col_by_machine else "", p_name if self.col_by_period else ""])) or "All Data"

            lines_to_create.append({
                'user_id': self.env.user.id,
                'machine_id': m_id,
                'period_name': p_name,
                'row_group_label': r_label,
                'col_group_label': c_label,
                'first_running_time': d['f_s'] if d['f_s'] else False,
                'produced': d['prod_qty'],
                'runtime_hours': run_h,
                'runtime_formatted': runtime_fmt,
                'waste_losses': waste_losses,
                'downtime_losses': downtime_losses,
                'oee': oee,
                'top_reject': top_reject_str,
                'top_alarm': top_alarm_str,
                'availability': availability,
                'performance': performance,
                'quality': quality,
            })

        if lines_to_create:
            lines_to_create.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0:
                lines_to_create = lines_to_create[:self.record_limit]
            self.env['mes.analytics.report.line'].create(lines_to_create)

    def _build_action_window(self):
        measures = [m for m, show in [
            ('produced', self.show_produced), ('runtime_hours', self.show_runtime),
            ('waste_losses', self.show_waste), ('downtime_losses', self.show_downtime), ('oee', self.show_oee),
            ('availability', self.show_availability), ('performance', self.show_performance), ('quality', self.show_quality)
        ] if show] or ['produced']

        return {
            'name': 'Shift Analytics Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.analytics.report.line',
            'view_mode': 'pivot,graph,tree', 
            'domain': [('user_id', '=', self.env.user.id)],
            'context': self._build_skd_context(measures)
        }

    def _get_empty_data_node(self, container, m_id, p_name):
        return container.setdefault((m_id, p_name), {
            'run_sec': 0.0, 'down_sec': 0.0, 'prod_qty': 0.0, 'rej_qty': 0.0, 
            'alarms': {}, 'rejects': {}, 'f_s': False
        })

class MesAnalyticsReportLine(models.TransientModel):
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

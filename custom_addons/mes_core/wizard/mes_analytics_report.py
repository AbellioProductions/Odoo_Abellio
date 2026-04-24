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
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()
        machines = self._get_filtered_machines()
        if not machines: return

        now_utc = fields.Datetime.now()
        start_global = self.start_datetime
        end_global = min(self.end_datetime, now_utc)
        
        data = {}
        def _empty():
            return {'run_sec': 0.0, 'down_sec': 0.0, 'prod_qty': 0.0, 'rej_qty': 0.0, 'alarms': {}, 'rejects': {}, 'f_s': False}

        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', 'in', machines.ids)])
        if not workcenters: return
        wc_dict = {wc.id: wc for wc in workcenters}
        mac_to_wc = {m.id: wc.id for wc in workcenters for m in machines if wc.machine_settings_id.id == m.id}

        tag_info = {}
        for m in machines:
            wc = wc_dict.get(mac_to_wc.get(m.id))
            if not wc: continue
            for s in m.count_tag_ids:
                if s.count_id:
                    tag_info[(m.name, s.tag_name)] = {
                        'is_prod': s.count_id.id == wc.production_count_id.id,
                        'r_id': s.count_id.id,
                        'is_cum': s.is_cumulative
                    }

        shifts = self.env['mes.shift'].search([('company_id', 'in', workcenters.mapped('company_id').ids)])
        shift_dict = {s.id: s for s in shifts}

        self.env.cr.execute("""
            SELECT id, machine_id, date, shift_id 
            FROM mes_machine_performance 
            WHERE machine_id IN %s AND state = 'done' 
            AND date >= %s AND date <= %s
        """, (tuple(workcenters.ids), (start_global - timedelta(days=2)).date().isoformat(), (end_global + timedelta(days=2)).date().isoformat()))
        
        doc_bounds = []
        for d in self.env.cr.dictfetchall():
            wc = wc_dict.get(d['machine_id'])
            shift = shift_dict.get(d['shift_id'])
            if not wc or not shift: continue
            tz = pytz.timezone(wc.company_id.tz or 'UTC')
            s_t = datetime.combine(d['date'], time(hour=int(shift.start_hour), minute=int((shift.start_hour % 1) * 60)))
            e_t = s_t + timedelta(hours=shift.duration)
            s_u = tz.localize(s_t, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            e_u = tz.localize(e_t, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            if e_u > start_global and s_u < end_global:
                doc_bounds.append({'id': d['id'], 'm_id': wc.machine_settings_id.id, 's': s_u, 'e': e_u})

        doc_bounds.sort(key=lambda x: x['s'])
        
        b_p_names, b_m_ids, b_t_ss, b_t_es = [], [], [], []
        g_p_names, g_m_names, g_t_ss, g_t_es = [], [], [], []
        doc_ids_to_fetch = set()
        doc_p_map = {}

        for m in machines:
            wc_id = mac_to_wc.get(m.id)
            if not wc_id: continue
            wc = wc_dict[wc_id]
            tz_name = wc.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_name)
            mac_shifts = shifts.filtered(lambda x: x.company_id.id == wc.company_id.id)
            periods_dict = self._get_logical_periods(start_global, end_global, mac_shifts, tz_name)
            
            global_act_int, _ = m._get_planned_working_intervals(start_global, end_global, wc)
            global_act_int = self._merge_intervals(global_act_int)
            
            m_docs = [d for d in doc_bounds if d['m_id'] == m.id]
            
            for p_name, t_blocks in periods_dict.items():
                if not t_blocks: continue
                p_s = min(t[0] for t in t_blocks)
                p_e = min(max(t[1] for t in t_blocks), now_utc)
                if p_s >= p_e: continue

                for t_s, t_e in t_blocks:
                    for g_s, g_e in global_act_int:
                        i_s = max(t_s, g_s)
                        i_e = min(t_e, g_e, now_utc)
                        if i_s < i_e:
                            b_p_names.append(p_name)
                            b_m_ids.append(m.id)
                            b_t_ss.append(i_s)
                            b_t_es.append(i_e)

                p_docs = [d for d in m_docs if d['e'] > p_s and d['s'] < p_e]
                if p_docs:
                    for d in p_docs:
                        doc_ids_to_fetch.add(d['id'])
                        doc_p_map[d['id']] = (m.id, p_name)
                    
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
                            g_m_names.append(m.name)
                            g_t_ss.append(s_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))
                            g_t_es.append(e_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))

        if b_p_names:
            self.env.cr.execute("""
                WITH blocks AS (
                    SELECT unnest(%s::varchar[]) AS p_name,
                           unnest(%s::int[]) AS m_id,
                           unnest(%s::timestamp[]) AS t_s,
                           unnest(%s::timestamp[]) AS t_e
                )
                SELECT b.p_name, b.m_id, MIN(r.start_time),
                       SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(r.end_time, %s), b.t_e) - GREATEST(r.start_time, b.t_s))))
                FROM blocks b
                JOIN mrp_workcenter wc ON wc.machine_settings_id = b.m_id
                JOIN mes_machine_performance doc ON doc.machine_id = wc.id
                JOIN mes_performance_running r ON r.performance_id = doc.id
                WHERE r.start_time < b.t_e AND COALESCE(r.end_time, %s) > b.t_s
                GROUP BY b.p_name, b.m_id
            """, (b_p_names, b_m_ids, b_t_ss, b_t_es, now_utc, now_utc))
            
            for p_name, m_id, f_s, r_sec in self.env.cr.fetchall():
                d = data.setdefault((m_id, p_name), _empty())
                d['run_sec'] += (r_sec or 0)
                if not d['f_s'] or (f_s and f_s < d['f_s']): d['f_s'] = f_s

            self.env.cr.execute("""
                WITH blocks AS (
                    SELECT unnest(%s::varchar[]) AS p_name,
                           unnest(%s::int[]) AS m_id,
                           unnest(%s::timestamp[]) AS t_s,
                           unnest(%s::timestamp[]) AS t_e
                )
                SELECT b.p_name, b.m_id, a.loss_id,
                       SUM(EXTRACT(EPOCH FROM (LEAST(COALESCE(a.end_time, %s), b.t_e) - GREATEST(a.start_time, b.t_s))))
                FROM blocks b
                JOIN mrp_workcenter wc ON wc.machine_settings_id = b.m_id
                JOIN mes_machine_performance doc ON doc.machine_id = wc.id
                JOIN mes_performance_alarm a ON a.performance_id = doc.id
                WHERE a.start_time < b.t_e AND COALESCE(a.end_time, %s) > b.t_s
                GROUP BY b.p_name, b.m_id, a.loss_id
            """, (b_p_names, b_m_ids, b_t_ss, b_t_es, now_utc, now_utc))
            
            for p_name, m_id, l_id, a_sec in self.env.cr.fetchall():
                d = data.setdefault((m_id, p_name), _empty())
                d['down_sec'] += (a_sec or 0)
                d['alarms'][l_id] = d['alarms'].get(l_id, 0) + (a_sec or 0)

        if doc_ids_to_fetch:
            d_tup = tuple(doc_ids_to_fetch)
            self.env.cr.execute("""
                SELECT performance_id, SUM(qty) FROM mes_performance_production
                WHERE performance_id IN %s GROUP BY performance_id
            """, (d_tup,))
            for d_id, qty in self.env.cr.fetchall():
                m_id, p_name = doc_p_map[d_id]
                d = data.setdefault((m_id, p_name), _empty())
                d['prod_qty'] += (qty or 0)
                
            self.env.cr.execute("""
                SELECT performance_id, reason_id, SUM(qty) FROM mes_performance_rejection
                WHERE performance_id IN %s GROUP BY performance_id, reason_id
            """, (d_tup,))
            for d_id, r_id, qty in self.env.cr.fetchall():
                m_id, p_name = doc_p_map[d_id]
                d = data.setdefault((m_id, p_name), _empty())
                d['rej_qty'] += (qty or 0)
                d['rejects'][r_id] = d['rejects'].get(r_id, 0) + (qty or 0)

        valid_tags = list(set(k[1] for k in tag_info.keys()))
        if g_t_ss and valid_tags:
            with self.env['mes.timescale.base']._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        WITH gaps AS (
                            SELECT unnest(%s::varchar[]) AS p_name,
                                   unnest(%s::varchar[]) AS m_name,
                                   unnest(%s::timestamp[]) AS start_t,
                                   unnest(%s::timestamp[]) AS end_t,
                                   generate_series(1, array_length(%s::varchar[], 1)) AS gap_id
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
                    """, (g_p_names, g_m_names, g_t_ss, g_t_es, g_p_names, valid_tags))
                    
                    mac_name_to_id = {m.name: m.id for m in machines}
                    for p_name, m_name, t_name, sum_val, cum_val in cur.fetchall():
                        ti = tag_info.get((m_name, t_name))
                        m_id = mac_name_to_id.get(m_name)
                        if ti and m_id:
                            qty = cum_val if ti['is_cum'] else sum_val
                            if qty > 0:
                                d = data.setdefault((m_id, p_name), _empty())
                                if ti['is_prod']:
                                    d['prod_qty'] += float(qty)
                                else:
                                    d['rej_qty'] += float(qty)
                                    r_id = ti['r_id']
                                    d['rejects'][r_id] = d['rejects'].get(r_id, 0) + float(qty)

        lines_to_create = []
        mac_cache = {m.id: m.name for m in machines}
        event_cache = {e.id: e.name for e in self.env['mes.event'].search([])}
        count_cache = {c.id: c.name for c in self.env['mes.counts'].search([])}

        for (m_id, p_name), d in data.items():
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
            if self.record_limit > 0: lines_to_create = lines_to_create[:self.record_limit]
            self.env['mes.analytics.report.line'].create(lines_to_create)

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

from odoo import models, fields, api
from datetime import timedelta, time, datetime
import pytz

class MesRejectReportWizard(models.TransientModel):
    _name = 'mes.reject.report.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Reject Report Matrix Wizard'

    cnt_filter_type = fields.Selection([('in', 'In List'), ('not_in', 'Not in List')], string="Count Condition", default='in', required=True)
    cnt_ids = fields.Many2many('mes.counts', string="Reject Counts")

    row_by_cnt = fields.Selection([('none', 'None'), ('flat', 'Element Only'), ('hierarchy', 'Hierarchy Only'), ('full', 'Hierarchy + Element')], string="Count", default='flat', required=True)
    row_by_is_mod = fields.Boolean("Is Module", default=False)
    row_by_wheel = fields.Boolean("Wheel Number", default=False)
    row_by_mod = fields.Boolean("Module Number", default=False)

    col_by_cnt = fields.Selection([('none', 'None'), ('flat', 'Element Only'), ('hierarchy', 'Hierarchy Only'), ('full', 'Hierarchy + Element')], string="Count", default='none', required=True)
    col_by_is_mod = fields.Boolean("Is Module", default=False)
    col_by_wheel = fields.Boolean("Wheel Number", default=False)
    col_by_mod = fields.Boolean("Module Number", default=False)

    show_qty = fields.Boolean("Total Quantity (pcs)", default=True)
    show_qty_per_hour = fields.Boolean("Qty per Hour (pcs/h)", default=True)

    limit_by = fields.Selection(selection='_get_limit_by_options', default='qty', required=True)

    @api.model
    def _get_limit_by_options(self):
        return [('qty', 'Quantity'), ('qty_per_hour', 'Qty per Hour')]

    def _resolve_path(self, path_str):
        if not path_str: return []
        ids = [int(x) for x in str(path_str).strip('/').split('/') if x]
        recs = self.env['mes.counts'].browse(ids)
        id_map = {rec.id: rec.name for rec in recs}
        return [id_map.get(i, str(i)) for i in ids]

    def action_generate_report(self):
        self.env['mes.reject.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()
        machines = self._get_filtered_machines()
        if not machines: return

        aggregated = {}
        period_runtimes = {}
        now_utc = fields.Datetime.now()
        
        valid_tags = []
        cnt_ids_list = []
        sig_map = {} 
        for m in machines:
            signals = m.count_tag_ids
            if self.cnt_ids:
                signals = signals.filtered(lambda s: s.count_id in self.cnt_ids) if self.cnt_filter_type == 'in' else signals.filtered(lambda s: s.count_id not in self.cnt_ids)
            for s in signals:
                if s.count_id:
                    valid_tags.append(s.tag_name)
                    cnt_ids_list.append(s.count_id.id)
                    sig_map[(m.name, s.tag_name)] = {'cnt': s.count_id, 'is_cum': s.is_cumulative}
        
        valid_tags = list(set(valid_tags))
        cnt_ids_tuple = tuple(set(cnt_ids_list))
        if not cnt_ids_tuple: return

        doc_ids_to_fetch = []
        tail_args = [] 

        for machine in machines:
            wc = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not wc: continue

            mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            shifts = self.env['mes.shift'].search([('company_id', '=', wc.company_id.id)], order='start_hour asc')
            periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, wc.company_id.tz or 'UTC')

            docs = self.env['mes.machine.performance'].search([
                ('machine_id', '=', wc.id), ('state', '=', 'done'),
                ('date', '>=', (self.start_datetime - timedelta(days=2)).date()),
                ('date', '<=', (self.end_datetime + timedelta(days=2)).date())
            ])
            
            doc_bounds = []
            for doc in docs:
                s_loc, e_loc = doc._get_local_shift_times()
                d_s = mac_tz.localize(s_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
                d_e = mac_tz.localize(e_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
                doc_bounds.append((d_s, d_e, doc))

            for p_name, time_blocks in periods_dict.items():
                if not time_blocks: continue
                p_start = min(t[0] for t in time_blocks)
                p_end = min(max(t[1] for t in time_blocks), now_utc)
                if p_start >= p_end: continue

                p_docs = [d for s, e, d in doc_bounds if s >= p_start and e <= p_end]
                
                if p_docs:
                    for d in p_docs: doc_ids_to_fetch.append((d.id, machine.id, p_name))
                    
                    first_doc_start = min(s for s, e, d in doc_bounds if d in p_docs)
                    last_doc_end = max(e for s, e, d in doc_bounds if d in p_docs)
                    
                    if p_start < first_doc_start:
                        s_loc = pytz.utc.localize(p_start).astimezone(mac_tz).replace(tzinfo=None)
                        e_loc = pytz.utc.localize(first_doc_start).astimezone(mac_tz).replace(tzinfo=None)
                        tail_args.append((p_name, machine.name, machine.id, s_loc, e_loc, wc.id, p_start, first_doc_start))
                        
                    if last_doc_end < p_end:
                        s_loc = pytz.utc.localize(last_doc_end).astimezone(mac_tz).replace(tzinfo=None)
                        e_loc = pytz.utc.localize(p_end).astimezone(mac_tz).replace(tzinfo=None)
                        tail_args.append((p_name, machine.name, machine.id, s_loc, e_loc, wc.id, last_doc_end, p_end))
                else:
                    s_loc = pytz.utc.localize(p_start).astimezone(mac_tz).replace(tzinfo=None)
                    e_loc = pytz.utc.localize(p_end).astimezone(mac_tz).replace(tzinfo=None)
                    tail_args.append((p_name, machine.name, machine.id, s_loc, e_loc, wc.id, p_start, p_end))

        if doc_ids_to_fetch:
            d_ids = tuple(set(x[0] for x in doc_ids_to_fetch))
            doc_map = {d: (m, p) for d, m, p in doc_ids_to_fetch}
            
            self.env.cr.execute("""
                SELECT performance_id, SUM(EXTRACT(EPOCH FROM (COALESCE(end_time, %s) - start_time))) 
                FROM mes_performance_running WHERE performance_id IN %s GROUP BY performance_id
            """, (now_utc, d_ids))
            for d_id, r_sec in self.env.cr.fetchall():
                m_id, p_name = doc_map[d_id]
                period_runtimes[(m_id, p_name)] = period_runtimes.get((m_id, p_name), 0.0) + ((r_sec or 0) / 3600.0)
                
            self.env.cr.execute("""
                SELECT performance_id, reason_id, SUM(qty) FROM mes_performance_rejection
                WHERE performance_id IN %s AND reason_id IN %s GROUP BY performance_id, reason_id
                UNION ALL
                SELECT performance_id, reason_id, SUM(qty) FROM mes_performance_production
                WHERE performance_id IN %s AND reason_id IN %s GROUP BY performance_id, reason_id
            """, (d_ids, cnt_ids_tuple, d_ids, cnt_ids_tuple))
            for d_id, cid, qty in self.env.cr.fetchall():
                m_id, p_name = doc_map[d_id]
                key = (m_id, p_name, cid)
                aggregated[key] = aggregated.get(key, 0.0) + qty

        if tail_args and valid_tags:
            g_p, g_m, g_s, g_e = [], [], [], []
            for p_name, m_name, m_id, s_loc, e_loc, wc_id, s_utc, e_utc in tail_args:
                wc = self.env['mrp.workcenter'].browse(wc_id)
                machine = self.env['mes.machine.settings'].browse(m_id)
                act_int, _ = machine._get_planned_working_intervals(s_utc, e_utc, wc)
                if act_int:
                    act_int = self._merge_intervals(act_int)
                    r_sec = machine._fetch_interval_stats(act_int, wc.id, mode='runtime')
                    period_runtimes[(m_id, p_name)] = period_runtimes.get((m_id, p_name), 0.0) + (r_sec / 3600.0)

                g_p.append(p_name)
                g_m.append(m_name)
                g_s.append(s_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))
                g_e.append(e_loc.strftime('%Y-%m-%d %H:%M:%S.%f'))

            if g_s:
                with self.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            WITH gaps AS (
                                SELECT unnest(%s::varchar[]) AS p_name,
                                       unnest(%s::varchar[]) AS m_name,
                                       unnest(%s::timestamp[]) AS start_t,
                                       unnest(%s::timestamp[]) AS end_t
                            )
                            SELECT g.p_name, g.m_name, t.tag_name, 
                                   COALESCE(SUM(t.value), 0), COALESCE(MAX(t.value) - MIN(t.value), 0)
                            FROM telemetry_count t
                            JOIN gaps g ON t.machine_name = g.m_name AND t.time >= g.start_t AND t.time < g.end_t
                            WHERE t.tag_name = ANY(%s)
                            GROUP BY g.p_name, g.m_name, t.tag_name
                        """, (g_p, g_m, g_s, g_e, valid_tags))
                        
                        mac_name_to_id = {m.name: m.id for m in machines}
                        for p_name, m_name, t_name, sum_val, cum_val in cur.fetchall():
                            sig = sig_map.get((m_name, t_name))
                            m_id = mac_name_to_id.get(m_name)
                            if sig and m_id:
                                qty = cum_val if sig['is_cum'] else sum_val
                                if qty > 0:
                                    key = (m_id, p_name, sig['cnt'].id)
                                    aggregated[key] = aggregated.get(key, 0.0) + float(qty)

        lines = []
        counts_cache = {c.id: c for c in self.env['mes.counts'].browse(cnt_ids_tuple)}
        mac_cache = {m.id: m.name for m in machines}

        for (m_id, p_name, cid), qty in aggregated.items():
            cnt = counts_cache[cid]
            r_h = period_runtimes.get((m_id, p_name), 0.0)
            hierarchy = self._resolve_path(cnt.parent_path or str(cid))
            if not hierarchy: hierarchy = [cnt.name]

            def build_label(by_mac, by_per, by_cnt, by_is_mod, by_wheel, by_mod):
                parts = []
                if by_mac: parts.append(mac_cache[m_id])
                if by_cnt != 'none':
                    if by_cnt == 'hierarchy': parts.append(" / ".join(hierarchy[:-1]) if len(hierarchy) > 1 else hierarchy[0])
                    elif by_cnt == 'flat': parts.append(hierarchy[-1])
                    elif by_cnt == 'full': parts.append(" / ".join(hierarchy))
                if by_per: parts.append(p_name)
                if by_is_mod: parts.append("Mod" if cnt.is_module_count else "Non-Mod")
                if by_wheel: parts.append(f"W: {cnt.wheel or '0'}")
                if by_mod: parts.append(f"M: {cnt.module or '0'}")
                return " | ".join(parts) if parts else "All Data"

            lines.append({
                'user_id': self.env.user.id,
                'machine_id': m_id,
                'period_name': p_name,
                'count_name': cnt.name,
                'row_group_label': build_label(self.row_by_machine, self.row_by_period, self.row_by_cnt, self.row_by_is_mod, self.row_by_wheel, self.row_by_mod),
                'col_group_label': build_label(self.col_by_machine, self.col_by_period, self.col_by_cnt, self.col_by_is_mod, self.col_by_wheel, self.col_by_mod),
                'qty': qty,
                'qty_per_hour': round(qty / r_h, 2) if r_h > 0 else 0.0
            })

        if lines:
            lines.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0: lines = lines[:self.record_limit]
            self.env['mes.reject.report.line'].create(lines)

        measures = []
        if self.show_qty: measures.append('qty')
        if self.show_qty_per_hour: measures.append('qty_per_hour')

        return {
            'name': 'Reject Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.reject.report.line',
            'view_mode': 'pivot,graph,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': self._build_skd_context(measures or ['qty'])
        }

class MesRejectReportLine(models.TransientModel):
    _name = 'mes.reject.report.line'
    _description = 'Reject Report Matrix Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")
    count_name = fields.Char(string="Reject Count")
    
    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    qty = fields.Float(string="Quantity", group_operator="sum")
    qty_per_hour = fields.Float(string="Qty per Hour", group_operator="avg")

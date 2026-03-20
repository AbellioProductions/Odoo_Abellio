from odoo import models, fields, api

class MesRejectReportWizard(models.TransientModel):
    _name = 'mes.reject.report.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Reject Report Matrix Wizard'

    cnt_filter_type = fields.Selection([
        ('in', 'In List'),
        ('not_in', 'Not in List')
    ], string="Count Condition", default='in', required=True)
    cnt_ids = fields.Many2many('mes.counts', string="Reject Counts")

    row_by_cnt = fields.Selection([
        ('none', 'None'),
        ('flat', 'Element Only'),
        ('hierarchy', 'Hierarchy Only'),
        ('full', 'Hierarchy + Element')
    ], string="Count", default='flat', required=True)
    row_by_is_mod = fields.Boolean("Is Module", default=False)
    row_by_wheel = fields.Boolean("Wheel Number", default=False)
    row_by_mod = fields.Boolean("Module Number", default=False)

    col_by_cnt = fields.Selection([
        ('none', 'None'),
        ('flat', 'Element Only'),
        ('hierarchy', 'Hierarchy Only'),
        ('full', 'Hierarchy + Element')
    ], string="Count", default='none', required=True)
    col_by_is_mod = fields.Boolean("Is Module", default=False)
    col_by_wheel = fields.Boolean("Wheel Number", default=False)
    col_by_mod = fields.Boolean("Module Number", default=False)

    show_qty = fields.Boolean("Total Quantity (pcs)", default=True)

    limit_by = fields.Selection(
        selection='_get_limit_by_options',
        default='qty',
        required=True
    )

    @api.model
    def _get_limit_by_options(self):
        return [
            ('qty', 'Quantity')
        ]

    def _resolve_path(self, path_str):
        if not path_str: 
            return []
        ids = [int(x) for x in str(path_str).strip('/').split('/') if x]
        recs = self.env['mes.counts'].browse(ids)
        id_map = {rec.id: rec.name for rec in recs}
        return [id_map.get(i, str(i)) for i in ids]

    def action_generate_report(self):
        self.env['mes.reject.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        shifts = self.env['mes.shift'].search([], order='start_hour asc')
        periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts)

        ts_mgr = self.env['mes.timescale.base']
        aggregated = {}

        if not machines:
            return

        with ts_mgr._connection() as conn:
            with conn.cursor() as cur:
                for machine in machines:
                    workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
                    
                    # 1. Получаем настройки сигналов прямо из Odoo ORM
                    signals = machine.count_tag_ids
                    if self.cnt_ids:
                        if self.cnt_filter_type == 'in':
                            signals = signals.filtered(lambda s: s.count_id in self.cnt_ids)
                        else:
                            signals = signals.filtered(lambda s: s.count_id not in self.cnt_ids)
                    
                    if not signals:
                        continue
                        
                    valid_tags = list(signals.mapped('tag_name'))

                    for p_name, time_blocks in periods_dict.items():
                        all_active_intervals = []
                        for t_s, t_e in time_blocks:
                            act_int, _ = machine._get_planned_working_intervals(t_s, t_e, workcenter)
                            all_active_intervals.extend(act_int)
                            
                        all_active_intervals = self._merge_intervals(all_active_intervals)
                        if not all_active_intervals: 
                            continue

                        for a_s, a_e in all_active_intervals:
                            # 2. Запрашиваем только сырую телеметрию из TimescaleDB (БЕЗ JOIN)
                            cur.execute("""
                                SELECT tag_name, 
                                       COALESCE(SUM(value), 0) as sum_val, 
                                       COALESCE(MAX(value) - MIN(value), 0) as cum_val
                                FROM telemetry_count 
                                WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
                                GROUP BY tag_name
                            """, (machine.name, valid_tags, a_s, a_e))
                            
                            for row in cur.fetchall():
                                t_name, sum_val, cum_val = row
                                
                                # 3. Склеиваем данные в Python с помощью Odoo ORM
                                sig = signals.filtered(lambda s: s.tag_name == t_name)
                                if not sig:
                                    continue
                                sig = sig[0]
                                
                                qty = cum_val if sig.is_cumulative else sum_val
                                if qty <= 0: 
                                    continue
                                
                                cnt = sig.count_id
                                key = (machine.id, machine.name, p_name, cnt.id, cnt.name, cnt.parent_path, cnt.is_module_count, cnt.wheel, cnt.module)
                                
                                if key not in aggregated:
                                    aggregated[key] = 0.0
                                aggregated[key] += float(qty)

        lines = []
        for key, qty in aggregated.items():
            m_id, m_name, p_name, cnt_id, cnt_name, parent_path, is_mod, wheel, mod = key
            
            hierarchy = self._resolve_path(parent_path or str(cnt_id))
            if not hierarchy: 
                hierarchy = [cnt_name]

            def build_label(by_mac, by_per, by_cnt, by_is_mod, by_wheel, by_mod):
                parts = []
                if by_mac: parts.append(m_name)
                if by_cnt != 'none':
                    if by_cnt == 'hierarchy':
                        parts.append(" / ".join(hierarchy[:-1]) if len(hierarchy) > 1 else hierarchy[0])
                    elif by_cnt == 'flat':
                        parts.append(hierarchy[-1])
                    elif by_cnt == 'full':
                        parts.append(" / ".join(hierarchy))
                if by_per: parts.append(p_name)
                if by_is_mod: parts.append("Mod" if is_mod else "Non-Mod")
                if by_wheel: parts.append(f"W: {wheel or '0'}")
                if by_mod: parts.append(f"M: {mod or '0'}")
                return " | ".join(parts) if parts else "All Data"

            r_label = build_label(self.row_by_machine, self.row_by_period, self.row_by_cnt, self.row_by_is_mod, self.row_by_wheel, self.row_by_mod)
            c_label = build_label(self.col_by_machine, self.col_by_period, self.col_by_cnt, self.col_by_is_mod, self.col_by_wheel, self.col_by_mod)

            lines.append({
                'user_id': self.env.user.id,
                'machine_id': m_id,
                'period_name': p_name,
                'count_name': cnt_name,
                'row_group_label': r_label,
                'col_group_label': c_label,
                'qty': qty
            })

        if lines:
            lines.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0:
                lines = lines[:self.record_limit]
            self.env['mes.reject.report.line'].create(lines)

        measures = ['qty']
        ctx = self._build_skd_context(measures)

        return {
            'name': 'Reject Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.reject.report.line',
            'view_mode': 'pivot,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': ctx
        }

class MesRejectReportLine(models.Model):
    _name = 'mes.reject.report.line'
    _description = 'Reject Report Matrix Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")
    count_name = fields.Char(string="Reject Count")
    
    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    qty = fields.Float(string="Quantity", group_operator="sum")
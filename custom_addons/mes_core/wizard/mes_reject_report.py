from odoo import models, fields, api
from psycopg2.extras import DictCursor

class MesRejectReportWiz(models.TransientModel):
    _name = 'mes.reject.report.wiz'
    
    _inherit = 'mes.report.base.wizard'
    _description = 'Reject Report Wizard'

    mac_ids = fields.Many2many('mes.machine.settings', string='Machines')
    mac_flt = fields.Selection([('in', 'Include'), ('not_in', 'Exclude')], default='in', required=True)

    cnt_ids = fields.Many2many('mes.counts', string='Reject Counts')
    cnt_flt = fields.Selection([('in', 'Include'), ('not_in', 'Exclude')], default='in', required=True)

    grp_mac = fields.Boolean('Machine')
    grp_dt = fields.Boolean('Date')
    grp_cnt = fields.Selection([
        ('none', 'None'),
        ('flat', 'Element Only'),
        ('hierarchy', 'Hierarchy Only'),
        ('full', 'Hierarchy + Element')
    ], default='flat', required=True)
    
    grp_is_mod = fields.Boolean('Is Module')
    grp_wheel = fields.Boolean('Wheel Number')
    grp_mod = fields.Boolean('Module Number')

    def action_gen_report(self):
        data = self.read()[0]
        return self.env.ref('mes_core.action_report_reject').report_action(self, data=data)


class ReportReject(models.AbstractModel):
    _name = 'report.mes_core.report_reject_tpl'
    _description = 'Reject Report Template Model'

    @api.model
    def _get_report_values(self, docids, data=None):
        req = data or {}
        raw = self._fetch_raw(req)
        agg = self._build_agg(raw, req)

        return {
            'doc_ids': docids,
            'doc_model': 'mes.reject.report.wiz',
            'data': req,
            'report_data': agg
        }

    def _fetch_raw(self, req):
        ts_mgr = self.env['mes.timescale.base']
        
        sql = """
            WITH target_mac AS (
                SELECT id, name FROM mes_machine_settings WHERE 1=1
            ),
            target_cnt AS (
                SELECT id, name, parent_path, is_module_count, wheel, module 
                FROM mes_counts WHERE 1=1
            ),
            sig_map AS (
                SELECT 
                    msc.tag_name, ms.name as mac_name, 
                    tc.id as cnt_id, tc.name as cnt_name, tc.parent_path,
                    tc.is_module_count, tc.wheel, tc.module, msc.is_cumulative
                FROM mes_signal_count msc
                JOIN target_mac ms ON ms.id = msc.machine_id
                JOIN target_cnt tc ON tc.id = msc.count_id
            ),
            raw_data AS (
                SELECT 
                    t.time::date as dt,
                    t.machine_name as mac,
                    sm.cnt_id, sm.cnt_name, sm.parent_path,
                    sm.is_module_count, sm.wheel, sm.module,
                    COALESCE(
                        CASE WHEN sm.is_cumulative THEN MAX(t.value) - MIN(t.value)
                        ELSE SUM(t.value) END, 
                    0) as qty
                FROM telemetry_count t
                JOIN sig_map sm ON sm.tag_name = t.tag_name AND sm.mac_name = t.machine_name
                WHERE t.time >= %s AND t.time <= %s
                GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, sm.is_cumulative
            )
            SELECT * FROM raw_data WHERE qty > 0
        """

        args = [req.get('start_datetime'), req.get('end_datetime')]
        
        mac_ids = tuple(req.get('mac_ids', []))
        if mac_ids:
            op = 'IN' if req.get('mac_flt') == 'in' else 'NOT IN'
            sql = sql.replace('WHERE 1=1', f'WHERE id {op} %s', 1)
            args.insert(0, mac_ids)

        cnt_ids = tuple(req.get('cnt_ids', []))
        if cnt_ids:
            op = 'IN' if req.get('cnt_flt') == 'in' else 'NOT IN'
            sql = sql.replace('WHERE 1=1', f'WHERE id {op} %s', 1)
            args.insert(1 if mac_ids else 0, cnt_ids)

        with ts_mgr._connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(sql, tuple(args))
                return cur.fetchall()

    def _build_agg(self, raw, req):
        res = {}
        
        for r in raw:
            keys = []
            
            if req.get('grp_dt'):
                keys.append(str(r['dt']))
            if req.get('grp_mac'):
                keys.append(r['mac'])
                
            cnt_lvl = req.get('grp_cnt')
            if cnt_lvl != 'none':
                path_str = r['parent_path'] or str(r['cnt_id'])
                hierarchy = self._resolve_path(path_str)
                
                if cnt_lvl == 'hierarchy':
                    keys.append(" / ".join(hierarchy[:-1]) if len(hierarchy) > 1 else hierarchy[0])
                elif cnt_lvl == 'flat':
                    keys.append(hierarchy[-1])
                elif cnt_lvl == 'full':
                    keys.append(" / ".join(hierarchy))

            if req.get('grp_is_mod'):
                keys.append("Mod" if r['is_module_count'] else "Non-Mod")
            if req.get('grp_wheel'):
                keys.append(f"W: {r['wheel'] or '0'}")
            if req.get('grp_mod'):
                keys.append(f"M: {r['module'] or '0'}")

            grp_key = " | ".join(keys) if keys else "Total"
            
            if grp_key not in res:
                res[grp_key] = 0.0
            res[grp_key] += float(r['qty'])

        return [{'grp': k, 'qty': v} for k, v in sorted(res.items())]

    def _resolve_path(self, path_str):
        ids = [int(x) for x in path_str.strip('/').split('/') if x]
        recs = self.env['mes.counts'].browse(ids)
        id_map = {rec.id: rec.name for rec in recs}
        return [id_map.get(i, str(i)) for i in ids]
import math
import pytz
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
        self._sync_fdw(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync_fdw(rec)
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
        return super().unlink()

    def _sync_fdw(self, rec):
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))

    def get_alarm_tag_name(self, default_type='OEE.nStopRootReason'):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('event_id.default_event_tag_type', '=', default_type)
        ], limit=1)
        return override.tag_name if override and override.tag_name else f"%{default_type}%"

    def resolve_plc_value_to_name(self, plc_value):
        self.ensure_one()
        plc_str = str(plc_value)
        
        if not plc_str.isdigit():
            return plc_str
            
        plc_int = int(plc_str)
        override = self.env['mes.signal.event'].search([('machine_id', '=', self.id), ('plc_value', '=', plc_int)], limit=1)
        if override and override.event_id:
            return override.event_id.name
            
        dict_event = self.env['mes.event'].search([('default_plc_value', '=', plc_int)], limit=1)
        return dict_event.name if dict_event else plc_str


    def _build_intersection_sql(self, active_intervals, tbl_name):
        """Generates the unified CTE for intersecting real logs with planned working windows."""
        val_list = [f"('{s.isoformat()}'::timestamp, '{e.isoformat()}'::timestamp)" for s, e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        return f"""
            WITH active_windows AS ( {active_cte} ),
            target_events AS (
                SELECT e.id, e.loss_id, e.start_time, COALESCE(e.end_time, %s) as end_time
                FROM {tbl_name} e
                JOIN mes_machine_performance p ON p.id = e.performance_id
                WHERE p.machine_id = %s 
                  AND p.date >= %s
                  AND e.start_time < %s 
                  AND (e.end_time > %s OR e.end_time IS NULL)
            ),
            intersected AS (
                SELECT e.id, e.loss_id,
                       GREATEST(e.start_time, aw.ai_start) as eff_start,
                       LEAST(e.end_time, aw.ai_end) as eff_end
                FROM target_events e
                INNER JOIN active_windows aw ON aw.ai_start < e.end_time AND aw.ai_end > e.start_time
            )
        """

    def get_top_alarm_str(self, active_intervals, wc_id):
        if not active_intervals:
            return "None"

        now_utc = fields.Datetime.now()
        start_time, end_time = active_intervals[0][0], active_intervals[-1][1]
        min_doc_date = (start_time - timedelta(days=1)).date()

        query = self._build_intersection_sql(active_intervals, 'mes_performance_alarm') + """
            SELECT loss_id, SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))) as total_dur 
            FROM intersected
            WHERE eff_start < eff_end
            GROUP BY loss_id 
            ORDER BY total_dur DESC LIMIT 1;
        """
        self.env.cr.execute(query, (now_utc, wc_id, min_doc_date, end_time, start_time))
        res = self.env.cr.fetchone()
        
        if res and res[0]:
            loss = self.env['mes.event'].browse(res[0])
            return f"{loss.name} ({int((res[1] or 0) // 60)} min)"
        return "None"

    def _fetch_interval_stats(self, active_intervals, wc_id, mode='runtime'):
        if not active_intervals:
            return [] if mode == 'downtime' else False if mode == 'first_start' else 0.0

        now_utc = fields.Datetime.now()
        start_time, end_time = active_intervals[0][0], active_intervals[-1][1]
        min_doc_date = (start_time - timedelta(days=1)).date()

        table_map = {
            'runtime': 'mes_performance_running',
            'downtime': 'mes_performance_alarm',
            'slowing': 'mes_performance_slowing',
            'first_start': 'mes_performance_running'
        }
        
        base_query = self._build_intersection_sql(active_intervals, table_map.get(mode))

        if mode == 'runtime':
            query = base_query + "SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))), 0) FROM intersected WHERE eff_start < eff_end"
            self.env.cr.execute(query, (now_utc, wc_id, min_doc_date, end_time, start_time))
            return float(self.env.cr.fetchone()[0] or 0.0)

        elif mode == 'downtime':
            query = base_query + """
                SELECT loss_id, COUNT(DISTINCT id) as freq, COALESCE(SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))), 0) as total_dur
                FROM intersected WHERE eff_start < eff_end GROUP BY loss_id
            """
            self.env.cr.execute(query, (now_utc, wc_id, min_doc_date, end_time, start_time))
            return self.env.cr.fetchall()

        elif mode == 'first_start':
            query = base_query + "SELECT MIN(eff_start) FROM intersected WHERE eff_start < eff_end"
            self.env.cr.execute(query, (now_utc, wc_id, min_doc_date, end_time, start_time))
            res = self.env.cr.fetchone()
            return res[0] if res else False

    def _get_planned_working_intervals(self, start_time, end_time, workcenter):
        if not workcenter:
            return [(start_time, end_time)], (end_time - start_time).total_seconds()

        now_utc = fields.Datetime.now()
        mac_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        s_utc = mac_tz.localize(start_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        calc_end_time = min(now_mac, end_time)
        e_utc = mac_tz.localize(calc_end_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

        if start_time >= calc_end_time:
            return [], 0.0

        downtimes = self.env['mes.flat.downtime'].search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', e_utc),
            ('end_time', '>', s_utc)
        ])

        intervals = []
        for dt in downtimes:
            dt_s_loc = pytz.utc.localize(dt.start_time).astimezone(mac_tz).replace(tzinfo=None)
            dt_e_loc = pytz.utc.localize(dt.end_time).astimezone(mac_tz).replace(tzinfo=None)
            
            dt_s = max(dt_s_loc, start_time)
            dt_e = min(dt_e_loc, calc_end_time)
            if dt_s < dt_e:
                intervals.append([dt_s, dt_e])

        if intervals:
            intervals.sort(key=lambda x: x[0])
            merged = [intervals[0]]
            for current in intervals[1:]:
                last = merged[-1]
                if current[0] <= last[1]:
                    last[1] = max(last[1], current[1])
                else:
                    merged.append(current)
            intervals = merged

        active_intervals = []
        current_time = start_time
        for dt in intervals:
            if current_time < dt[0]:
                active_intervals.append((current_time, dt[0]))
            current_time = max(current_time, dt[1])

        if current_time < calc_end_time:
            active_intervals.append((current_time, calc_end_time))

        total_sec = sum((i[1] - i[0]).total_seconds() for i in active_intervals)
        return active_intervals, total_sec
    


    def _fetch_waste_stats_raw(self, cursor, start_time, end_time):
        cursor.execute("""
            SELECT tag_name, COALESCE(SUM(value), 0), COALESCE(MAX(value) - MIN(value), 0)
            FROM telemetry_count 
            WHERE machine_name = %s AND time >= %s AND time < %s
            GROUP BY tag_name
        """, (self.name, start_time, end_time))
        return {row[0]: {'sum': float(row[1]), 'cum': float(row[2])} for row in cursor.fetchall()}

    def _fetch_timeline_raw(self, start_time, end_time, wc_id):
        wc = self.env['mrp.workcenter'].browse(wc_id)
        mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
        
        s_utc = mac_tz.localize(start_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        e_utc = mac_tz.localize(end_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        
        def _fmt(dt_utc):
            if not dt_utc: return ''
            if isinstance(dt_utc, str): dt_utc = fields.Datetime.from_string(dt_utc)
            return pytz.utc.localize(dt_utc).astimezone(mac_tz).replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')

        perfs = self.env['mes.machine.performance'].search([
            ('machine_id', '=', wc_id), 
            ('date', '>=', start_time.date() - timedelta(days=1)), 
            ('date', '<=', end_time.date() + timedelta(days=1))
        ])
        
        if not perfs: return []

        now_utc = fields.Datetime.now()
        res = []

        mapping = [
            ('mes.performance.running', 'Running', '#28a745', 'running'),
            ('mes.performance.alarm', 'Alarm', '#dc3545', 'alarm'),
            ('mes.performance.slowing', 'Slowing', '#6c757d', 'slowing')
        ]

        for model, default_name, default_color, status in mapping:
            logs = self.env[model].search([
                ('performance_id', 'in', perfs.ids), ('start_time', '<', e_utc), 
                '|', ('end_time', '>', s_utc), ('end_time', '=', False)
            ])
            for log in logs:
                end_t = log.end_time or now_utc
                res.append({
                    'start': _fmt(log.start_time), 'end': _fmt(end_t),
                    'name': log.loss_id.name if log.loss_id else default_name,
                    'status': status, 'color': log.loss_id.color or default_color,
                    'duration': (end_t - log.start_time).total_seconds()
                })
        return res

    def _fetch_production_chart_raw(self, cursor, tag_names, start_time, end_time, bucket_min):
        if not tag_names: return []
        cursor.execute(f"""
            SELECT tag_name, time_bucket('{bucket_min} minutes', time) AS bucket,
                   COALESCE(SUM(value), 0), COALESCE(MAX(value) - MIN(value), 0)
            FROM telemetry_count
            WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time < %s
            GROUP BY tag_name, bucket ORDER BY bucket
        """, (self.name, tag_names, start_time, end_time))
        return cursor.fetchall()

    def _calculate_kpi(self, total_running_sec, total_produced, total_planned_sec, wc):
        run_sec = max(0.0, total_running_sec)
        h, m, s = int(run_sec // 3600), int((run_sec % 3600) // 60), int(run_sec % 60)
        
        ideal_rate = (wc.ideal_capacity_per_min / 60.0) if (wc and wc.ideal_capacity_per_min > 0) else 1.0 
            
        avail = max(0.0, min(run_sec / total_planned_sec if total_planned_sec > 0 else 0, 1.0))
        perf = max(0.0, min(total_produced / (run_sec * ideal_rate) if run_sec > 0 else 0, 1.0))
        oee = avail * perf

        waste = max(0.0, 1 - (total_produced / (run_sec * ideal_rate))) if (total_planned_sec > 0 and run_sec > 0) else 0.0

        return {
            'availability': round(avail * 100, 2),
            'performance': round(perf * 100, 2),
            'quality': 100.0,
            'oee': round(oee * 100, 2),
            'waste_losses': round(waste * 100, 2),
            'downtime_losses': round(max(0.0, 1.0 - avail) * 100, 2) if total_planned_sec > 0 else 0.0,
            'total_produced': total_produced,
            'runtime_formatted': f"{h:02d}:{m:02d}:{s:02d}",
        }


    @api.model
    def get_realtime_oee_batch(self, wcs):
        if not wcs: return {}
        res, cfgs = {}, {}

        now_utc = fields.Datetime.now()
        for wc in wcs:
            mac = wc.machine_settings_id
            if not mac: continue

            s_time, e_time = self.env['mes.shift'].get_current_shift_window(wc)
            if not s_time or not e_time:
                res[wc.id] = {'error': 'No active shift'}
                continue
                
            mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            calc_e_time = min(pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None), e_time)

            state_tag, _ = wc.runtime_event_id.get_mapping_for_machine(mac) if wc.runtime_event_id else (None, None)
            count_tag, is_cumul = wc.production_count_id.get_count_config_for_machine(mac) if wc.production_count_id else (None, False)

            if not state_tag or not count_tag:
                res[wc.id] = {'error': 'Config Error'}
                continue

            act_ints, plan_sec = mac._get_planned_working_intervals(s_time, e_time, wc)

            cfgs[wc.id] = {
                'mac': mac, 'count_tag': count_tag, 'is_cumul': is_cumul,
                'act_ints': act_ints, 'plan_sec': plan_sec, 'count_id': wc.production_count_id.id,
                's_time': s_time, 'calc_e_time': calc_e_time
            }

        if not cfgs: return res

        c_data = {}
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                for wc_id, cfg in cfgs.items():
                    m_name = cfg['mac'].name
                    if m_name not in c_data:
                        c_data[m_name] = cfg['mac']._fetch_waste_stats_raw(cur, cfg['s_time'], cfg['calc_e_time'])

        all_rej = self.env['mes.counts'].search([])

        for wc_id, cfg in cfgs.items():
            mac = cfg['mac']
            tot_prod = c_data.get(mac.name, {}).get(cfg['count_tag'], {}).get('cum' if cfg['is_cumul'] else 'sum', 0)

            top_rej_cnt, top_rej_name = 0, "None"
            for r_cnt in all_rej:
                if r_cnt.id == cfg['count_id']: continue
                r_tag, r_is_cum = r_cnt.get_count_config_for_machine(mac)
                if not r_tag: continue
                    
                r_amt = c_data.get(mac.name, {}).get(r_tag, {}).get('cum' if r_is_cum else 'sum', 0)
                if r_amt > top_rej_cnt:
                    top_rej_cnt, top_rej_name = r_amt, r_cnt.name

            kpi = mac._calculate_kpi(
                mac._fetch_interval_stats(cfg['act_ints'], wc_id, mode='runtime'), 
                tot_prod, cfg['plan_sec'], wcs.browse(wc_id)
            )
            kpi.update({
                'first_running_time': mac._fetch_interval_stats(cfg['act_ints'], wc_id, mode='first_start'),
                'top_alarm': mac.get_top_alarm_str(cfg['act_ints'], wc_id),
                'top_rejection': f"{top_rej_name} ({int(top_rej_cnt)})" if top_rej_cnt > 0 else "None"
            })
            res[wc_id] = kpi

        return res

    def _calculate_kpi_for_window(self, workcenter, start_time, end_time):
        if not workcenter:
            return None

        active_intervals, plan_sec = self._get_planned_working_intervals(start_time, end_time, workcenter)
        if plan_sec <= 0:
            return None

        run_sec = self._fetch_interval_stats(active_intervals, workcenter.id, mode='runtime')
        tot_prod = 0.0
        
        docs = self.env['mes.machine.performance'].search([
            ('machine_id', '=', workcenter.id),
            ('state', '=', 'done')
        ])
        
        valid_docs = docs.filtered(
            lambda d: d._get_utc_time(d._get_local_shift_times()[0]) <= start_time and 
                      d._get_utc_time(d._get_local_shift_times()[1]) >= end_time
        )

        if valid_docs:
            prods = valid_docs.production_ids.filtered(lambda p: p.reason_id == workcenter.production_count_id)
            tot_prod = sum(prods.mapped('qty'))
        else:
            count_tag, is_cumul = workcenter.production_count_id.get_count_config_for_machine(self) if workcenter.production_count_id else (None, False)
            if count_tag:
                with self.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        if is_cumul:
                            cur.execute("""
                                SELECT COALESCE(MAX(value) - MIN(value), 0) 
                                FROM telemetry_count 
                                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time < %s
                            """, (self.name, count_tag, start_time, end_time))
                        else:
                            cur.execute("""
                                SELECT COALESCE(SUM(value), 0) 
                                FROM telemetry_count 
                                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time < %s
                            """, (self.name, count_tag, start_time, end_time))
                        
                        res = cur.fetchone()
                        if res and res[0]:
                            tot_prod = float(res[0])
                
        kpi = self._calculate_kpi(run_sec, tot_prod, plan_sec, workcenter)
        kpi['produced'] = tot_prod
        kpi['first_running_time'] = self._fetch_interval_stats(active_intervals, workcenter.id, mode='first_start')
        
        return kpi

    def action_import_machine_counts(self):
        self.ensure_one()
        return {
            'name': 'Import Machine Counts',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.dictionary.import.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_import_mode': 'machine',
                'default_import_type': 'count',
                'default_machine_id': self.id,
            }
        }

    def action_import_machine_events(self):
        self.ensure_one()
        return {
            'name': 'Import Machine Events/Alarms',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.dictionary.import.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_import_mode': 'machine',
                'default_import_type': 'event',
                'default_machine_id': self.id,
            }
        }


class MesSignalBase(models.AbstractModel):
    _name = 'mes.signal.base'
    _description = 'Base Signal Config'
    _inherit = ['mes.timescale.base']

    tag_name = fields.Char(string='Signal Tag', required=True)
    poll_type = fields.Selection([('cyclic', 'Cyclic'), ('on_change', 'On Change')], default='cyclic', required=True)
    poll_frequency = fields.Integer(string='Freq (ms)', default=10000)
    param_type = fields.Selection([
        ('auto', 'Auto'), ('bool', 'Boolean'), ('int', 'Integer'), ('double', 'Double'), ('string', 'String')
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
            rec.machine_id.name, rec.tag_name, rec.poll_type, rec.poll_frequency, rec.param_type, self._signal_type
        ))

class MesSignalCount(models.Model):
    _name = 'mes.signal.count'
    _inherit = 'mes.signal.base'
    _description = 'Count Signals'
    _signal_type = 'count'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    count_id = fields.Many2one('mes.counts', string='Dictionary Count', required=True)
    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)

    _sql_constraints = [('tag_count_uniq', 'unique(machine_id, tag_name, count_id)', 'Mapping exists!')]

    def unlink(self):
        for rec in self: self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

    @api.onchange('count_id')
    def _onchange_count_id(self):
        if self.count_id: self.is_cumulative = self.count_id.is_cumulative

class MesSignalEvent(models.Model):
    _name = 'mes.signal.event'
    _inherit = 'mes.signal.base'
    _description = 'Event Signals'
    _signal_type = 'event'

    poll_type = fields.Selection(selection_add=[], default='on_change')
    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    event_id = fields.Many2one('mes.event', string='Dictionary Event', required=True)
    plc_value = fields.Integer(string='PLC Value', required=True)

    _sql_constraints = [('tag_val_event_uniq', 'unique(machine_id, tag_name, plc_value, event_id)', 'Mapping exists!')]

    def unlink(self):
        for rec in self:
            if self.search_count([('machine_id', '=', rec.machine_id.id), ('tag_name', '=', rec.tag_name), ('id', '!=', rec.id)]) == 0:
                self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

class MesSignalProcess(models.Model):
    _name = 'mes.signal.process'
    _inherit = 'mes.signal.base'
    _description = 'Process Signals'
    _signal_type = 'process'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    process_id = fields.Many2one('mes.process', string='Dictionary Process', required=True)

    _sql_constraints = [('tag_process_uniq', 'unique(machine_id, tag_name, process_id)', 'Mapping exists!')]

    def unlink(self):
        for rec in self: self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

class MesWasteLossStat(models.TransientModel):
    _name = 'mes.waste.loss.stat'
    _description = 'Waste Losses Statistics'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine')
    name = fields.Char(string='Waste Type (Count)')
    waste_sum = fields.Float(string='Shift Total (pcs)')
    waste_per_hour = fields.Float(string='Waste per Hour (pcs/h)')

    @api.model
    def _generate_stats(self, machine_id):
        machine = self.env['mes.machine.settings'].browse(machine_id)
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        if not workcenter: return
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window(workcenter)
        if not start_time: return
        
        mac_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        calc_end_time = min(pytz.utc.localize(fields.Datetime.now()).astimezone(mac_tz).replace(tzinfo=None), shift_end)
        
        active_ints, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        total_run_sec = machine._fetch_interval_stats(active_ints, workcenter.id, mode='runtime')
        hours_run = (total_run_sec / 3600.0) if total_run_sec > 0 else 0.0

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                raw_counts = machine._fetch_waste_stats_raw(cur, start_time, calc_end_time)

        vals = []
        for c_def in machine.count_tag_ids:
            if c_def.count_id == workcenter.production_count_id: continue
            
            data = raw_counts.get(c_def.tag_name, {'sum': 0.0, 'cum': 0.0})
            val = data.get('cum') if c_def.is_cumulative else data.get('sum')
            
            if val > 0:
                vals.append({
                    'machine_id': machine.id,
                    'name': c_def.count_id.name if c_def.count_id else c_def.tag_name,
                    'waste_sum': val,
                    'waste_per_hour': (val / hours_run) if hours_run > 0 else 0.0
                })
        if vals: self.with_context(skip_generation=True).create(vals)

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
    def _generate_stats(self, machine_id):
        machine = self.env['mes.machine.settings'].browse(machine_id)
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        if not workcenter: return
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window(workcenter)
        if not start_time: return
        
        active_ints, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        total_run_sec = machine._fetch_interval_stats(active_ints, workcenter.id, mode='runtime')
        hours_run = (total_run_sec / 3600.0) if total_run_sec > 0 else 0.0
        
        stats = {}
        for loss_id, freq, dur in machine._fetch_interval_stats(active_ints, workcenter.id, mode='downtime'):
            evt_name = self.env['mes.event'].browse(loss_id).name
            if evt_name not in stats: stats[evt_name] = {'freq': 0, 'dur': 0.0}
            stats[evt_name]['freq'] += freq
            stats[evt_name]['dur'] += dur
        
        vals = []
        for name, data in stats.items():
            dur_min = data['dur'] / 60.0
            if dur_min > 0 or data['freq'] > 0:
                vals.append({
                    'machine_id': machine.id, 'name': name, 'frequency': data['freq'],
                    'freq_per_hour': (data['freq'] / hours_run) if hours_run > 0 else 0.0,
                    'total_time': dur_min, 'time_per_hour': (dur_min / hours_run) if hours_run > 0 else 0.0
                })
        if vals: self.with_context(skip_generation=True).create(vals)
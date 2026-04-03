import re
import pytz
import logging
from datetime import datetime, timedelta
from odoo import models, fields, api
from odoo.exceptions import ValidationError

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
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
        return super().unlink()

    def get_alarm_tag_name(self, default_type='OEE.nStopRootReason'):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('event_id.default_event_tag_type', '=', default_type)
        ], limit=1)
        
        if override and override.tag_name:
            return override.tag_name
        
        return f"%{default_type}%"

    def resolve_plc_value_to_name(self, plc_value):
        self.ensure_one()
        plc_str = str(plc_value)
        
        if not plc_str.isdigit():
            return plc_str
            
        plc_int = int(plc_str)
        
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('plc_value', '=', plc_int)
        ], limit=1)
        
        if override and override.event_id:
            return override.event_id.name
            
        dict_event = self.env['mes.event'].search([
            ('default_plc_value', '=', plc_int)
        ], limit=1)
        
        if dict_event:
            return dict_event.name
            
        return plc_str
    
    def get_top_alarm_str(self, active_intervals, wc_id):
        if not active_intervals:
            return "None"

        start_time = active_intervals[0][0]
        end_time = active_intervals[-1][1]
        now_utc = fields.Datetime.now()

        val_list = [f"('{a_s.isoformat()}'::timestamp, '{a_e.isoformat()}'::timestamp)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        query = f"""
            WITH active_windows AS ( {active_cte} ),
            alarms AS (
                SELECT DISTINCT a.loss_id, a.start_time, COALESCE(a.end_time, %s) as end_time
                FROM mes_performance_alarm a
                JOIN mes_machine_performance p ON p.id = a.performance_id
                WHERE p.machine_id = %s AND a.start_time < %s AND (a.end_time > %s OR a.end_time IS NULL)
            ),
            intersected AS (
                SELECT a.loss_id,
                       GREATEST(a.start_time, aw.ai_start) as eff_start,
                       LEAST(a.end_time, aw.ai_end) as eff_end
                FROM alarms a
                INNER JOIN active_windows aw ON aw.ai_start < a.end_time AND aw.ai_end > a.start_time
            )
            SELECT loss_id, SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))) as total_dur 
            FROM intersected
            WHERE eff_start < eff_end
            GROUP BY loss_id 
            ORDER BY total_dur DESC LIMIT 1;
        """
        self.env.cr.execute(query, (now_utc, wc_id, end_time, start_time))
        res = self.env.cr.fetchone()
        
        if res and res[0]:
            loss = self.env['mes.event'].browse(res[0])
            duration_min = int((res[1] or 0) // 60)
            return f"{loss.name} ({duration_min} min)"
            
        return "None"

    def _get_planned_working_intervals(self, start_time, end_time, workcenter):

        now_utc = fields.Datetime.now()
        
        if workcenter:
            tz_str = workcenter.company_id.tz or 'UTC'
            mac_tz = pytz.timezone(tz_str)
            
            now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
            
            s_utc = mac_tz.localize(start_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            e_utc = mac_tz.localize(min(now_mac, end_time), is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        else:
            now_mac = now_utc
            s_utc = start_time
            e_utc = min(now_utc, end_time)

        calc_end_time = min(now_mac, end_time)

        if start_time >= calc_end_time:
            return [], 0.0

        if not workcenter:
            return [(start_time, calc_end_time)], (calc_end_time - start_time).total_seconds()

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

        dt_merged = []
        if intervals:
            intervals.sort(key=lambda x: x[0])
            dt_merged = [intervals[0]]
            for current in intervals[1:]:
                last = dt_merged[-1]
                if current[0] <= last[1]:
                    dt_merged[-1] = [last[0], max(last[1], current[1])]
                else:
                    dt_merged.append(current)
                

        active_intervals = []
        current_time = start_time
        for dt in dt_merged:
            if current_time < dt[0]:
                active_intervals.append((current_time, dt[0]))
            current_time = max(current_time, dt[1])

        if current_time < calc_end_time:
            active_intervals.append((current_time, calc_end_time))

        total_planned_runtime_sec = sum((i[1] - i[0]).total_seconds() for i in active_intervals)

        return active_intervals, total_planned_runtime_sec

    def _fetch_interval_stats(self, active_intervals, wc_id, mode='runtime'):
        if not active_intervals:
            if mode == 'downtime': return []
            elif mode == 'first_start': return False
            else: return 0.0

        start_time = active_intervals[0][0]
        end_time = active_intervals[-1][1]
        now_utc = fields.Datetime.now()

        val_list = [f"('{a_s.isoformat()}'::timestamp, '{a_e.isoformat()}'::timestamp)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        table_map = {
            'runtime': 'mes_performance_running',
            'downtime': 'mes_performance_alarm',
            'slowing': 'mes_performance_slowing',
            'first_start': 'mes_performance_running'
        }
        tbl = table_map.get(mode, 'mes_performance_running')

        if mode == 'runtime':
            query = f"""
                WITH active_windows AS ( {active_cte} ),
                runs AS (
                    SELECT DISTINCT start_time, COALESCE(end_time, %s) as end_time
                    FROM {tbl} r
                    JOIN mes_machine_performance p ON p.id = r.performance_id
                    WHERE p.machine_id = %s AND start_time < %s AND (end_time > %s OR end_time IS NULL)
                ),
                intersected AS (
                    SELECT GREATEST(start_time, aw.ai_start) as eff_start,
                           LEAST(end_time, aw.ai_end) as eff_end
                    FROM runs
                    INNER JOIN active_windows aw ON aw.ai_start < runs.end_time AND aw.ai_end > runs.start_time
                )
                SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))), 0)
                FROM intersected WHERE eff_start < eff_end
            """
            self.env.cr.execute(query, (now_utc, wc_id, end_time, start_time))
            res = self.env.cr.fetchone()
            return float(res[0]) if res else 0.0

        elif mode == 'downtime':
            query = f"""
                WITH active_windows AS ( {active_cte} ),
                alarms AS (
                    SELECT DISTINCT loss_id, start_time, COALESCE(end_time, %s) as end_time
                    FROM {tbl} a
                    JOIN mes_machine_performance p ON p.id = a.performance_id
                    WHERE p.machine_id = %s AND start_time < %s AND (end_time > %s OR end_time IS NULL)
                ),
                intersected AS (
                    SELECT loss_id,
                           GREATEST(start_time, aw.ai_start) as eff_start,
                           LEAST(end_time, aw.ai_end) as eff_end
                    FROM alarms
                    INNER JOIN active_windows aw ON aw.ai_start < alarms.end_time AND aw.ai_end > alarms.start_time
                )
                SELECT loss_id, COUNT(*) as freq, COALESCE(SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))), 0) as total_dur
                FROM intersected WHERE eff_start < eff_end
                GROUP BY loss_id
            """
            self.env.cr.execute(query, (now_utc, wc_id, end_time, start_time))
            return self.env.cr.fetchall()
            
        elif mode == 'first_start':
            query = f"""
                WITH active_windows AS ( {active_cte} ),
                runs AS (
                    SELECT DISTINCT start_time, COALESCE(end_time, %s) as end_time
                    FROM {tbl} r
                    JOIN mes_machine_performance p ON p.id = r.performance_id
                    WHERE p.machine_id = %s AND start_time < %s AND (end_time > %s OR end_time IS NULL)
                ),
                intersected AS (
                    SELECT GREATEST(start_time, aw.ai_start) as eff_start,
                           LEAST(end_time, aw.ai_end) as eff_end
                    FROM runs
                    INNER JOIN active_windows aw ON aw.ai_start < runs.end_time AND aw.ai_end > runs.start_time
                )
                SELECT MIN(eff_start) FROM intersected WHERE eff_start < eff_end
            """
            self.env.cr.execute(query, (now_utc, wc_id, end_time, start_time))
            res = self.env.cr.fetchone()
            return res[0] if res and res[0] else False
        
    def _fetch_waste_stats_raw(self, cursor, start_time, end_time):
        query = """
            SELECT tag_name, 
                   COALESCE(SUM(value), 0) as sum_val, 
                   COALESCE(MAX(value) - MIN(value), 0) as cum_val
            FROM telemetry_count 
            WHERE machine_name = %s AND time >= %s AND time < %s
            GROUP BY tag_name
        """
        cursor.execute(query, (self.name, start_time, end_time))

        return {row[0]: {'sum': float(row[1]), 'cum': float(row[2])} for row in cursor.fetchall()}

    def _fetch_timeline_raw(self, start_time, end_time, wc_id):
        
        wc = self.env['mrp.workcenter'].browse(wc_id)
        tz_name = wc.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        
        s_utc = mac_tz.localize(start_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        e_utc = mac_tz.localize(end_time, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        
        def to_local_str(dt_utc):
            if not dt_utc:
                return ''
            if isinstance(dt_utc, str):
                dt_utc = fields.Datetime.from_string(dt_utc)
            local_dt = pytz.utc.localize(dt_utc).astimezone(mac_tz)
            return local_dt.replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')

        perfs = self.env['mes.machine.performance'].search([
            ('machine_id', '=', wc_id), 
            ('date', '>=', start_time.date() - timedelta(days=1)), 
            ('date', '<=', end_time.date() + timedelta(days=1))
        ])
        
        if not perfs:
            return []
            
        runs = self.env['mes.performance.running'].search([
            ('performance_id', 'in', perfs.ids), 
            ('start_time', '<', e_utc), 
            '|', ('end_time', '>', s_utc), ('end_time', '=', False)
        ])
        alarms = self.env['mes.performance.alarm'].search([
            ('performance_id', 'in', perfs.ids), 
            ('start_time', '<', e_utc), 
            '|', ('end_time', '>', s_utc), ('end_time', '=', False)
        ])
        slows = self.env['mes.performance.slowing'].search([
            ('performance_id', 'in', perfs.ids), 
            ('start_time', '<', e_utc), 
            '|', ('end_time', '>', s_utc), ('end_time', '=', False)
        ])
        
        res = []
        now_utc = fields.Datetime.now()
        
        for r in runs:
            e_t = r.end_time or now_utc
            color = r.loss_id.color or '#28a745'
            res.append({
                'start': to_local_str(r.start_time),
                'end': to_local_str(e_t),
                'name': r.loss_id.name if r.loss_id else 'Running',
                'status': 'running',
                'color': color,
                'duration': (e_t - r.start_time).total_seconds()
            })
            
        for a in alarms:
            e_t = a.end_time or now_utc
            color = a.loss_id.color or '#dc3545'
            res.append({
                'start': to_local_str(a.start_time),
                'end': to_local_str(e_t),
                'name': a.loss_id.name if a.loss_id else 'Alarm',
                'status': 'alarm',
                'color': color,
                'duration': (e_t - a.start_time).total_seconds()
            })
            
        for s in slows:
            e_t = s.end_time or now_utc
            color = s.loss_id.color or '#6c757d'
            res.append({
                'start': to_local_str(s.start_time),
                'end': to_local_str(e_t),
                'name': s.loss_id.name if s.loss_id else 'Slowing',
                'status': 'slowing',
                'color': color,
                'duration': (e_t - s.start_time).total_seconds()
            })
            
        return res

    def _fetch_production_chart_raw(self, cursor, tag_names, start_time, end_time, bucket_min):
        if not tag_names: return []
        query = f"""
            SELECT tag_name, time_bucket('{bucket_min} minutes', time) AS bucket,
                   COALESCE(SUM(value), 0) as sum_val,
                   COALESCE(MAX(value) - MIN(value), 0) as cum_val
            FROM telemetry_count
            WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time < %s
            GROUP BY tag_name, bucket ORDER BY bucket
        """
        cursor.execute(query, (self.name, tag_names, start_time, end_time))
        return cursor.fetchall()

    def _calculate_kpi(self, total_running_sec, total_produced, total_planned_runtime_sec, workcenter):
        total_running_sec = max(0.0, total_running_sec)
        
        h, m, s = int(total_running_sec // 3600), int((total_running_sec % 3600) // 60), int(total_running_sec % 60)
        runtime_formatted = f"{h:02d}:{m:02d}:{s:02d}"

        ideal_rate_per_sec = (workcenter.ideal_capacity_per_min / 60.0) if (workcenter and workcenter.ideal_capacity_per_min > 0) else 1.0 
            
        raw_availability = total_running_sec / total_planned_runtime_sec if total_planned_runtime_sec > 0 else 0
        availability = max(0.0, min(raw_availability, 1.0))
        
        raw_performance = total_produced / (total_running_sec * ideal_rate_per_sec) if total_running_sec > 0 else 0
        performance = max(0.0, min(raw_performance, 1.0))
        
        quality = 1.0 
        oee = availability * performance * quality

        downtime_losses = max(0.0, 1.0 - raw_availability) if total_planned_runtime_sec > 0 else 0.0
        perfect_amount_for_runtime = total_running_sec * ideal_rate_per_sec

        waste_losses = 1 - (total_produced / perfect_amount_for_runtime) if (total_planned_runtime_sec > 0 and perfect_amount_for_runtime > 0) else 0.0
        waste_losses = max(0.0, waste_losses)

        return {
            'availability': round(availability * 100, 2),
            'performance': round(performance * 100, 2),
            'quality': round(quality * 100, 2),
            'oee': round(oee * 100, 2),
            'waste_losses': round(waste_losses * 100, 2),
            'downtime_losses': round(downtime_losses * 100, 2),
            'total_produced': total_produced,
            'runtime_formatted': runtime_formatted,
        }

    def _calculate_kpi_for_window(self, workcenter, start_time, end_time):
        if not workcenter: return None
        machine = workcenter.machine_settings_id

        active_intervals, total_planned_sec = self._get_planned_working_intervals(start_time, end_time, workcenter)
        if total_planned_sec <= 0: return None

        total_running_sec = self._fetch_interval_stats(active_intervals, workcenter.id, mode='runtime')
        
        total_produced = 0.0
        doc = self.env['mes.machine.performance'].search([
            ('machine_id', '=', workcenter.id),
            ('state', '=', 'done')
        ]).filtered(lambda d: d._get_utc_time(d._get_local_shift_times()[0]) <= start_time and d._get_utc_time(d._get_local_shift_times()[1]) >= end_time)

        if doc:
            prods = doc.production_ids.filtered(lambda p: p.reason_id == workcenter.production_count_id)
            total_produced = sum(prods.mapped('qty'))
        else:
            good_count_tag, is_cumulative = workcenter.production_count_id.get_count_config_for_machine(machine) if workcenter.production_count_id else (None, False)
            if good_count_tag:
                with self.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        if is_cumulative:
                            cur.execute("SELECT COALESCE(MAX(value) - MIN(value), 0) FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time < %s", (self.name, good_count_tag, start_time, end_time))
                        else:
                            cur.execute("SELECT COALESCE(SUM(value), 0) FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time < %s", (self.name, good_count_tag, start_time, end_time))
                        res = cur.fetchone()
                        if res and res[0]:
                            total_produced = float(res[0])
                
        kpi = self._calculate_kpi(total_running_sec, total_produced, total_planned_sec, workcenter)
        kpi['produced'] = total_produced
        kpi['first_running_time'] = self._fetch_interval_stats(active_intervals, workcenter.id, mode='first_start')
        
        return kpi

    def action_open_waste_losses(self):
        self.ensure_one()
        
        self.env['mes.waste.loss.stat'].search([
            ('machine_id', '=', self.id),
            ('create_uid', '=', self.env.uid)
        ]).unlink()
        
        self.env['mes.waste.loss.stat']._generate_stats(self.id)
        
        return {
            'name': 'Waste Losses Details',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.waste.loss.stat',
            'view_mode': 'tree',
            'domain': [('machine_id', '=', self.id), ('create_uid', '=', self.env.uid)],
            'context': {'default_machine_id': self.id},
            'target': 'new',
        }

    def action_open_downtime_losses(self):
        self.ensure_one()
        
        self.env['mes.downtime.loss.stat'].search([
            ('machine_id', '=', self.id),
            ('create_uid', '=', self.env.uid)
        ]).unlink()
        
        self.env['mes.downtime.loss.stat']._generate_stats(self.id)
        
        return {
            'name': 'Downtime Losses Details',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.downtime.loss.stat',
            'view_mode': 'tree',
            'domain': [('machine_id', '=', self.id), ('create_uid', '=', self.env.uid)],
            'context': {'default_machine_id': self.id},
            'target': 'new',
        }

    @api.model
    def get_realtime_oee_batch(self, wcs):
        if not wcs:
            return {}

        res = {}
        cfgs = {}

        for wc in wcs:
            mac = wc.machine_settings_id
            if not mac: continue

            s_time, e_time = self.env['mes.shift'].get_current_shift_window(wc)
            if not s_time or not e_time:
                res[wc.id] = {'error': 'No active shift'}
                continue
                
            now_utc = fields.Datetime.now()
            mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
            calc_e_time = min(now_mac, e_time)

            state_tag, run_val = wc.runtime_event_id.get_mapping_for_machine(mac) if wc.runtime_event_id else (None, None)
            count_tag, is_cumul = wc.production_count_id.get_count_config_for_machine(mac) if wc.production_count_id else (None, False)

            if not state_tag or not count_tag:
                res[wc.id] = {'error': 'Configuration error: Missing state or count tag'}
                continue

            act_ints, plan_sec = mac._get_planned_working_intervals(s_time, e_time, wc)

            cfgs[wc.id] = {
                'mac': mac,
                'count_tag': count_tag,
                'is_cumul': is_cumul,
                'act_ints': act_ints,
                'plan_sec': plan_sec,
                'count_id': wc.production_count_id.id,
                's_time': s_time,
                'calc_e_time': calc_e_time
            }

        if not cfgs:
            return res

        ts_base = self.env['mes.timescale.base']
        
        with ts_base._connection() as conn:
            with conn.cursor() as cur:
                c_data = {}
                
                for wc_id, cfg in cfgs.items():
                    m_name = cfg['mac'].name
                    if m_name not in c_data:
                        q_counts = """
                            SELECT tag_name, 
                                   COALESCE(SUM(value), 0) as sum_val, 
                                   COALESCE(MAX(value) - MIN(value), 0) as cum_val
                            FROM telemetry_count 
                            WHERE machine_name = %s AND time >= %s AND time < %s
                            GROUP BY tag_name
                        """
                        cur.execute(q_counts, (m_name, cfg['s_time'], cfg['calc_e_time']))
                        c_data[m_name] = {}
                        for t_name, s_val, c_val in cur.fetchall():
                            c_data[m_name][t_name] = {'sum': float(s_val), 'cum': float(c_val)}

        all_rej = self.env['mes.counts'].search([])

        for wc_id, cfg in cfgs.items():
            mac = cfg['mac']
            m_name = mac.name
            
            t_data = c_data.get(m_name, {}).get(cfg['count_tag'], {})
            tot_prod = t_data.get('cum' if cfg['is_cumul'] else 'sum', 0)

            top_rej_cnt = 0
            top_rej_name = "None"
            
            for r_cnt in all_rej:
                if r_cnt.id == cfg['count_id']: continue
                r_tag, r_is_cum = r_cnt.get_count_config_for_machine(mac)
                if not r_tag: continue
                    
                r_val = c_data.get(m_name, {}).get(r_tag, {})
                r_amt = r_val.get('cum' if r_is_cum else 'sum', 0)
                
                if r_amt > top_rej_cnt:
                    top_rej_cnt = r_amt
                    top_rej_name = r_cnt.name

            top_rej_str = f"{top_rej_name} ({int(top_rej_cnt)})" if top_rej_cnt > 0 else "None"
            
            top_al_str = mac.get_top_alarm_str(cfg['act_ints'], wc_id)
            run_sec = mac._fetch_interval_stats(cfg['act_ints'], wc_id, mode='runtime')
            first_run = mac._fetch_interval_stats(cfg['act_ints'], wc_id, mode='first_start')

            wc = wcs.browse(wc_id)
            kpi = mac._calculate_kpi(run_sec, tot_prod, cfg['plan_sec'], wc)
            
            kpi.update({
                'first_running_time': first_run,
                'top_alarm': top_al_str,
                'top_rejection': top_rej_str
            })
            
            res[wc_id] = kpi

        return res

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
        ('auto', 'Auto'), ('bool', 'Boolean'), ('int', 'Integer'),
        ('double', 'Double/Real'), ('string', 'String')
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
            rec.machine_id.name, rec.tag_name, 
            rec.poll_type, rec.poll_frequency, rec.param_type, self._signal_type
        ))

class MesSignalCount(models.Model):
    _name = 'mes.signal.count'
    _inherit = 'mes.signal.base'
    _description = 'Count Signals'
    _signal_type = 'count'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    count_id = fields.Many2one('mes.counts', string='Dictionary Count', required=True)

    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)

    _sql_constraints = [
        ('tag_count_uniq', 'unique(machine_id, tag_name, count_id)', 'This exact Tag to Count mapping already exists!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

    @api.onchange('count_id')
    def _onchange_count_id(self):
        if self.count_id:
            self.is_cumulative = self.count_id.is_cumulative

class MesSignalEvent(models.Model):
    _name = 'mes.signal.event'
    _inherit = 'mes.signal.base'
    _description = 'Event Signals'
    _signal_type = 'event'

    poll_type = fields.Selection(selection_add=[], default='on_change')

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    event_id = fields.Many2one('mes.event', string='Dictionary Event', required=True)
    plc_value = fields.Integer(string='PLC Value', required=True)

    _sql_constraints = [
        ('tag_val_event_uniq', 'unique(machine_id, tag_name, plc_value, event_id)', 'This exact Tag+Value to Event mapping already exists!')
    ]

    def unlink(self):
        for rec in self:
            remaining = self.search_count([
                ('machine_id', '=', rec.machine_id.id),
                ('tag_name', '=', rec.tag_name),
                ('id', '!=', rec.id)
            ])
            if remaining == 0:
                self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

class MesSignalProcess(models.Model):
    _name = 'mes.signal.process'
    _inherit = 'mes.signal.base'
    _description = 'Process Signals'
    _signal_type = 'process'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    process_id = fields.Many2one('mes.process', string='Dictionary Process', required=True)

    _sql_constraints = [
        ('tag_process_uniq', 'unique(machine_id, tag_name, process_id)', 'This exact Tag to Process mapping already exists!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
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
        if not machine.exists(): return
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window(workcenter)
        if not start_time: return
        
        now_utc = fields.Datetime.now()
        mac_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        calc_end_time = min(now_mac, shift_end)
        
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        total_running_sec = machine._fetch_interval_stats(active_intervals, workcenter.id, mode='runtime')
        hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0

        vals_list = []
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                raw_counts = machine._fetch_waste_stats_raw(cur, start_time, calc_end_time)
                
                waste_by_dict = {}
                for count_def in machine.count_tag_ids:
                    if workcenter and count_def.count_id == workcenter.production_count_id:
                        continue
                        
                    tag_data = raw_counts.get(count_def.tag_name, {'sum': 0.0, 'cum': 0.0})
                    waste_val = tag_data.get('cum') if count_def.is_cumulative else tag_data.get('sum')
                    
                    if waste_val > 0:
                        dict_name = count_def.count_id.name if count_def.count_id else count_def.tag_name
                        waste_by_dict[dict_name] = waste_by_dict.get(dict_name, 0.0) + waste_val
                
                for name, val in waste_by_dict.items():
                    vals_list.append({
                        'machine_id': machine.id,
                        'name': name,
                        'waste_sum': val,
                        'waste_per_hour': (val / hours_run) if hours_run > 0 else 0.0
                    })
                    
        if vals_list:
            self.with_context(skip_generation=True).create(vals_list)

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
        if not machine.exists(): return
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window(workcenter)
        if not start_time: return
        
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        total_running_sec = machine._fetch_interval_stats(active_intervals, workcenter.id, mode='runtime')
        hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0
        
        rows = machine._fetch_interval_stats(active_intervals, workcenter.id, mode='downtime')
        
        stats_by_event = {}
        for row in rows:
            loss_id, freq, duration_sec = row[0], row[1], row[2]
            
            evt_name = self.env['mes.event'].browse(loss_id).name
            if evt_name not in stats_by_event:
                stats_by_event[evt_name] = {'freq': 0, 'dur': 0.0}
            stats_by_event[evt_name]['freq'] += freq
            stats_by_event[evt_name]['dur'] += duration_sec
        
        vals_list = []
        for evt_name, data in stats_by_event.items():
            dur_min = data['dur'] / 60.0
            if dur_min > 0 or data['freq'] > 0:
                vals_list.append({
                    'machine_id': machine.id,
                    'name': evt_name,
                    'frequency': data['freq'],
                    'freq_per_hour': (data['freq'] / hours_run) if hours_run > 0 else 0.0,
                    'total_time': dur_min,
                    'time_per_hour': (dur_min / hours_run) if hours_run > 0 else 0.0
                })
        
        if vals_list:
            self.with_context(skip_generation=True).create(vals_list)
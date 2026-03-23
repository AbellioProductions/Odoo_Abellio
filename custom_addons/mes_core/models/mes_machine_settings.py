import re
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
    
    def get_top_alarm_str(self, cursor, active_intervals):
        if not active_intervals:
            return "None"

        start_time = active_intervals[0][0]
        end_time = active_intervals[-1][1]

        val_list = [f"('{a_s} UTC'::timestamptz, '{a_e} UTC'::timestamptz)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        alarm_tag = self.get_alarm_tag_name('OEE.nStopRootReason')

        alarm_query = f"""
            WITH active_windows AS ( {active_cte} ),
            boundary AS (
                SELECT time, tag_name, value FROM telemetry_event
                WHERE machine_name = %s AND time < %s 
                ORDER BY time DESC LIMIT 1
            ),
            events AS (
                SELECT GREATEST(time, %s) as time, tag_name, value FROM boundary 
                UNION ALL
                SELECT time, tag_name, value FROM telemetry_event
                WHERE machine_name = %s AND time >= %s AND time <= %s
            ),
            state_durations AS (
                SELECT tag_name, value as alarm_code, time as state_start,
                       COALESCE(LEAD(time) OVER (ORDER BY time ASC), %s) as state_end
                FROM events
            ),
            intersected_durations AS (
                SELECT tag_name, alarm_code,
                       GREATEST(state_start, aw.ai_start) as eff_start,
                       LEAST(state_end, aw.ai_end) as eff_end
                FROM state_durations sd
                INNER JOIN active_windows aw ON aw.ai_start < sd.state_end AND aw.ai_end > sd.state_start
            )
            SELECT alarm_code, SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))) as total_dur 
            FROM intersected_durations
            WHERE tag_name LIKE %s AND alarm_code != 0 AND alarm_code IS NOT NULL AND eff_start < eff_end
            GROUP BY alarm_code 
            ORDER BY total_dur DESC LIMIT 1;
        """
        cursor.execute(alarm_query, (
            self.name, start_time,   
            start_time, self.name, start_time, end_time,
            end_time,
            alarm_tag
        ))
        res_al = cursor.fetchone()
        
        if res_al and res_al[0]:
            duration_min = int((res_al[1] or 0) // 60)
            alarm_name = self.resolve_plc_value_to_name(res_al[0])
            return f"{alarm_name} ({duration_min} min)"
            
        return "None"


    def _get_planned_working_intervals(self, start_time, end_time, workcenter):
        now = fields.Datetime.now()
        calc_end_time = min(now, end_time)

        if start_time >= calc_end_time:
            return [], 0.0

        if not workcenter:
            return [(start_time, calc_end_time)], (calc_end_time - start_time).total_seconds()

        downtimes = self.env['mes.flat.downtime'].search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', calc_end_time),
            ('end_time', '>', start_time)
        ])

        intervals = []
        for dt in downtimes:
            dt_s = max(dt.start_time, start_time)
            dt_e = min(dt.end_time, calc_end_time)

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

    def _fetch_interval_stats(self, cursor, active_intervals, tags, mode='runtime', state_tag=None, state_val=None):
        if not active_intervals or not tags:
            if mode == 'downtime': return []
            elif mode == 'first_start': return False
            else: return 0.0

        start_time = active_intervals[0][0]
        end_time = active_intervals[-1][1]

        val_list = [f"('{a_s} UTC'::timestamptz, '{a_e} UTC'::timestamptz)" for a_s, a_e in active_intervals]
        active_cte = "SELECT * FROM (VALUES " + ", ".join(val_list) + ") AS ai(ai_start, ai_end)"

        query = f"""
            WITH active_windows AS ( {active_cte} ),
            boundary AS (
                SELECT time, value, tag_name, 0::bigint as id 
                FROM telemetry_event
                WHERE machine_name = %s AND time < %s 
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            events AS (
                SELECT GREATEST(time, %s) as time, value, tag_name, id FROM boundary 
                UNION ALL
                SELECT time, value, tag_name, id FROM telemetry_event
                WHERE machine_name = %s AND time >= %s AND time <= %s
            ),
            state_durations AS (
                SELECT id, tag_name, value, time as state_start,
                       COALESCE(LEAD(time) OVER (ORDER BY time ASC, id ASC), %s) as state_end
                FROM events
            ),
            intersected_durations AS (
                SELECT sd.id, sd.tag_name, sd.value,
                       GREATEST(sd.state_start, aw.ai_start) as eff_start,
                       LEAST(sd.state_end, aw.ai_end) as eff_end
                FROM state_durations sd
                INNER JOIN active_windows aw ON aw.ai_start < sd.state_end AND aw.ai_end > sd.state_start
            )
        """

        args = [
            self.name, start_time,   
            start_time, self.name, start_time, end_time,
            end_time
        ]

        if mode == 'runtime':
            query += """
                SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))), 0)
                FROM intersected_durations
                WHERE tag_name = ANY(%s) AND value = %s AND eff_start < eff_end
            """
            args.extend([tags, str(state_val)])
            cursor.execute(query, tuple(args))
            res = cursor.fetchone()
            return float(res[0]) if res else 0.0

        elif mode == 'downtime':
            query += """
                SELECT tag_name, value as alarm_code, COUNT(DISTINCT id) as freq, SUM(EXTRACT(EPOCH FROM (eff_end - eff_start))) as total_dur
                FROM intersected_durations
                WHERE tag_name = ANY(%s) AND eff_start < eff_end
                GROUP BY tag_name, value
            """
            args.extend([tags])
            cursor.execute(query, tuple(args))
            return cursor.fetchall()
            
        elif mode == 'first_start':
            query += """
                SELECT MIN(eff_start)
                FROM intersected_durations
                WHERE tag_name = ANY(%s) AND value = %s AND eff_start < eff_end
            """
            args.extend([tags, str(state_val)])
            cursor.execute(query, tuple(args))
            res = cursor.fetchone()
            return res[0].replace(tzinfo=None) if res and res[0] else False
        
    def _fetch_waste_stats_raw(self, cursor, start_time, end_time):
        query = """
            SELECT tag_name, 
                   COALESCE(SUM(value), 0) as sum_val, 
                   COALESCE(MAX(value) - MIN(value), 0) as cum_val
            FROM telemetry_count 
            WHERE machine_name = %s AND time >= %s AND time <= %s
            GROUP BY tag_name
        """
        cursor.execute(query, (self.name, start_time, end_time))

        return {row[0]: {'sum': float(row[1]), 'cum': float(row[2])} for row in cursor.fetchall()}

    def _fetch_timeline_raw(self, cursor, start_time, end_time, event_tags):
        if not event_tags: return []
        query = """
            WITH boundary AS (
                SELECT time as time, value, tag_name, 0::bigint as id 
                FROM telemetry_event
                WHERE machine_name = %s AND time < %s 
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            events AS (
                SELECT GREATEST(time, %s) as time, value, tag_name, id FROM boundary 
                UNION ALL
                SELECT time, value, tag_name, id FROM telemetry_event
                WHERE machine_name = %s AND time >= %s AND time <= %s
            ),
            intervals AS (
                SELECT time as start_time, 
                       COALESCE(LEAD(time) OVER (ORDER BY time, id), %s) as end_time,
                       value, tag_name
                FROM events
            )
            SELECT start_time, end_time, value, tag_name 
            FROM intervals 
            WHERE start_time < end_time
        """
        cursor.execute(query, (
            self.name, start_time,
            start_time, self.name, start_time, end_time,
            end_time
        ))
        return cursor.fetchall()

    def _fetch_production_chart_raw(self, cursor, tag_names, start_time, end_time, bucket_min):
        if not tag_names: return []
        query = f"""
            SELECT tag_name, time_bucket('{bucket_min} minutes', time) AS bucket,
                   COALESCE(SUM(value), 0) as sum_val,
                   COALESCE(MAX(value) - MIN(value), 0) as cum_val
            FROM telemetry_count
            WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
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
        
        if not workcenter:
            return None
        
        machine = workcenter.machine_settings_id
        
        state_tag, running_plc_value = workcenter.runtime_event_id.get_mapping_for_machine(machine) if workcenter.runtime_event_id else (None, None)
        good_count_tag, is_cumulative = workcenter.production_count_id.get_count_config_for_machine(machine) if workcenter.production_count_id else (None, False)

        if not state_tag or not good_count_tag:
            return None

        active_intervals, total_planned_sec = self._get_planned_working_intervals(start_time, end_time, workcenter)
        if total_planned_sec <= 0:
            return None

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                total_running_sec = self._fetch_interval_stats(
                    cur, active_intervals, [state_tag], mode='runtime', state_tag=state_tag, state_val=running_plc_value
                )
                
                total_produced = 0
                if is_cumulative:
                    cur.execute("SELECT COALESCE(MAX(value) - MIN(value), 0) FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s", (self.name, good_count_tag, start_time, end_time))
                else:
                    cur.execute("SELECT COALESCE(SUM(value), 0) FROM telemetry_count WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s", (self.name, good_count_tag, start_time, end_time))
                res = cur.fetchone()
                if res and res[0]:
                    total_produced += float(res[0])
                        
                kpi = self._calculate_kpi(total_running_sec, total_produced, total_planned_sec, workcenter)
                
                kpi['produced'] = total_produced
                kpi['first_running_time'] = self._fetch_interval_stats(
                    cur, active_intervals, [state_tag], mode='first_start', state_tag=state_tag, state_val=running_plc_value
                )
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
    def get_realtime_oee_batch(self, workcenters):
        if not workcenters:
            return {}

        dummy_setting = self.search([], limit=1)
        if not dummy_setting:
            return {wc.id: {'error': 'No machine settings found'} for wc in workcenters}

        start_time, shift_end = dummy_setting.env['mes.shift'].get_current_shift_window()
        if not start_time or not shift_end:
            return {wc.id: {'error': 'No active shift'} for wc in workcenters}

        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        results = {}
        configs = {}
        machine_names = []

        for wc in workcenters:
            machine = wc.machine_settings_id
            if not machine:
                continue

            state_tag, running_plc_value = wc.runtime_event_id.get_mapping_for_machine(machine) if wc.runtime_event_id else (None, None)
            good_count_tag, is_cumulative = wc.production_count_id.get_count_config_for_machine(machine) if wc.production_count_id else (None, False)

            if not state_tag or not good_count_tag:
                results[wc.id] = {'error': 'Configuration error: Missing state or count tag'}
                continue

            active_intervals, total_planned_runtime_sec = machine._get_planned_working_intervals(start_time, shift_end, wc)

            configs[wc.id] = {
                'machine': machine,
                'state_tag': state_tag,
                'running_plc_value': running_plc_value,
                'good_count_tag': good_count_tag,
                'is_cumulative': is_cumulative,
                'active_intervals': active_intervals,
                'total_planned_sec': total_planned_runtime_sec,
                'prod_count_id': wc.production_count_id.id
            }
            if machine.name not in machine_names:
                machine_names.append(machine.name)

        if not configs:
            return results

        ts_manager = self.env['mes.timescale.base']
        
        with ts_manager._connection() as conn:
            with conn.cursor() as cur:
                query_counts = """
                    SELECT machine_name, tag_name, 
                           COALESCE(SUM(value), 0) as sum_val, 
                           COALESCE(MAX(value) - MIN(value), 0) as cum_val
                    FROM telemetry_count 
                    WHERE machine_name = ANY(%s) AND time >= %s AND time <= %s
                    GROUP BY machine_name, tag_name
                """
                cur.execute(query_counts, (machine_names, start_time, calc_end_time))
                
                count_data = {}
                for m_name, t_name, s_val, c_val in cur.fetchall():
                    count_data.setdefault(m_name, {})[t_name] = {'sum': float(s_val), 'cum': float(c_val)}

                all_reject_counts = self.env['mes.counts'].search([])

                for wc_id, cfg in configs.items():
                    machine = cfg['machine']
                    m_name = machine.name
                    
                    t_data = count_data.get(m_name, {}).get(cfg['good_count_tag'], {})
                    total_produced = t_data.get('cum' if cfg['is_cumulative'] else 'sum', 0)

                    top_rej_count = 0
                    top_rej_name = "None"
                    
                    for r_count in all_reject_counts:
                        if r_count.id == cfg['prod_count_id']:
                            continue
                        r_tag, r_is_cum = r_count.get_count_config_for_machine(machine)
                        if not r_tag: 
                            continue
                            
                        r_val = count_data.get(m_name, {}).get(r_tag, {})
                        r_amount = r_val.get('cum' if r_is_cum else 'sum', 0)
                        
                        if r_amount > top_rej_count:
                            top_rej_count = r_amount
                            top_rej_name = r_count.name

                    top_rejection_str = f"{top_rej_name} ({int(top_rej_count)})" if top_rej_count > 0 else "None"

                    top_alarm_str = machine.get_top_alarm_str(cur, cfg['active_intervals'])
                    
                    total_running_sec = machine._fetch_interval_stats(
                        cur, cfg['active_intervals'], [cfg['state_tag']], mode='runtime', 
                        state_tag=cfg['state_tag'], state_val=cfg['running_plc_value']
                    )
                    first_running_time = machine._fetch_interval_stats(
                        cur, cfg['active_intervals'], [cfg['state_tag']], mode='first_start', 
                        state_tag=cfg['state_tag'], state_val=cfg['running_plc_value']
                    )

                    wc = workcenters.browse(wc_id)
                    kpi = machine._calculate_kpi(
                        total_running_sec, total_produced, cfg['total_planned_sec'], wc
                    )
                    
                    kpi.update({
                        'first_running_time': first_running_time,
                        'top_alarm': top_alarm_str,
                        'top_rejection': top_rejection_str
                    })
                    
                    results[wc_id] = kpi

        return results

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
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window()
        if not start_time: return
        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        state_sig = machine.event_tag_ids.filtered(lambda x: x.event_id == workcenter.runtime_event_id)
        state_tag = state_sig[0].tag_name if state_sig else None
        running_plc_value = state_sig[0].plc_value if state_sig else 0

        vals_list = []
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                total_running_sec = machine._fetch_interval_stats(
                    cur, active_intervals, [state_tag], mode='runtime', state_tag=state_tag, state_val=running_plc_value
                ) if state_tag else 0.0
                
                hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0

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
        
        start_time, shift_end = self.env['mes.shift'].get_current_shift_window()
        if not start_time: return
        calc_end_time = min(fields.Datetime.now(), shift_end)
        
        workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
        active_intervals, _ = machine._get_planned_working_intervals(start_time, shift_end, workcenter)
        
        state_sig = machine.event_tag_ids.filtered(lambda x: x.event_id == workcenter.runtime_event_id)
        state_tag = state_sig[0].tag_name if state_sig else None
        running_plc_value = state_sig[0].plc_value if state_sig else 0

        #all_event_tags = list(set(machine.event_tag_ids.mapped('tag_name')))
        alarm_tag = machine.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
        
        vals_list = []
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                active_intervals = machine._get_planned_working_intervals(start_time, shift_end, workcenter)[0]

                _logger.info(f"Machine: {machine.id} - Active intervals for downtime {alarm_tag} stats: {active_intervals}")
                
                total_running_sec = machine._fetch_interval_stats(
                    cur, active_intervals, [state_tag], mode='runtime', state_tag=state_tag, state_val=running_plc_value
                ) if state_tag else 0.0
                hours_run = (total_running_sec / 3600.0) if total_running_sec > 0 else 0.0
                
                rows = machine._fetch_interval_stats(cur, active_intervals, [alarm_tag], mode='downtime')
                
                stats_by_event = {}
                for row in rows:
                    tag_name, alarm_code, freq, duration_sec = row[0], row[1], row[2], row[3]
                    
                    matched_signals = machine.event_tag_ids.filtered(lambda x: x.tag_name == tag_name and x.plc_value == alarm_code)
                    
                    if not matched_signals:
                        continue
                        
                    for sig in matched_signals:
                        evt_name = sig.event_id.name
                        if evt_name not in stats_by_event:
                            stats_by_event[evt_name] = {'freq': 0, 'dur': 0.0}
                        stats_by_event[evt_name]['freq'] += freq
                        stats_by_event[evt_name]['dur'] += duration_sec
                
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
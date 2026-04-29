import pytz
import logging
from datetime import datetime, timedelta, time
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

class MesMachinePerformance(models.Model):
    _name = 'mes.machine.performance'
    _description = 'Machine Performance Data (OEE)'
    _order = 'date desc, shift_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Doc ID', default='New', readonly=True, copy=False)
    date = fields.Date(string='Date', required=True, default=fields.Date.context_today)
    shift_id = fields.Many2one('mes.shift', string='Shift', required=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    company_id = fields.Many2one('res.company', string='Company', related='machine_id.company_id', store=True, readonly=True)
    state = fields.Selection([('draft', 'Draft'), ('done', 'Locked')], string='Status', default='draft', tracking=True)

    alarm_ids = fields.One2many('mes.performance.alarm', 'performance_id', string='Alarms')
    running_ids = fields.One2many('mes.performance.running', 'performance_id', string='Running Logs')
    slowing_ids = fields.One2many('mes.performance.slowing', 'performance_id', string='Slowing Logs')
    rejection_ids = fields.One2many('mes.performance.rejection', 'performance_id', string='Rejections')
    production_ids = fields.One2many('mes.performance.production', 'performance_id', string='Production Output')

    _sql_constraints = [
        ('uniq_report', 'unique(machine_id, date, shift_id)', 'Report for this shift already exists!')
    ]

    @api.model_create_multi
    def create(self, vals_list):
        machine_ids = [v.get('machine_id') for v in vals_list if v.get('machine_id')]
        machines = {m.id: m.name for m in self.env['mrp.workcenter'].browse(machine_ids)}

        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                date = vals.get('date')
                machine_id = vals.get('machine_id')
                machine_name = machines.get(machine_id, str(machine_id))
                vals['name'] = f"PERF/{date}/{machine_name}"
        
        return super().create(vals_list)

    def action_set_draft(self):
        for rec in self:
            if rec.state == 'done':
                rec.write({'state': 'draft'})

    @api.model
    def cron_process_pending_events(self):
        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', '!=', False)])
        for wc in workcenters:
            if wc.is_hist_syncing:
                continue
            try:
                self._sync_machine_fsm(wc)
            except Exception as e:
                _logger.error("CRON FSM FAULT | WC: %s | Err: %s", wc.name, str(e))

    def _sync_machine_fsm(self, wc):
        self.env.flush_all()
        
        open_states = []
        for model in ['mes.performance.running', 'mes.performance.alarm', 'mes.performance.slowing']:
            records = self.env[model].search([
                ('performance_id.machine_id', '=', wc.id),
                ('end_time', '=', False)
            ])
            open_states.extend(records)

        active_state = None
        if open_states:
            open_states.sort(key=lambda x: x.start_time, reverse=True)
            active_state = open_states[0]
            
            for orphan in open_states[1:]:
                if orphan.performance_id.state != 'done':
                    orphan.write({'end_time': active_state.start_time})

        latest_ts = None
        for model in ['mes.performance.running', 'mes.performance.alarm', 'mes.performance.slowing']:
            rec_start = self.env[model].search([('performance_id.machine_id', '=', wc.id)], order='start_time desc', limit=1)
            if rec_start and (not latest_ts or rec_start.start_time > latest_ts):
                latest_ts = rec_start.start_time
            
            rec_end = self.env[model].search([('performance_id.machine_id', '=', wc.id), ('end_time', '!=', False)], order='end_time desc', limit=1)
            if rec_end and (not latest_ts or rec_end.end_time > latest_ts):
                latest_ts = rec_end.end_time

        last_utc_ts = latest_ts if latest_ts else (fields.Datetime.now() - timedelta(days=1))
        
        tz_name = wc.company_id.tz or 'UTC'
        local_tz = pytz.timezone(tz_name)
        last_local_ts = pytz.utc.localize(last_utc_ts).astimezone(local_tz).replace(tzinfo=None)
        start_time_string = last_local_ts.strftime('%Y-%m-%d %H:%M:%S.%f')
        
        machine_settings = wc.machine_settings_id

        events = self._fetch_telemetry_events(
            workcenter=wc, 
            machine_settings=machine_settings, 
            start_time_string=start_time_string, 
            end_time_string=None, 
            include_initial=False
        )

        if not events:
            return

        for row in events:
            ts_raw, _, tag, val = row 
            
            evt_utc = self._convert_raw_time_to_utc(ts_raw, local_tz)
            
            if active_state and evt_utc <= active_state.start_time:
                continue

            tgt_model, evt_id = self._classify_telemetry_event(wc, machine_settings, tag, val)

            if not tgt_model or not evt_id:
                continue

            active_model = active_state._name if active_state else None
            active_loss_id = active_state.loss_id.id if active_state else None

            if active_model == tgt_model and active_loss_id == evt_id:
                continue

            if active_model == 'mes.performance.alarm' and tgt_model != 'mes.performance.running':
                continue

            if active_state and active_state.performance_id.state != 'done':
                active_state.write({'end_time': evt_utc})

            perf_doc = self._get_or_create_doc(wc, evt_utc)
            if not perf_doc or perf_doc.state == 'done':
                active_state = None
                continue

            active_state = self.env[tgt_model].create({
                'performance_id': perf_doc.id,
                'loss_id': evt_id,
                'start_time': evt_utc
            })

    @api.model
    def classify_fsm_transition(self, wc, tag, val):
        plc_val = int(val) if val is not None else 0
        mac = wc.machine_settings_id
        
        evt = self._resolve_event(mac, tag, plc_val)
        if not evt:
            return None, None

        is_run = False
        if wc.runtime_event_id:
            run_sig = mac.event_tag_ids.filtered(lambda x: x.event_id == wc.runtime_event_id)
            if run_sig:
                is_run = (run_sig[0].tag_name == tag and run_sig[0].plc_value == plc_val)
            else:
                is_run = (wc.runtime_event_id.default_event_tag_type == tag and wc.runtime_event_id.default_plc_value == plc_val)
        
        if is_run:
            return 'mes.performance.running', evt.id

        stop_tag = mac.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
        if tag == stop_tag or tag == 'OEE.nStopRootReason':
            return 'mes.performance.alarm', evt.id

        return 'mes.performance.slowing', evt.id

    @api.model
    def _resolve_event(self, mac, tag, val):
        sig = self.env['mes.signal.event'].search([('machine_id', '=', mac.id), ('tag_name', '=', tag), ('plc_value', '=', val)], limit=1)
        if sig: 
            return sig.event_id
            
        evt = self.env['mes.event'].search([('default_event_tag_type', '=', tag), ('default_plc_value', '=', val)], limit=1)
        if evt: 
            return evt
            
        grp = self.env['mes.event'].search([('name', '=', 'Unknown'), ('parent_id', '=', False)], limit=1)
        if not grp: 
            grp = self.env['mes.event'].create({'name': 'Unknown'})
            
        return self.env['mes.event'].create({
            'name': f'Unknown {tag} Code {val}',
            'parent_id': grp.id, 
            'default_event_tag_type': tag, 
            'default_plc_value': val
        })

    @api.model
    def cron_manage_shifts(self):
        now_utc = fields.Datetime.now()
        draft_docs = self.search([('state', '=', 'draft')], order='date asc', limit=50)

        for doc in draft_docs:
            try:
                shift_start_loc, shift_end_loc = doc._get_local_shift_times()
                shift_end_utc = doc._get_utc_time(shift_end_loc)
                
                closure_threshold_utc = shift_end_utc + timedelta(minutes=5)

                if now_utc >= closure_threshold_utc:
                    doc._close_shift_and_calculate_totals(shift_start_loc, shift_end_loc, shift_end_utc)
                    self.env.cr.commit()
            except Exception as exc:
                self.env.cr.rollback()
                _logger.error("SHIFT_CLOSE_FAIL | Doc: %s | Err: %s", doc.id, str(exc))

    def _close_shift_and_calculate_totals(self, shift_start_loc, shift_end_loc, shift_end_utc):
        workcenter = self.machine_id
        machine_settings = workcenter.machine_settings_id

        if not machine_settings:
            self.write(
            {
                'state': 'done'
            })
            return

        self._clear_previous_shift_data()

        shift_start_utc = self._get_utc_time(shift_start_loc)
        start_time_string = shift_start_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_time_string = shift_end_loc.strftime('%Y-%m-%d %H:%M:%S.%f')

        telemetry_events = self._fetch_telemetry_events(workcenter, machine_settings, start_time_string, end_time_string)
        self._process_telemetry_events(workcenter, machine_settings, telemetry_events, shift_end_utc)
        self._process_telemetry_counts(machine_settings, start_time_string, end_time_string)

        if self._is_empty_shift():
            self.unlink()
        else:
            self.write(
            {
                'state': 'done'
            })

    def _clear_previous_shift_data(self):
        self.running_ids.unlink()
        self.alarm_ids.unlink()
        self.slowing_ids.unlink()
        self.production_ids.unlink()
        self.rejection_ids.unlink()

    def _fetch_telemetry_events(self, workcenter, machine_settings, start_time_string, end_time_string=None, include_initial=True):
        is_state_logic = workcenter.telemetry_state_logic == 'states'
        
        ctes = []
        params = []
        
        if include_initial:
            ctes.append("""
                LastTags AS (
                    SELECT tag_name, value FROM (
                        SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time DESC) as rn
                        FROM telemetry_event
                        WHERE machine_name = %s AND time <= %s 
                          AND (tag_name = 'OEE.nMachineState' OR (tag_name = 'OEE.nStopRootReason' AND value <> 0)) 
                    ) sub WHERE rn = 1
                )
            """)
            params.extend([machine_settings.name, start_time_string])
            
            base_query = """
                SELECT CAST(%s AS TIMESTAMP) AS time,
                    CASE WHEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') = 1 THEN 'OEE.nStopRootReason' ELSE 'OEE.nMachineState' END AS tag_name,
                    CASE WHEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') = 1 THEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nStopRootReason') ELSE (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') END AS value
                WHERE (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') IS NOT NULL
                UNION ALL
            """
            params.append(start_time_string)
        else:
            base_query = ""

        time_filter = "time > %s"
        time_params = [start_time_string]
        
        if end_time_string:
            time_filter += " AND time <= %s"
            time_params.append(end_time_string)

        base_query += f"""
            SELECT time, tag_name, value 
            FROM telemetry_event 
            WHERE machine_name = %s AND {time_filter} AND (tag_name = 'OEE.nMachineState' OR (tag_name = 'OEE.nStopRootReason' AND value <> 0)) 
        """
        params.extend([machine_settings.name] + time_params)
        
        ctes.append(f"BaseEvents AS ({base_query})")

        if is_state_logic:
            ctes.append("""
                ReasonTimeline AS (
                    SELECT time, tag_name, value,
                           CASE WHEN tag_name = 'OEE.nStopRootReason' THEN time END AS reason_time,
                           CASE WHEN tag_name = 'OEE.nStopRootReason' THEN value END AS reason_value
                    FROM BaseEvents
                ),
                ReasonTimelineWithPrevNext AS (
                    SELECT time, tag_name, value,
                           MAX(reason_time) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS previous_reason_time,
                           MAX(reason_value) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS previous_reason_value,
                           MIN(reason_time) OVER (ORDER BY time ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_reason_time,
                           MIN(reason_value) OVER (ORDER BY time ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS next_reason_value
                    FROM ReasonTimeline
                ),
                ResolvedEvents AS (
                    SELECT time, 
                           CASE WHEN tag_name = 'OEE.nMachineState' AND value = 1 THEN 'OEE.nStopRootReason' ELSE tag_name END AS tag_name,
                           CASE WHEN tag_name = 'OEE.nMachineState' AND value = 1 THEN
                                CASE WHEN previous_reason_time IS NULL AND next_reason_time IS NULL THEN value
                                     WHEN previous_reason_time IS NULL THEN next_reason_value
                                     WHEN next_reason_time IS NULL THEN previous_reason_value
                                     WHEN EXTRACT(EPOCH FROM (next_reason_time - time)) < EXTRACT(EPOCH FROM (time - previous_reason_time)) THEN next_reason_value
                                     ELSE previous_reason_value END
                           ELSE value END AS value
                    FROM ReasonTimelineWithPrevNext
                )
            """)
            target_table = "ResolvedEvents"
        else:
            target_table = "BaseEvents"

        ctes.append(f"""
            FlickerFilter AS (
                SELECT time, tag_name, value,
                       LAG(tag_name) OVER (ORDER BY time) AS previous_tag,
                       LAG(value) OVER (ORDER BY time) AS previous_value,
                       LEAD(tag_name) OVER (ORDER BY time) AS next_tag,
                       LEAD(value) OVER (ORDER BY time) AS next_value
                FROM {target_table}
            ),
            FilteredEvents AS (
                SELECT time, tag_name, value
                FROM FlickerFilter
                WHERE NOT (tag_name = 'OEE.nMachineState' AND value IN (3, 5) AND previous_tag = 'OEE.nStopRootReason' AND previous_value = next_value)
            )
        """)

        if end_time_string:
            end_time_expr = f"""
                COALESCE(
                    LEAD(time) OVER (ORDER BY time), 
                    CASE WHEN EXISTS (SELECT 1 FROM telemetry_event WHERE machine_name = %s AND time > %s) THEN CAST(%s AS TIMESTAMP) ELSE NULL END
                )
            """
            params.extend([machine_settings.name, end_time_string, end_time_string])
        else:
            end_time_expr = "LEAD(time) OVER (ORDER BY time)"

        query = f"""
            WITH {','.join(ctes)}
            SELECT time AS start_time, {end_time_expr} AS end_time, tag_name, value
            FROM FilteredEvents
            ORDER BY time ASC
        """
        
        if not end_time_string:
            query += " LIMIT 5000"

        with self.env['mes.timescale.base']._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def _process_telemetry_events(self, workcenter, machine_settings, telemetry_events, shift_end_utc):
        timezone_name = workcenter.company_id.tz or 'UTC'
        local_timezone = pytz.timezone(timezone_name)

        current_target_model = None
        current_loss_identifier = None
        current_event_start_utc = None
        last_event_end_utc = None

        for event_row in telemetry_events:
            raw_start_time, raw_end_time, tag_name, value = event_row
            
            start_time_utc = self._convert_raw_time_to_utc(raw_start_time, local_timezone)
            end_time_utc = self._convert_raw_time_to_utc(raw_end_time, local_timezone) if raw_end_time else None

            target_model, loss_identifier = self._classify_telemetry_event(workcenter, machine_settings, tag_name, value)

            if target_model and loss_identifier:
                if target_model != current_target_model or loss_identifier != current_loss_identifier:
                    if current_target_model and current_loss_identifier and current_event_start_utc:
                        self.env[current_target_model].create(
                        {
                            'performance_id': self.id,
                            'loss_id': current_loss_identifier,
                            'start_time': current_event_start_utc,
                            'end_time': start_time_utc
                        })
                    
                    current_target_model = target_model
                    current_loss_identifier = loss_identifier
                    current_event_start_utc = start_time_utc

            last_event_end_utc = end_time_utc

        if current_target_model and current_loss_identifier and current_event_start_utc:
            final_end_time = last_event_end_utc if last_event_end_utc else shift_end_utc
            
            self.env[current_target_model].create(
            {
                'performance_id': self.id,
                'loss_id': current_loss_identifier,
                'start_time': current_event_start_utc,
                'end_time': final_end_time
            })
            
            if final_end_time == shift_end_utc:
                self._handle_shift_overflow(workcenter, shift_end_utc, current_target_model, current_loss_identifier)

    def _classify_telemetry_event(self, workcenter, machine_settings, tag_name, value):
        plc_value = int(float(value)) if value is not None else 0
        
        event = self._resolve_event(machine_settings, tag_name, plc_value)
        if not event:
            return None, None

        is_running_state = False
        if workcenter.runtime_event_id:
            running_signals = machine_settings.event_tag_ids.filtered(lambda tag_config: tag_config.event_id == workcenter.runtime_event_id)
            if running_signals:
                is_running_state = (running_signals[0].tag_name == tag_name and running_signals[0].plc_value == plc_value)
            else:
                is_running_state = (workcenter.runtime_event_id.default_event_tag_type == tag_name and workcenter.runtime_event_id.default_plc_value == plc_value)

        if is_running_state:
            return 'mes.performance.running', workcenter.runtime_event_id.id

        stop_tag = 'OEE.nStopRootReason'
        if hasattr(machine_settings, 'get_alarm_tag_name'):
            stop_tag = machine_settings.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')

        if tag_name == stop_tag or tag_name == 'OEE.nStopRootReason':
            return 'mes.performance.alarm', event.id

        return 'mes.performance.slowing', event.id

    def _convert_raw_time_to_utc(self, raw_time, local_timezone):
        if not raw_time:
            return None
        if isinstance(raw_time, str):
            event_datetime = fields.Datetime.to_datetime(raw_time.replace('T', ' ').replace('Z', '')[:19])
        else:
            event_datetime = raw_time.replace(tzinfo=None)
            
        return local_timezone.localize(event_datetime).astimezone(pytz.utc).replace(tzinfo=None)

    def _handle_shift_overflow(self, workcenter, shift_end_utc, target_model, loss_identifier):
        next_document = self._get_or_create_doc(workcenter, shift_end_utc + timedelta(seconds=1))
            
        if next_document and next_document.state == 'draft':
            existing_seed_record = self.env[target_model].search(
            [
                ('performance_id', '=', next_document.id),
                ('start_time', '=', shift_end_utc),
                ('end_time', '=', False)
            ])
            
            if not existing_seed_record:
                self.env[target_model].create(
                {
                    'performance_id': next_document.id,
                    'loss_id': loss_identifier,
                    'start_time': shift_end_utc
                })

    def _process_telemetry_counts(self, machine_settings, start_time_string, end_time_string):
        production_values = []
        rejection_values = []

        count_query = """
            SELECT 
                t.tag_name, 
                COALESCE(SUM(t.value), 0) AS sum_val, 
                COALESCE(MAX(t.value), 0) - COALESCE(
                    (SELECT value FROM telemetry_count start_t
                     WHERE start_t.machine_name = %s 
                       AND start_t.tag_name = t.tag_name 
                       AND start_t.time <= %s 
                     ORDER BY start_t.time DESC LIMIT 1), 
                    COALESCE(MIN(t.value), 0)
                ) AS cum_val
            FROM telemetry_count t
            WHERE t.machine_name = %s AND t.time > %s AND t.time <= %s
            GROUP BY t.tag_name
        """

        with self.env['mes.timescale.base']._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(count_query, (machine_settings.name, start_time_string, machine_settings.name, start_time_string, end_time_string))
                
                for row_data in cursor.fetchall():
                    tag_name, sum_value, cumulative_value = row_data
                    tag_configuration = machine_settings.count_tag_ids.filtered(lambda tag_setting: tag_setting.tag_name == tag_name)
                    
                    if tag_configuration:
                        quantity = cumulative_value if tag_configuration[0].is_cumulative else sum_value
                        
                        if quantity > 0:
                            count_dictionary_id = tag_configuration[0].count_id
                            value_dictionary = \
                            {
                                'performance_id': self.id,
                                'qty': float(quantity),
                                'reason_id': count_dictionary_id.id
                            }
                            
                            if count_dictionary_id == self.machine_id.production_count_id:
                                production_values.append(value_dictionary)
                            else:
                                rejection_values.append(value_dictionary)

        if production_values:
            self.env['mes.performance.production'].create(production_values)
            
        if rejection_values:
            self.env['mes.performance.rejection'].create(rejection_values)

    def _is_empty_shift(self):
        has_production_records = bool(self.production_ids)
        has_running_records = bool(self.running_ids)
        
        return not (has_production_records or has_running_records)
    
    def _get_or_create_doc(self, workcenter, timestamp_utc):
        machine_timezone = pytz.timezone(workcenter.company_id.tz or 'UTC')
        timestamp_local = pytz.utc.localize(timestamp_utc).astimezone(machine_timezone).replace(tzinfo=None)
        
        current_hour_decimal = timestamp_local.hour + timestamp_local.minute / 60.0 + timestamp_local.second / 3600.0
        available_shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)])
        valid_shifts = [shift for shift in available_shifts if not (shift.workcenter_ids and workcenter.id not in shift.workcenter_ids.ids)]
        
        current_shift = None
        
        for shift in valid_shifts:
            if (shift.start_hour < shift.end_hour and shift.start_hour <= current_hour_decimal < shift.end_hour) or \
               (shift.start_hour >= shift.end_hour and (current_hour_decimal >= shift.start_hour or current_hour_decimal < shift.end_hour)):
                current_shift = shift
                break

        if not current_shift:
            return None
            
        start_date = timestamp_local.date()
        
        if current_shift.start_hour > current_shift.end_hour and current_hour_decimal < current_shift.end_hour:
            start_date -= timedelta(days=1)
            
        document = self.search(
        [
            ('machine_id', '=', workcenter.id), 
            ('shift_id', '=', current_shift.id), 
            ('date', '=', start_date)
        ], limit=1)
        
        if not document:
            document = self.create(
            {
                'machine_id': workcenter.id, 
                'shift_id': current_shift.id, 
                'date': start_date
            })
            
        return document

    def _get_local_shift_times(self):
        self.ensure_one()
        s_time = datetime.combine(
            self.date,
            time(hour=int(self.shift_id.start_hour), minute=int((self.shift_id.start_hour % 1) * 60))
        )
        e_time = s_time + timedelta(hours=self.shift_id.duration)
        return s_time, e_time

    def _get_utc_time(self, local_naive_dt):
        self.ensure_one()
        tz_name = self.machine_id.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        local_dt = mac_tz.localize(local_naive_dt.replace(tzinfo=None))
        return local_dt.astimezone(pytz.utc).replace(tzinfo=None)

class MesPerformanceAlarm(models.Model):
    _name = 'mes.performance.alarm'
    _description = 'Machine Alarms'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Alarm Reason', required=True)
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceRunning(models.Model):
    _name = 'mes.performance.running'
    _description = 'Machine Runnings'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Activity Type', required=True) 
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceSlowing(models.Model):
    _name = 'mes.performance.slowing'
    _description = 'Machine Slowing Logs'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    loss_id = fields.Many2one('mes.event', string='Slowing Reason') 
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

class MesPerformanceRejection(models.Model):
    _name = 'mes.performance.rejection'
    _description = 'Machine Rejections'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Rejection Reason') 

class MesPerformanceProduction(models.Model):
    _name = 'mes.performance.production'
    _description = 'Machine Production'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Count Type')

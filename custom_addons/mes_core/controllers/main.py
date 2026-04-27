import json
import logging
import psycopg2.extras
import pytz
from datetime import datetime, timedelta
from odoo import http, exceptions, fields
from odoo.http import request

log = logging.getLogger(__name__)

class MesTelemetryApi(http.Controller):
    
    def _parse_batch(self, batch):
        if not batch: return []
        res = []
        now_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        for row in batch:
            if isinstance(row, dict):
                ts, arr_ts, mac, tag, val, evt_id = row.get('time'), row.get('arrived_time', now_utc), row.get('machine_name'), row.get('tag_name'), row.get('value'), row.get('evt_id')
            else:
                if len(row) == 6: ts, arr_ts, mac, tag, val, evt_id = row
                else: ts, arr_ts, mac, tag, val = row[:5]; evt_id = None
            res.append((ts, arr_ts or now_utc, mac, tag, val, evt_id))
        return res

    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_hist(self, events=None, counts=None, processes=None, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()
            
        evts, cnts, prcs = self._parse_batch(events), self._parse_batch(counts), self._parse_batch(processes)
        db = request.env['mes.timescale.base']
        
        try:
            with db._connection() as conn:
                with conn.cursor() as cur:
                    if evts: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_event (time, arrived_time, machine_name, tag_name, value, evt_id) VALUES %s ON CONFLICT DO NOTHING;", evts, page_size=10000)
                    if cnts: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_count (time, arrived_time, machine_name, tag_name, value, evt_id) VALUES %s ON CONFLICT DO NOTHING;", cnts, page_size=10000)
                    if prcs: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_process (time, arrived_time, machine_name, tag_name, value, evt_id) VALUES %s ON CONFLICT DO NOTHING;", prcs, page_size=10000)
            return {'status': 'success', 'events_rx': len(evts), 'counts_rx': len(cnts), 'processes_rx': len(prcs)}
        except Exception as e:
            log.error(f"TX Import Fault: {e}")
            return {'status': 'error', 'message': str(e)}

    @http.route('/mes/api/get_machine_config', type='json', auth='user', methods=['POST'])
    def get_mac_cfg(self, mac_name, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_read'):
            raise exceptions.AccessDenied()
            
        mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
        if not mac: return {'error': f"Machine {mac_name} not found"}
        
        tags = []
        for ct in mac.count_tag_ids:
            if ct.tag_name: tags.append({'tag_name': ct.tag_name, 'type': 'count', 'mode': ct.poll_type, 'interval_sec': (ct.poll_frequency or 1000) / 1000.0, 'is_cumul': bool(ct.is_cumulative)})
        for et in mac.event_tag_ids:
            if et.tag_name: tags.append({'tag_name': et.tag_name, 'type': 'event', 'mode': et.poll_type, 'interval_sec': (et.poll_frequency or 1000) / 1000.0, 'is_cumul': False})
        for pt in mac.process_tag_ids:
            if pt.tag_name: tags.append({'tag_name': pt.tag_name, 'type': 'process', 'mode': pt.poll_type, 'interval_sec': (pt.poll_frequency or 1000) / 1000.0, 'is_cumul': False})
        return {'tags': tags}

    @http.route('/mes/api/logger/status', type='json', auth='user', methods=['POST'], csrf=False)
    def set_log_sts(self, mac_name, evt_type, ts, err_msg=None, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()
            
        try:
            mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
            if not mac: return {'status': 'error', 'msg': 'mac_not_found'}
            dt_val = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            f_map = {'conn': 'log_conn_dt', 'cfg_req': 'log_cfg_req_dt', 'cfg_ok': 'log_cfg_ok_dt', 'bind_req': 'log_bind_req_dt', 'bind_ok': 'log_bind_ok_dt', 'plc_recv': 'log_plc_recv_dt', 'odoo_send': 'log_odoo_send_dt', 'err': 'log_err_dt'}
            vals = {}
            if evt_type in f_map: vals[f_map[evt_type]] = dt_val
            if evt_type == 'err' and err_msg: vals['log_err_msg'] = err_msg
            if vals: mac.write(vals)
            return {'status': 'ok'}
        except Exception as e:
            return {'status': 'error', 'msg': str(e)}

    @http.route('/mes/api/snapshot', type='json', auth='user', methods=['POST'], csrf=False)
    def get_production_snapshot(self, **kwargs):
        if not request.env.user.has_group('mes_core.group_mes_api_read'):
            raise exceptions.AccessDenied()

        machine_numbers = kwargs.get('machine_numbers', [])
        requested_fields = [f.lower() for f in kwargs.get('fields', [])]
        target_time_str = kwargs.get('target_time')

        if not machine_numbers or not requested_fields or not target_time_str:
            return {'error': 'Missing required parameters'}

        try:
            target_utc = fields.Datetime.to_datetime(target_time_str.replace('T', ' ').replace('Z', '')[:19])
        except ValueError:
            return {'error': 'Invalid datetime format. Use YYYY-MM-DD HH:MM:SS'}

        workcenters = request.env['mrp.workcenter'].sudo().search([('machine_number', 'in', machine_numbers)])
        if not workcenters: return {'error': 'Machines not found'}

        response_data = {'snapshot_time': target_utc.strftime('%Y-%m-%d %H:%M:%S UTC'), 'machines': []}

        for wc in workcenters:
            machine_set = wc.machine_settings_id
            if not machine_set: continue

            company_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            target_loc = pytz.utc.localize(target_utc).astimezone(company_tz).replace(tzinfo=None)
            curr_h = target_loc.hour + target_loc.minute / 60.0 + target_loc.second / 3600.0

            shifts = request.env['mes.shift'].sudo().search([('company_id', '=', wc.company_id.id)])
            val_shifts = shifts.filtered(lambda s: not s.workcenter_ids or wc.id in s.workcenter_ids.ids)

            active_shift = None
            shift_date = target_loc.date()

            for s in val_shifts:
                if s.start_hour < s.end_hour:
                    if s.start_hour <= curr_h < s.end_hour:
                        active_shift = s
                        break
                else:
                    if curr_h >= s.start_hour:
                        active_shift = s
                        break
                    elif curr_h < s.end_hour:
                        active_shift = s
                        shift_date -= timedelta(days=1)
                        break

            if not active_shift: continue

            shift_start_loc = datetime.combine(shift_date, datetime.min.time()) + timedelta(hours=active_shift.start_hour)
            shift_end_loc = shift_start_loc + timedelta(hours=active_shift.duration)
            shift_start_utc = company_tz.localize(shift_start_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            shift_end_utc = company_tz.localize(shift_end_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

            if target_utc > shift_end_utc: continue

            resolved_fields = []
            count_tags = []
            process_tags = []

            for field_name in requested_fields:
                matched = False
                for c_tag in machine_set.count_tag_ids:
                    if c_tag.count_id and c_tag.count_id.name.lower() == field_name:
                        resolved_fields.append({'requested_name': field_name, 'actual_name': c_tag.count_id.name, 'type': 'count', 'tag_name': c_tag.tag_name, 'is_cumulative': c_tag.is_cumulative})
                        count_tags.append(c_tag.tag_name)
                        matched = True
                        break
                if matched: continue

                for p_tag in machine_set.process_tag_ids:
                    if p_tag.process_id and p_tag.process_id.name.lower() == field_name:
                        resolved_fields.append({'requested_name': field_name, 'actual_name': p_tag.process_id.name, 'type': 'process', 'tag_name': p_tag.tag_name, 'is_bobbin': 'bobbin' in p_tag.process_id.name.lower()})
                        process_tags.append(p_tag.tag_name)
                        matched = True
                        break

            machine_result = {'machine_number': getattr(wc, 'machine_number', wc.id), 'machine_name': machine_set.name, 'shift': f"{shift_start_utc.strftime('%Y-%m-%d %H:%M:%S')} - {shift_end_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC", 'data': []}

            count_results = {}
            if count_tags:
                with request.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT tag_name, COALESCE(SUM(value), 0), COALESCE(MAX(value) - MIN(value), 0)
                            FROM telemetry_count WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s GROUP BY tag_name
                        """, (machine_set.name, count_tags, shift_start_utc, target_utc))
                        for row in cur.fetchall():
                            count_results[row[0]] = {'sum': row[1], 'cum': row[2]}

            process_results = {}
            if process_tags:
                with request.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            WITH first_vals AS (
                                SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time ASC) as rn
                                FROM telemetry_process WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
                            ),
                            last_vals AS (
                                SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time DESC) as rn
                                FROM telemetry_process WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
                            )
                            SELECT f.tag_name, f.value, l.value FROM first_vals f JOIN last_vals l ON f.tag_name = l.tag_name WHERE f.rn = 1 AND l.rn = 1
                        """, (machine_set.name, process_tags, shift_start_utc, target_utc, machine_set.name, process_tags, shift_start_utc, target_utc))
                        for row in cur.fetchall():
                            process_results[row[0]] = {'start': row[1], 'end': row[2]}

            for rf in resolved_fields:
                val = 0.0
                if rf['type'] == 'count':
                    dr = count_results.get(rf['tag_name'])
                    if dr: val = dr['cum'] if rf['is_cumulative'] else dr['sum']
                elif rf['type'] == 'process':
                    dr = process_results.get(rf['tag_name'])
                    if dr: val = abs(dr['end'] - dr['start']) if rf['is_bobbin'] else (dr['end'] - dr['start'])
                machine_result['data'].append({'field': rf['actual_name'], 'result': float(val)})

            response_data['machines'].append(machine_result)

        return response_data

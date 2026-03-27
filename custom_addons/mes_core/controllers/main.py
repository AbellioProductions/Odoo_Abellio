from odoo import http
from odoo.http import request
import psycopg2.extras
import logging
import pytz
from datetime import datetime

_logger = logging.getLogger(__name__)

class MesTelemetryImportAPI(http.Controller):
    
    def _process_telemetry_batch(self, batch):
        if not batch:
            return []
            
        processed = []
        now_utc = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        tz_cache = {}

        for item in batch:
            if isinstance(item, dict):
                t_val = item.get('time')
                a_val = item.get('arrived_time')
                m_name = item.get('machine_name')
                tag = item.get('tag_name')
                val = item.get('value')
            else:
                t_val, a_val, m_name, tag, val = item

            if m_name not in tz_cache:
                mac = request.env['mes.machine.settings'].sudo().search([('name', '=', m_name)], limit=1)
                wc = request.env['mrp.workcenter'].sudo().search([('machine_settings_id', '=', mac.id)], limit=1)
                tz_name = wc.company_id.tz if wc and wc.company_id.tz else 'UTC'
                tz_cache[m_name] = pytz.timezone(tz_name)

            tz_obj = tz_cache[m_name]

            if t_val:
                t_str = str(t_val)
                time_format = '%Y-%m-%d %H:%M:%S.%f' if '.' in t_str else '%Y-%m-%d %H:%M:%S'
                dt_naive = datetime.strptime(t_str, time_format)
                dt_local = tz_obj.localize(dt_naive)
                t_val = dt_local.astimezone(pytz.UTC).strftime(time_format)

            if not a_val:
                a_val = now_utc

            processed.append((t_val, a_val, m_name, tag, val))
            
        return processed

    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_historical_data(self, events=None, counts=None, processes=None, **kwargs):
        events = self._process_telemetry_batch(events or [])
        counts = self._process_telemetry_batch(counts or [])
        processes = self._process_telemetry_batch(processes or [])
        
        ts_manager = request.env['mes.timescale.base']
        
        try:
            with ts_manager._connection() as conn:
                with conn.cursor() as cur:
                    if events:
                        query_events = """
                            INSERT INTO telemetry_event (time, arrived_time, machine_name, tag_name, value) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, query_events, events, page_size=10000)
                        
                    if counts:
                        query_counts = """
                            INSERT INTO telemetry_count (time, arrived_time, machine_name, tag_name, value) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, query_counts, counts, page_size=10000)

                    if processes:
                        query_processes = """
                            INSERT INTO telemetry_process (time, arrived_time, machine_name, tag_name, value) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, query_processes, processes, page_size=10000)
            
            return {
                'status': 'success', 
                'events_received': len(events), 
                'counts_received': len(counts),
                'processes_received': len(processes)
            }
            
        except Exception as e:
            _logger.error(f"Historical Import Failed: {str(e)}")
            return {'status': 'error', 'message': str(e)}

    @http.route('/mes/api/get_machine_config', type='json', auth='user', methods=['POST'])
    def get_machine_config(self, mac_name, **kwargs):
        machine = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
        if not machine:
            return {'error': f"Machine {mac_name} not found"}

        tags_config = []
        
        for ct in machine.count_tag_ids:
            if ct.tag_name:
                tags_config.append({
                    'tag_name': ct.tag_name,
                    'type': 'count',
                    'mode': ct.poll_type,
                    'interval_sec': (ct.poll_frequency or 1000) / 1000.0
                })

        for et in machine.event_tag_ids:
            if et.tag_name:
                tags_config.append({
                    'tag_name': et.tag_name,
                    'type': 'event',
                    'mode': et.poll_type,
                    'interval_sec': (et.poll_frequency or 1000) / 1000.0
                })

        for pt in machine.process_tag_ids:
            if pt.tag_name:
                tags_config.append({
                    'tag_name': pt.tag_name,
                    'type': 'process',
                    'mode': pt.poll_type,
                    'interval_sec': (pt.poll_frequency or 1000) / 1000.0
                })

        return {'tags': tags_config}

    @http.route('/mes/api/logger/status', type='json', auth='user', methods=['POST'], csrf=False)
    def update_logger_status(self, mac_name, evt_type, ts, err_msg=None, **kwargs):
        try:
            mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
            if not mac:
                return {'status': 'error', 'msg': 'mac_not_found'}

            dt_val = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            vals = {}
            
            f_map = {
                'conn': 'log_conn_dt',
                'cfg_req': 'log_cfg_req_dt',
                'cfg_ok': 'log_cfg_ok_dt',
                'bind_req': 'log_bind_req_dt',
                'bind_ok': 'log_bind_ok_dt',
                'plc_recv': 'log_plc_recv_dt',
                'odoo_send': 'log_odoo_send_dt',
                'err': 'log_err_dt'
            }

            if evt_type in f_map:
                vals[f_map[evt_type]] = dt_val
            
            if evt_type == 'err' and err_msg:
                vals['log_err_msg'] = err_msg

            if vals:
                mac.write(vals)

            return {'status': 'ok'}
        except Exception as e:
            return {'status': 'error', 'msg': str(e)}
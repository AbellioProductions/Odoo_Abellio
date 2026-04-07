from datetime import datetime
import logging
import psycopg2.extras
from odoo import http
from odoo.http import request

log = logging.getLogger(__name__)

class MesTelemetryApi(http.Controller):
    
    def _parse_batch(self, batch):
        if not batch:
            return []
            
        res = []
        now_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')

        for row in batch:
            if isinstance(row, dict):
                ts = row.get('time')
                arr_ts = row.get('arrived_time', now_utc)
                mac = row.get('machine_name')
                tag = row.get('tag_name')
                val = row.get('value')
                evt_id = row.get('evt_id')
            else:
                if len(row) == 6:
                    ts, arr_ts, mac, tag, val, evt_id = row
                else:
                    ts, arr_ts, mac, tag, val = row[:5]
                    evt_id = None

            arr_ts = arr_ts or now_utc
            res.append((ts, arr_ts, mac, tag, val, evt_id))
            
        return res

    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_hist(self, events=None, counts=None, processes=None, **kw):
        evts = self._parse_batch(events)
        cnts = self._parse_batch(counts)
        prcs = self._parse_batch(processes)
        
        db = request.env['mes.timescale.base']
        
        try:
            with db._connection() as conn:
                with conn.cursor() as cur:
                    if evts:
                        q_evt = """
                            INSERT INTO telemetry_event (time, arrived_time, machine_name, tag_name, value, evt_id) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name, evt_id) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, q_evt, evts, page_size=10000)

                    if cnts:
                        q_cnt = """
                            INSERT INTO telemetry_count (time, arrived_time, machine_name, tag_name, value, evt_id) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name, evt_id) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, q_cnt, cnts, page_size=10000)

                    if prcs:
                        q_prc = """
                            INSERT INTO telemetry_process (time, arrived_time, machine_name, tag_name, value, evt_id) 
                            VALUES %s 
                            ON CONFLICT (time, machine_name, tag_name, evt_id) DO NOTHING;
                        """
                        psycopg2.extras.execute_values(cur, q_prc, prcs, page_size=10000)
            
            return {
                'status': 'success', 
                'events_rx': len(evts), 
                'counts_rx': len(cnts),
                'processes_rx': len(prcs)
            }
            
        except Exception as e:
            log.error(f"TX Import Fault: {e}")
            return {'status': 'error', 'message': str(e)}

    @http.route('/mes/api/get_machine_config', type='json', auth='user', methods=['POST'])
    def get_mac_cfg(self, mac_name, **kw):
        mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
        if not mac:
            return {'error': f"Machine {mac_name} not found"}

        tags = []
        
        for ct in mac.count_tag_ids:
            if ct.tag_name:
                tags.append({
                    'tag_name': ct.tag_name,
                    'type': 'count',
                    'mode': ct.poll_type,
                    'interval_sec': (ct.poll_frequency or 1000) / 1000.0,
                    'is_cumul': bool(ct.is_cumulative)
                })

        for et in mac.event_tag_ids:
            if et.tag_name:
                tags.append({
                    'tag_name': et.tag_name,
                    'type': 'event',
                    'mode': et.poll_type,
                    'interval_sec': (et.poll_frequency or 1000) / 1000.0,
                    'is_cumul': False
                })

        for pt in mac.process_tag_ids:
            if pt.tag_name:
                tags.append({
                    'tag_name': pt.tag_name,
                    'type': 'process',
                    'mode': pt.poll_type,
                    'interval_sec': (pt.poll_frequency or 1000) / 1000.0,
                    'is_cumul': False
                })

        return {'tags': tags}

    @http.route('/mes/api/logger/status', type='json', auth='user', methods=['POST'], csrf=False)
    def set_log_sts(self, mac_name, evt_type, ts, err_msg=None, **kw):
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
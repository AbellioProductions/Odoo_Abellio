from odoo import http
from odoo.http import request
import psycopg2.extras
import logging

_logger = logging.getLogger(__name__)

class MesTelemetryImportAPI(http.Controller):
    
    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_historical_data(self, events=None, counts=None, processes=None, **kwargs):
        events = events or []
        counts = counts or []
        processes = processes or []
        
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
from odoo import http
from odoo.http import request
import psycopg2.extras
import logging

_logger = logging.getLogger(__name__)

class MesTelemetryImportAPI(http.Controller):
    
    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_historical_data(self, **kwargs):
        data = request.params
        events = data.get('events', [])
        counts = data.get('counts', [])
        processes = data.get('processes', [])
        
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
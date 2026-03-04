import base64
import csv
import io
import logging
from odoo import models, fields, api, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class MesDictionaryImportWizard(models.TransientModel):
    _name = 'mes.dictionary.import.wizard'
    _description = 'Import Dictionaries and Signals from CSV'

    import_type = fields.Selection([
        ('event', 'Alarms / Events'),
        ('count', 'Rejections / Counts')
    ], string='Import Type', required=True, default='event')
    
    import_mode = fields.Selection([
        ('global', 'Global Dictionary'),
        ('machine', 'Machine Signals')
    ], string='Import Mode', required=True, default='global')

    machine_id = fields.Many2one('mes.machine.settings', string='Target Machine')
    
    file = fields.Binary(string='CSV File', required=True)
    filename = fields.Char(string='Filename')

    def do_import(self):
        self.ensure_one()
        if not self.file:
            raise UserError(_("Please upload a file."))
            
        rows = self._read_csv()
        mapped_data = [self._map_row(r) for r in rows]
        
        dict_records = self._sync_global_dictionary(mapped_data)
        
        if self.import_mode == 'machine' and self.machine_id:
            self._sync_machine_signals(mapped_data, dict_records)
            
        return self._build_success_action(len(mapped_data))

    def _read_csv(self):
        try:
            csv_data = base64.b64decode(self.file)
            data_file = io.StringIO(csv_data.decode("utf-8-sig"))
            return list(csv.DictReader(data_file, delimiter=';'))
        except Exception as e:
            raise UserError(_("Invalid file format: %s") % e)

    def _map_row(self, row):
        if self.import_type == 'event':
            return {
                'code': row.get('AlarmCode', '').strip(),
                'name': row.get('Description', '').strip(),
                'tag': row.get('DefaultOPCTag', '').strip(),
                'plc_val': int(row.get('DefaultPLCValue', 0)) if row.get('DefaultPLCValue', '').isdigit() else 0,
                'parent_name': row.get('ParentName', '').strip()
            }
        else:
            return {
                'code': row.get('EventCode', '').strip(),
                'name': row.get('Description', '').strip(),
                'tag': row.get('Tag', '').strip(),
                'wheel': int(row.get('Wheel', 0)) if row.get('Wheel', '').isdigit() else 0,
                'module': int(row.get('Module', 0)) if row.get('Module', '').isdigit() else 0,
                'parent_name': row.get('ParentName', '').strip()
            }

    def _sync_global_dictionary(self, mapped_data):
        model_name = 'mes.event' if self.import_type == 'event' else 'mes.counts'
        data_list = []
        
        for item in mapped_data:
            vals = {}
            if self.import_type == 'event':
                vals['default_event_tag_type'] = item['tag']
                vals['default_plc_value'] = item['plc_val']
            else:
                vals['default_OPCTag'] = item['tag']
                vals['wheel'] = item['wheel']
                vals['module'] = item['module']
                
            data_list.append({
                'code': item['code'],
                'name': item['name'],
                'parent_name': item['parent_name'],
                'vals': vals
            })
            
        self.env[model_name].sync_batch(data_list)
        
        codes = [item['code'] for item in mapped_data if item['code']]
        names = [item['name'] for item in mapped_data if not item['code']]
        
        domain = ['|', ('code', 'in', codes), ('name', 'in', names)]
        records = self.env[model_name].search(domain)
        
        return {r.code or r.name: r for r in records}

    def _sync_machine_signals(self, mapped_data, dict_records):
        if self.import_type == 'event':
            signal_model = self.env['mes.signal.event']
            existing = signal_model.search([('machine_id', '=', self.machine_id.id)])
            existing_map = {(r.tag_name, r.plc_value): r for r in existing}
            
            create_vals = []
            for item in mapped_data:
                dict_rec = dict_records.get(item['code'] or item['name'])
                if not dict_rec:
                    continue
                
                key = (item['tag'], item['plc_val'])
                if key not in existing_map:
                    create_vals.append({
                        'machine_id': self.machine_id.id,
                        'event_id': dict_rec.id,
                        'tag_name': item['tag'],
                        'plc_value': item['plc_val']
                    })
            if create_vals:
                signal_model.create(create_vals)
                
        else:
            signal_model = self.env['mes.signal.count']
            existing = signal_model.search([('machine_id', '=', self.machine_id.id)])
            existing_map = {r.tag_name: r for r in existing}
            
            create_vals = []
            for item in mapped_data:
                dict_rec = dict_records.get(item['code'] or item['name'])
                if not dict_rec:
                    continue
                    
                if item['tag'] not in existing_map:
                    create_vals.append({
                        'machine_id': self.machine_id.id,
                        'count_id': dict_rec.id,
                        'tag_name': item['tag'],
                        'is_cumulative': dict_rec.is_cumulative 
                    })
            if create_vals:
                signal_model.create(create_vals)

    def _build_success_action(self, records_count):
        target = f"{self.machine_id.name}" if self.import_mode == 'machine' else "Global Dictionary"
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Import Success',
                'message': f'Successfully imported {records_count} records into {target}.',
                'type': 'success',
                'sticky': False,
                'next': {'type': 'ir.actions.act_window_close'},
            }
        }
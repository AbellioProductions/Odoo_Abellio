from odoo import models, fields, api
from datetime import datetime, timedelta
import pytz

class MesPlannedDowntime(models.Model):
    _name = 'mes.planned.downtime'
    _description = 'Planned Downtime Rule'

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)
    
    rule_type = fields.Selection([
        ('one_time', 'One Time'),
        ('daily', 'Weekdays (Mon-Fri)'),
        ('weekend', 'Weekends')
    ], default='one_time', required=True)

    machine_ids = fields.Many2many('mrp.workcenter', string='Affected Machines')
    
    date_start = fields.Datetime(required=True)
    date_end = fields.Datetime(required=True)

    @api.model
    def generate_flat_schedule_for_week(self, days_ahead=14):
        if isinstance(days_ahead, (list, tuple)) or not isinstance(days_ahead, int):
            days_ahead = 14
            
        flat_model = self.env['mes.flat.downtime']
        now_utc = datetime.utcnow()
        
        user_tz = self.env.context.get('tz') or self.env.user.tz or 'UTC'
        tz = pytz.timezone(user_tz)
        
        local_now = pytz.utc.localize(now_utc).astimezone(tz)
        start_date = local_now.date()
        end_date = start_date + timedelta(days=days_ahead)

        vals_list = []
        rules = self if self else self.search([('active', '=', True)])

        for rule in rules:
            if not rule.date_start or not rule.date_end:
                continue

            if rule.rule_type == 'one_time':
                flat_model.search([('rule_id', '=', rule.id)]).unlink()
            else:
                flat_model.search([
                    ('rule_id', '=', rule.id), 
                    ('start_time', '>=', now_utc)
                ]).unlink()

            loc_ref_start = pytz.utc.localize(rule.date_start).astimezone(tz)
            loc_ref_end = pytz.utc.localize(rule.date_end).astimezone(tz)
            
            ref_start_time = loc_ref_start.time()
            ref_end_time = loc_ref_end.time()
            
            ref_duration_days = (loc_ref_end.date() - loc_ref_start.date()).days

            for machine in rule.machine_ids:
                if rule.rule_type == 'one_time':
                    vals_list.append({
                        'machine_id': machine.id,
                        'rule_id': rule.id,
                        'start_time': rule.date_start,
                        'end_time': rule.date_end,
                    })
                
                elif rule.rule_type == 'daily':
                    for i in range((end_date - start_date).days + 1):
                        target_date = start_date + timedelta(days=i)
                        
                        if target_date < loc_ref_start.date():
                            continue

                        if target_date.weekday() < 5: 
                            target_loc_start = tz.localize(datetime.combine(target_date, ref_start_time))
                            target_loc_end = tz.localize(datetime.combine(target_date + timedelta(days=ref_duration_days), ref_end_time))
                            
                            utc_start_save = target_loc_start.astimezone(pytz.utc).replace(tzinfo=None)
                            utc_end_save = target_loc_end.astimezone(pytz.utc).replace(tzinfo=None)
                            
                            if utc_end_save > now_utc:
                                vals_list.append({
                                    'machine_id': machine.id,
                                    'rule_id': rule.id,
                                    'start_time': utc_start_save,
                                    'end_time': utc_end_save,
                                })
                                
                elif rule.rule_type == 'weekend':
                    target_date = loc_ref_start.date()
                    
                    while target_date < start_date:
                        target_date += timedelta(weeks=1)
                        
                    while target_date <= end_date:
                        target_loc_start = tz.localize(datetime.combine(target_date, ref_start_time))
                        target_loc_end = tz.localize(datetime.combine(target_date + timedelta(days=ref_duration_days), ref_end_time))
                        
                        utc_start_save = target_loc_start.astimezone(pytz.utc).replace(tzinfo=None)
                        utc_end_save = target_loc_end.astimezone(pytz.utc).replace(tzinfo=None)
                        
                        if utc_end_save > now_utc:
                            vals_list.append({
                                'machine_id': machine.id,
                                'rule_id': rule.id,
                                'start_time': utc_start_save,
                                'end_time': utc_end_save,
                            })
                            
                        target_date += timedelta(weeks=1)
        
        if vals_list:
            flat_model.create(vals_list)

class MesFlatDowntime(models.Model):
    _name = 'mes.flat.downtime'
    _description = 'Generated Downtime Schedule'
    _order = 'start_time asc'

    machine_id = fields.Many2one('mrp.workcenter', required=True, ondelete='cascade', string='Workcenter')
    rule_id = fields.Many2one('mes.planned.downtime', required=True, ondelete='cascade', string='Downtime Rule')
    start_time = fields.Datetime(required=True)
    end_time = fields.Datetime(required=True)

    duration = fields.Float(compute='_compute_duration', string='Duration (Hours)')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 3600.0
            else:
                rec.duration = 0.0
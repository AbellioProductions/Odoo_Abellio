from odoo import models, fields, api
from datetime import datetime, timedelta, time
import pytz

class MesFlatDowntime(models.Model):
    _name = 'mes.flat.downtime'
    _description = 'Raw Downtime Table (Flat Schedule)'
    _order = 'start_time desc'

    machine_id = fields.Many2one('mrp.workcenter', string="Machine", required=True, ondelete='cascade')
    rule_id = fields.Many2one('mes.planned.downtime', string="Rule", ondelete='cascade')
    start_time = fields.Datetime("Start Time (UTC)", required=True)
    end_time = fields.Datetime("End Time (UTC)", required=True)
    duration = fields.Float("Duration (hours)", compute="_compute_duration")

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 3600.0
            else:
                rec.duration = 0.0

class MesPlannedDowntime(models.Model):
    _name = 'mes.planned.downtime'
    _description = 'Planned Downtime Rules'

    name = fields.Char("Rule Name", required=True)
    active = fields.Boolean("Active", default=True)
    machine_ids = fields.Many2many('mrp.workcenter', string="Machines", required=True)
    
    rule_type = fields.Selection([
        ('daily', 'Weekdays (Shift Change)'),
        ('weekend', 'Weekends'),
        ('one_time', 'One-time Downtime (PM)')
    ], string="Schedule Type", required=True, default='daily')

    daily_start = fields.Float("Start (HH:MM)", help="For example: 5:45 = 5.75")
    daily_end = fields.Float("End (HH:MM)")

    weekend_start_day = fields.Selection([
        ('0', 'Monday'), ('1', 'Tuesday'), ('2', 'Wednesday'), 
        ('3', 'Thursday'), ('4', 'Friday'), ('5', 'Saturday'), ('6', 'Sunday')
    ], string="Start Day", default='5')
    weekend_start_time = fields.Float("Start Time")
    weekend_end_day = fields.Selection([
        ('0', 'Monday'), ('1', 'Tuesday'), ('2', 'Wednesday'), 
        ('3', 'Thursday'), ('4', 'Friday'), ('5', 'Saturday'), ('6', 'Sunday')
    ], string="End Day", default='0')
    weekend_end_time = fields.Float("End Time")

    date_start = fields.Datetime("Downtime start (UTC)")
    date_end = fields.Datetime("Downtime end (UTC)")

    def _float_to_time(self, f):
        hours = int(f)
        minutes = int(round((f - hours) * 60))
        if minutes == 60:
            hours += 1
            minutes = 0
        return time(min(hours, 23), min(minutes, 59))

    @api.model
    def generate_flat_schedule_for_week(self, days_ahead=14):
        if isinstance(days_ahead, (list, tuple)) or not isinstance(days_ahead, int):
            days_ahead = 14
            
        flat_model = self.env['mes.flat.downtime']
        now_utc = datetime.utcnow()
        
        tz = pytz.timezone(self.env.user.tz or 'Europe/Dublin')
        local_now = datetime.now(tz)
        start_date = local_now.date()
        end_date = start_date + timedelta(days=days_ahead)

        vals_list = []
        rules = self if self else self.search([('active', '=', True)])

        for rule in rules:
            if rule.rule_type == 'one_time':
                flat_model.search([('rule_id', '=', rule.id)]).unlink()
            else:
                flat_model.search([
                    ('rule_id', '=', rule.id), 
                    ('start_time', '>=', now_utc)
                ]).unlink()

            for machine in rule.machine_ids:
                if rule.rule_type == 'one_time':
                    if rule.date_start and rule.date_end:
                        vals_list.append({
                            'machine_id': machine.id,
                            'rule_id': rule.id,
                            'start_time': rule.date_start,
                            'end_time': rule.date_end,
                        })
                
                elif rule.rule_type == 'daily':
                    for i in range((end_date - start_date).days + 1):
                        current_date = start_date + timedelta(days=i)
                        if current_date.weekday() < 5: 
                            s_time = rule._float_to_time(rule.daily_start)
                            e_time = rule._float_to_time(rule.daily_end)
                            
                            loc_start = tz.localize(datetime.combine(current_date, s_time))
                            loc_end = tz.localize(datetime.combine(current_date, e_time))
                            
                            if loc_start >= loc_end:
                                loc_end += timedelta(days=1)
                            
                            if loc_end > pytz.utc.localize(now_utc):
                                vals_list.append({
                                    'machine_id': machine.id,
                                    'rule_id': rule.id,
                                    'start_time': loc_start.astimezone(pytz.utc).replace(tzinfo=None),
                                    'end_time': loc_end.astimezone(pytz.utc).replace(tzinfo=None),
                                })
                                
                elif rule.rule_type == 'weekend':
                    for i in range((end_date - start_date).days + 1):
                        current_date = start_date + timedelta(days=i)
                        if str(current_date.weekday()) == rule.weekend_start_day:
                            s_time = rule._float_to_time(rule.weekend_start_time)
                            loc_start = tz.localize(datetime.combine(current_date, s_time))
                            
                            days_to_add = (int(rule.weekend_end_day) - int(rule.weekend_start_day)) % 7
                            if days_to_add == 0 and rule.weekend_end_time > rule.weekend_start_time:
                                days_to_add = 0
                            elif days_to_add == 0:
                                days_to_add = 7
                                
                            end_date_calc = current_date + timedelta(days=days_to_add)
                            e_time = rule._float_to_time(rule.weekend_end_time)
                            loc_end = tz.localize(datetime.combine(end_date_calc, e_time))
                            
                            if loc_end > pytz.utc.localize(now_utc):
                                vals_list.append({
                                    'machine_id': machine.id,
                                    'rule_id': rule.id,
                                    'start_time': loc_start.astimezone(pytz.utc).replace(tzinfo=None),
                                    'end_time': loc_end.astimezone(pytz.utc).replace(tzinfo=None),
                                })
        
        if vals_list:
            flat_model.create(vals_list)
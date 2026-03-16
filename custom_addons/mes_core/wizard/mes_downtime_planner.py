from odoo import models, fields, api
from datetime import datetime, time, timedelta
import pytz

class MesDowntimePlannerWizard(models.TransientModel):
    _name = 'mes.downtime.planner.wizard'

    selected_date = fields.Date(default=fields.Date.context_today, required=True)
    
    existing_downtime_ids = fields.Many2many(
        'mes.flat.downtime', 
        compute='_compute_existing_downtimes'
    )

    creation_mode = fields.Selection([
        ('shift', 'Select Shift'),
        ('custom', 'Custom Time')
    ], default='shift', required=True)

    shift_id = fields.Many2one('mes.shift')
    
    custom_start = fields.Float(default=0.0)
    custom_end = fields.Float(default=23.98)

    @api.depends('selected_date')
    def _compute_existing_downtimes(self):
        for wiz in self:
            if not wiz.selected_date:
                wiz.existing_downtime_ids = False
                continue
                
            user_tz = self.env.context.get('tz') or self.env.user.tz or 'UTC'
            tz = pytz.timezone(user_tz)
            
            loc_start = tz.localize(datetime.combine(wiz.selected_date, time(0, 0, 0)))
            loc_end = tz.localize(datetime.combine(wiz.selected_date, time(23, 59, 59)))
            
            utc_start = loc_start.astimezone(pytz.utc).replace(tzinfo=None)
            utc_end = loc_end.astimezone(pytz.utc).replace(tzinfo=None)

            downtimes = self.env['mes.flat.downtime'].search([
                ('start_time', '<=', utc_end),
                ('end_time', '>=', utc_start)
            ])
            wiz.existing_downtime_ids = downtimes.ids

    def _float_to_time(self, f):
        hours = int(f)
        minutes = int(round((f - hours) * 60))
        if minutes >= 60:
            hours += 1
            minutes = 0
        return time(min(hours, 23), min(minutes, 59))

    def action_continue_to_rule(self):
        self.ensure_one()
        user_tz = self.env.context.get('tz') or self.env.user.tz or 'UTC'
        tz = pytz.timezone(user_tz)
        
        final_start = False
        final_end = False

        if self.creation_mode == 'shift' and self.shift_id:
            h_s = int(self.shift_id.start_hour)
            m_s = int((self.shift_id.start_hour - h_s) * 60)
            loc_start = tz.localize(datetime.combine(self.selected_date, time(h_s, m_s)))
            
            h_e = int(self.shift_id.end_hour)
            m_e = int((self.shift_id.end_hour - h_e) * 60)
            loc_end = tz.localize(datetime.combine(self.selected_date, time(h_e, m_e)))
            
            if loc_end <= loc_start:
                loc_end += timedelta(days=1)
                
            final_start = loc_start.astimezone(pytz.utc).replace(tzinfo=None)
            final_end = loc_end.astimezone(pytz.utc).replace(tzinfo=None)
            
        elif self.creation_mode == 'custom':
            s_time = self._float_to_time(self.custom_start)
            e_time = self._float_to_time(self.custom_end)
            
            loc_start = tz.localize(datetime.combine(self.selected_date, s_time))
            loc_end = tz.localize(datetime.combine(self.selected_date, e_time))
            
            if loc_end <= loc_start:
                loc_end += timedelta(days=1)
                
            final_start = loc_start.astimezone(pytz.utc).replace(tzinfo=None)
            final_end = loc_end.astimezone(pytz.utc).replace(tzinfo=None)

        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mes.planned.downtime',
            'view_mode': 'form',
            'target': 'current',
            'context': {
                'default_rule_type': 'one_time',
                'default_date_start': final_start,
                'default_date_end': final_end,
            }
        }
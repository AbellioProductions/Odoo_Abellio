from odoo import models, fields, api
from datetime import datetime, time, timedelta
import pytz

class MesReportBaseWizard(models.TransientModel):
    _name = 'mes.report.base.wizard'
    _description = 'Base SKD Report Engine'

    start_datetime = fields.Datetime(string="Start Date & Time", required=True)
    end_datetime = fields.Datetime(string="End Date & Time", required=True)
    
    machine_filter_type = fields.Selection([
        ('in', 'In List'),
        ('not_in', 'Not in List')
    ], string="Machine Condition", default='in', required=True)
    machine_ids = fields.Many2many('mes.machine.settings', string="Machines")
    
    time_scale = fields.Selection([
        ('shift', 'By Shift (Date + Shift)'),
        ('day', 'By Logical Day'),
        ('month', 'By Month'),
        ('period', 'Entire Selected Period')
    ], string="Time Aggregation", default='month', required=True)

    record_limit = fields.Integer(default=0)    

    limit_by = fields.Selection(
            selection='_get_limit_by_options',
            default='total_time',
            required=True
        )

    row_by_machine = fields.Boolean("Machine", default=True)
    row_by_period = fields.Boolean("Period", default=False)
    col_by_machine = fields.Boolean("Machine", default=False)
    col_by_period = fields.Boolean("Period", default=True)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        tz_name = self.env.company.tz or self.env.user.tz or 'UTC'
        tz_obj = pytz.timezone(tz_name)
        
        now_loc = datetime.now(pytz.UTC).astimezone(tz_obj)
        today_loc = now_loc.date()

        shifts = self.env['mes.shift'].search([('company_id', '=', self.env.company.id)], order='start_hour asc')
        if shifts:
            m_shift = shifts[0]
            n_shift = shifts[-1]

            s_hr = int(m_shift.start_hour)
            s_min = int(round((m_shift.start_hour - s_hr) * 60))
            s_dt_loc = tz_obj.localize(datetime.combine(today_loc, time(s_hr, s_min)))
            res['start_datetime'] = s_dt_loc.astimezone(pytz.UTC).replace(tzinfo=None)

            e_hr = int(n_shift.end_hour)
            e_min = int(round((n_shift.end_hour - e_hr) * 60))
            e_dt_loc = tz_obj.localize(datetime.combine(today_loc, time(e_hr, e_min)))

            if n_shift.end_hour <= m_shift.start_hour:
                e_dt_loc += timedelta(days=1)
                
            res['end_datetime'] = e_dt_loc.astimezone(pytz.UTC).replace(tzinfo=None)
            
        return res

    @api.model
    def _get_limit_by_options(self):
        return [
            ('total_time', 'Total Time')
        ]
    
    def _get_filtered_machines(self):
        domain = []
        if self.machine_ids:
            operator = 'in' if self.machine_filter_type == 'in' else 'not in'
            domain.append(('id', operator, self.machine_ids.ids))
        return self.env['mes.machine.settings'].search(domain)

    def _is_item_allowed(self, item_id, filter_ids, filter_type):
        if not filter_ids:
            return True  
        if filter_type == 'in':
            return item_id in filter_ids
        else:
            return item_id not in filter_ids

    def _get_logical_periods(self, start_dt, end_dt, shifts, tz_name):
        tz_obj = pytz.timezone(tz_name)
        s_utc = pytz.UTC.localize(start_dt)
        e_utc = pytz.UTC.localize(end_dt)
        
        s_loc = s_utc.astimezone(tz_obj)
        e_loc = e_utc.astimezone(tz_obj)
        
        cur_date = s_loc.date()
        end_date = e_loc.date()
        
        periods = {}
        
        while cur_date <= end_date:
            for shift in shifts:
                s_hr = int(shift.start_hour)
                s_min = int(round((shift.start_hour - s_hr) * 60))
                shift_s = tz_obj.localize(datetime.combine(cur_date, time(s_hr, s_min)))
                
                e_hr = int(shift.end_hour)
                e_min = int(round((shift.end_hour - e_hr) * 60))
                shift_e = tz_obj.localize(datetime.combine(cur_date, time(e_hr, e_min)))
                
                if shift.end_hour <= shift.start_hour:
                    shift_e += timedelta(days=1)
                    
                if shift_s < e_utc and shift_e > s_utc:
                    act_s = max(shift_s, s_utc).astimezone(pytz.UTC).replace(tzinfo=None)
                    act_e = min(shift_e, e_utc).astimezone(pytz.UTC).replace(tzinfo=None)
                    
                    if self.time_scale == 'shift':
                        p_name = f"{shift_s.strftime('%Y-%m-%d %H:%M')} [{shift.name}]"
                    elif self.time_scale == 'day':
                        p_name = shift_s.strftime('%Y-%m-%d')
                    elif self.time_scale == 'month':
                        p_name = shift_s.strftime('%Y-%m')
                    else:
                        p_name = "All Period"
                        
                    if p_name not in periods:
                        periods[p_name] = []
                    periods[p_name].append((act_s, act_e))
                    
            cur_date += timedelta(days=1)
            
        return periods

    def _merge_intervals(self, intervals):
        if not intervals: return []
        intervals.sort(key=lambda x: x[0])
        merged = [intervals[0]]
        for current in intervals[1:]:
            last = merged[-1]
            if current[0] <= last[1]: merged[-1] = (last[0], max(last[1], current[1]))
            else: merged.append(current)
        return merged

    def _build_skd_context(self, measures):
        return {
            'pivot_row_groupby': ['row_group_label'],
            'pivot_column_groupby': ['col_group_label'],
            'graph_groupbys': ['period_name', 'row_group_label'],
            'graph_measure': measures[0] if measures else 'frequency',
            'pivot_measures': measures,
        }

    def action_generate_report(self):
        pass

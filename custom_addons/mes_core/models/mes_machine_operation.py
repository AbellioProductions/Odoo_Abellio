from odoo import models, fields, api
from odoo.exceptions import ValidationError

class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    auto_assign_idle_min = fields.Float(default=5.0)

class MesMachineOperation(models.Model):
    _name = 'mes.machine.operation'
    _description = 'Machine Operation Log'
    _order = 'start_dt desc'

    workcenter_id = fields.Many2one('mrp.workcenter', required=True)
    report_id = fields.Many2one('mes.production.report')
    job_number = fields.Char()
    
    start_dt = fields.Datetime(required=True)
    end_dt = fields.Datetime()
    duration_min = fields.Float(compute='_compute_duration', store=True)
    
    op_type = fields.Selection([
        ('job', 'Job'),
        ('idle', 'Idle'),
        ('waste', 'Waste')
    ], default='idle', required=True)

    @api.depends('start_dt', 'end_dt')
    def _compute_duration(self):
        for rec in self:
            if rec.start_dt and rec.end_dt:
                delta = rec.end_dt - rec.start_dt
                rec.duration_min = delta.total_seconds() / 60.0
            else:
                rec.duration_min = 0.0

    @api.constrains('start_dt', 'end_dt')
    def _check_dates(self):
        for rec in self:
            if rec.end_dt and rec.start_dt > rec.end_dt:
                raise ValidationError("End date cannot be earlier than start date.")

    def action_mark_waste(self):
        self.write({'op_type': 'waste'})

    def action_assign_job(self):
        return {
            'name': 'Assign Job',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.operation.assign.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {
                'default_operation_id': self.id, 
                'default_workcenter_id': self.workcenter_id.id
            }
        }

    def action_split_interval(self):
        return {
            'name': 'Split Interval',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.operation.split.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_operation_id': self.id}
        }

    @api.model
    def handle_verify_start(self, workcenter_id, job_number, start_dt):
        wc = self.env['mrp.workcenter'].browse(workcenter_id)
        last_op = self.search([('workcenter_id', '=', workcenter_id)], order='start_dt desc', limit=1)
        
        if last_op and not last_op.end_dt:
            last_op.end_dt = start_dt
            if last_op.op_type == 'idle':
                if last_op.duration_min <= wc.auto_assign_idle_min:
                    last_op.write({
                        'op_type': 'job',
                        'job_number': job_number
                    })

        self.create({
            'workcenter_id': workcenter_id,
            'job_number': job_number,
            'start_dt': start_dt,
            'op_type': 'job'
        })

    @api.model
    def handle_verify_end(self, workcenter_id, end_dt):
        last_op = self.search([
            ('workcenter_id', '=', workcenter_id),
            ('op_type', '=', 'job'),
            ('end_dt', '=', False)
        ], order='start_dt desc', limit=1)
        
        if last_op:
            last_op.end_dt = end_dt
            
        self.create({
            'workcenter_id': workcenter_id,
            'start_dt': end_dt,
            'op_type': 'idle'
        })
from odoo import models, fields, api
from odoo.exceptions import ValidationError

class MesOperationAssignWizard(models.TransientModel):
    _name = 'mes.operation.assign.wizard'
    _description = 'Operation Assign Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    workcenter_id = fields.Many2one('mrp.workcenter')
    machine_id = fields.Many2one(related='workcenter_id.machine_settings_id')
    
    report_id = fields.Many2one(
        'mes.production.report', 
        domain="[('machine_id', '=', machine_id)]",
        required=True
    )

    def action_confirm(self):
        self.operation_id.write({
            'op_type': 'job',
            'report_id': self.report_id.id,
            'job_number': self.report_id.name
        })


class MesOperationSplitWizard(models.TransientModel):
    _name = 'mes.operation.split.wizard'
    _description = 'Operation Split Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    split_dt = fields.Datetime(required=True)

    @api.constrains('split_dt')
    def _check_split_dt(self):
        for rec in self:
            op = rec.operation_id
            if rec.split_dt <= op.start_dt or (op.end_dt and rec.split_dt >= op.end_dt):
                raise ValidationError("Split time must be within the operation interval.")

    def action_confirm(self):
        op = self.operation_id
        op.copy({
            'start_dt': self.split_dt,
            'end_dt': op.end_dt,
            'op_type': op.op_type,
            'report_id': op.report_id.id,
            'job_number': op.job_number
        })
        op.end_dt = self.split_dt
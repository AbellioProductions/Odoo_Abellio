from odoo import models, fields, api

class MesMachineSettings(models.Model):
    _inherit = 'mes.machine.settings'

    log_conn_dt = fields.Datetime()
    log_cfg_req_dt = fields.Datetime()
    log_cfg_ok_dt = fields.Datetime()
    log_bind_req_dt = fields.Datetime()
    log_bind_ok_dt = fields.Datetime()
    log_plc_recv_dt = fields.Datetime()
    log_odoo_send_dt = fields.Datetime()
    
    log_err_msg = fields.Char()
    log_err_dt = fields.Datetime()

class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    log_conn_dt = fields.Datetime(related='machine_settings_id.log_conn_dt')
    log_cfg_req_dt = fields.Datetime(related='machine_settings_id.log_cfg_req_dt')
    log_cfg_ok_dt = fields.Datetime(related='machine_settings_id.log_cfg_ok_dt')
    log_bind_req_dt = fields.Datetime(related='machine_settings_id.log_bind_req_dt')
    log_bind_ok_dt = fields.Datetime(related='machine_settings_id.log_bind_ok_dt')
    log_plc_recv_dt = fields.Datetime(related='machine_settings_id.log_plc_recv_dt')
    log_odoo_send_dt = fields.Datetime(related='machine_settings_id.log_odoo_send_dt')
    
    log_err_msg = fields.Char(related='machine_settings_id.log_err_msg')
    log_err_dt = fields.Datetime(related='machine_settings_id.log_err_dt')
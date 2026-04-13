{
    'name': 'MES Core System',
    'version': '2.0',
    'summary': 'Machine Performance, Production Reports, Tasks',
    'author': 'Constantine',
    'category': 'Manufacturing/MES',
    'depends': ['base', 'mrp', 'mail', 'hr', 'queue_job'],
    'data': [
        'security/mes_security.xml',
        'security/ir.model.access.csv',

        'data/mes_cron.xml',

        'views/mes_root_menus.xml',
        'views/mes_report_base_views.xml',

        'wizard/dictionary_import_wizard.xml',
        'wizard/raw_data_csv_import_wizard.xml',

        'wizard/mes_downtime_planner.xml',
        
        'wizard/mes_hist_performance_wiz.xml',
        'wizard/mes_operation_wizards.xml',

        'views/mes_dictionaries_views.xml',
        'views/mes_machine_performance_view.xml',
        'views/mes_production_report_view.xml',
        'views/mes_task_view.xml',
        'views/res_config_settings_view.xml',
        'views/hr_employee_view.xml',
        'views/mes_telemetry_stats_views.xml',
        'views/mes_planned_downtime_views.xml',
        'views/mes_machine_operation.xml',
        
        'views/mes_telemetry_views.xml',
        'views/mes_machine_settings_views.xml',
        
        'wizard/mes_reject_report.xml',        
        'wizard/mes_analytics_report.xml',
        'wizard/mes_alarm_report.xml',

        

        'views/mes_menus.xml',
    ],

    'assets': {
        'web.assets_backend': [
            'mes_core/static/src/js/auto_refresh.js',
            'mes_core/static/src/js/list_auto_refresh.js',
            'mes_core/static/src/js/mes_time_util.js',
            'mes_core/static/src/js/machine_live_charts.js',
            'mes_core/static/src/js/hist_machine_charts.js',
            'mes_core/static/src/xml/machine_live_charts.xml',
            'mes_core/static/src/xml/hist_machine_charts.xml',
        ],
    },
    
    'installable': True,
    'application': True,
    'license': 'LGPL-3',
}

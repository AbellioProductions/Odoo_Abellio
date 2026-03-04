/** @odoo-module **/

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";
import { Component, onMounted, onWillUnmount, useState, useRef } from "@odoo/owl";

export class MachineLiveCharts extends Component {
    setup() {
        this.orm = useService("orm");
        this.canvasRef = useRef("chartCanvas");
        this.chartInstance = null;
        this.refreshInterval = null;
        
        this.state = useState({
            timeline: [],
            error: false,
            totalDuration: 1
        });

        onMounted(async () => {
            await loadJS("/web/static/lib/Chart/Chart.js");
            await this.fetchData();
            
            if (this.props.record.resId) {
                const freq = Math.max(this.props.record.data.refresh_frequency || 60, 10);
                this.refreshInterval = setInterval(() => this.fetchData(), freq * 1000);
            }
        });

        onWillUnmount(() => {
            if (this.refreshInterval) clearInterval(this.refreshInterval);
            if (this.chartInstance) this.chartInstance.destroy();
        });
    }

    async fetchData() {
        if (!this.props.record.resId) {
            this.state.error = "Please save the machine to view live charts.";
            return;
        }

        const result = await this.orm.call(
            "mrp.workcenter", 
            "get_live_chart_data", 
            [this.props.record.resId]
        );

        if (result.error) {
            this.state.error = result.error;
            return;
        }

        this.state.error = false;
        this.updateTimeline(result.timeline);
        this.updateChart(result.chart);
    }

    updateTimeline(data) {
        if (!data || !data.length) {
            this.state.timeline = [];
            return;
        }
        
        const total = data.reduce((acc, curr) => acc + curr.duration, 0);
        this.state.totalDuration = total > 0 ? total : 1;
        
        this.state.timeline = data.map(item => ({
            ...item,
            widthPct: (item.duration / this.state.totalDuration) * 100,
            durationMin: Math.round(item.duration / 60)
        }));
    }

    updateChart(data) {
        if (!this.canvasRef.el) return;
        
        if (this.chartInstance) {
            this.chartInstance.data.labels = data.labels;
            this.chartInstance.data.datasets[0].data = data.production;
            this.chartInstance.data.datasets[1].data = data.ideal;
            this.chartInstance.update();
            return;
        }

        const ctx = this.canvasRef.el.getContext("2d");
        this.chartInstance = new window.Chart(ctx, {
            type: 'line', 
            data: {
                labels: data.labels,
                datasets: [
                    {
                        label: 'Good Parts',
                        data: data.production,
                        borderColor: '#28a745',
                        backgroundColor: 'rgba(40, 167, 69, 0.15)', 
                        borderWidth: 2,
                        fill: true, 
                        tension: 0.3, 
                        pointRadius: 3, 
                        pointBackgroundColor: '#28a745',
                        order: 2
                    },
                    {
                        label: 'Ideal Capacity',
                        data: data.ideal,
                        type: 'line',
                        borderColor: '#dc3545',
                        borderWidth: 2,
                        borderDash: [5, 5], 
                        fill: false, 
                        pointRadius: 0, 
                        order: 1
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0 }, 
                scales: {
                    yAxes: [{ 
                        ticks: { 
                            beginAtZero: true 
                        } 
                    }]
                },
                tooltips: {
                    mode: 'index',
                    intersect: false,
                },
                hover: {
                    mode: 'nearest',
                    intersect: true
                }
            }
        });
    }
}

MachineLiveCharts.template = "mes_core.MachineLiveChartsTmpl";

registry.category("view_widgets").add("machine_live_charts", {
    component: MachineLiveCharts,
});
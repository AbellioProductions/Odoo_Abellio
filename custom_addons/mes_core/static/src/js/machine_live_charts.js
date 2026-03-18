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
        this.rawData = null; 
        
        this.state = useState({
            error: false,
            visibleTimeline: [],
            zoomLevel: 1,  
            panOffset: 0,
            availableCounts: [],
            selectedCountId: false,
            selectedCountName: 'Good Parts',
            availableProcesses: [],
            selectedProcessId: false,
            selectedProcessName: ''
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

        await this.orm.call("mrp.workcenter", "action_force_metrics_update", [[this.props.record.resId]]);
        if (this.props.record.load) {
            await this.props.record.load();
        }

        const result = await this.orm.call(
            "mrp.workcenter", 
            "get_live_chart_data", 
            [
                this.props.record.resId, 
                this.state.selectedCountId || false,
                this.state.selectedProcessId || false
            ]
        );

        if (result.error) {
            this.state.error = result.error;
            return;
        }

        this.state.error = false;
        this.rawData = result;
        
        this.state.availableCounts = result.available_counts;
        this.state.selectedCountId = result.selected_count_id;
        this.state.selectedCountName = result.selected_count_name;

        if (result.available_processes) {
            this.state.availableProcesses = result.available_processes;
        }
        if (result.selected_process_id) {
            this.state.selectedProcessId = result.selected_process_id;
            this.state.selectedProcessName = result.selected_process_name;
        }
        
        this.applyZoomAndPan(); 
    }

    async onCountChange(ev) {
        this.state.selectedCountId = parseInt(ev.target.value);
        await this.fetchData();
    }

    async onProcessChange(ev) {
        const val = ev.target.value;
        this.state.selectedProcessId = val ? parseInt(val) : false;
        this.state.selectedProcessName = val ? ev.target.options[ev.target.selectedIndex].text : '';
        await this.fetchData();
    }

    onWheelZoom(ev) {
        ev.preventDefault(); 
        const zoomStep = 0.5;
        let newZoom = parseFloat(this.state.zoomLevel);
        
        if (ev.deltaY < 0) {
            newZoom = Math.min(20, newZoom + zoomStep);
        } else {
            newZoom = Math.max(1, newZoom - zoomStep);
        }
        
        this.state.zoomLevel = newZoom;
        this.applyZoomAndPan();
    }

    applyZoomAndPan() {
        if (!this.rawData) return;
        
        const zl = parseFloat(this.state.zoomLevel) || 1;
        const pan = parseFloat(this.state.panOffset) || 0;
        const totalSec = this.rawData.chart_duration_sec || 28800;
        const bucketSec = (this.rawData.chart && this.rawData.chart.bucket_sec) ? this.rawData.chart.bucket_sec : 900;
        
        let shiftStartMs = Date.now();
        if (this.rawData.shift_start) {
            shiftStartMs = new Date(this.rawData.shift_start).getTime();
        } else if (this.rawData.chart && this.rawData.chart.labels && this.rawData.chart.labels.length > 0) {
            shiftStartMs = new Date(this.rawData.chart.labels[0]).getTime();
        }
        if (isNaN(shiftStartMs)) shiftStartMs = Date.now();

        const desiredViewSec = totalSec / zl;
        const maxOffsetSec = totalSec - desiredViewSec;
        const desiredStartSec = maxOffsetSec * (pan / 100);
        const desiredEndSec = desiredStartSec + desiredViewSec;

        let startIdx = Math.floor(desiredStartSec / bucketSec);
        let endIdx = Math.ceil(desiredEndSec / bucketSec);
        startIdx = Math.max(0, startIdx);
        
        const prodData = (this.rawData.chart && this.rawData.chart.production) || [];
        const labelsLength = prodData.length > 0 ? prodData.length : 1;
        endIdx = Math.min(labelsLength - 1, endIdx);

        if (endIdx - startIdx < 1) {
            endIdx = Math.min(labelsLength - 1, startIdx + 1);
        }

        const actualStartSec = startIdx * bucketSec;
        const actualEndSec = endIdx * bucketSec;
        const actualViewSec = actualEndSec - actualStartSec;

        this.state.visibleTimeline = [];
        if (this.rawData.timeline) {
            for (const block of this.rawData.timeline) {
                const blockStartSec = (new Date(block.start).getTime() - shiftStartMs) / 1000;
                const blockEndSec = (new Date(block.end).getTime() - shiftStartMs) / 1000;
                const clampedStart = Math.max(actualStartSec, blockStartSec);
                const clampedEnd = Math.min(actualEndSec, blockEndSec);

                if (clampedStart < clampedEnd) {
                    this.state.visibleTimeline.push({
                        ...block,
                        widthPct: ((clampedEnd - clampedStart) / actualViewSec) * 100,
                        durationMin: Math.round(block.duration / 60)
                    });
                }
            }
        }

        let slicedProcess = null;
        if (this.rawData.chart && Array.isArray(this.rawData.chart.process)) {
            let rawProcess = [];
            for (let i = 0; i < this.rawData.chart.process.length; i++) {
                const pt = this.rawData.chart.process[i];
                if (pt != null && pt.x !== undefined) {
                    const xVal = (new Date(pt.x).getTime() - shiftStartMs) / 1000;
                    if (!isNaN(xVal)) {
                        rawProcess.push({ x: xVal, y: Number(pt.y) });
                    }
                }
            }
            
            slicedProcess = rawProcess.filter(pt => pt.x >= actualStartSec && pt.x <= actualEndSec);
            
            const beforeStart = rawProcess.filter(pt => pt.x < actualStartSec);
            if (beforeStart.length > 0) {
                slicedProcess.unshift({ x: actualStartSec, y: beforeStart[beforeStart.length - 1].y });
            }
            if (slicedProcess.length > 0) {
                slicedProcess.push({ x: actualEndSec, y: slicedProcess[slicedProcess.length - 1].y });
            }
        }

        const slicedProduction = [];
        const slicedIdeal = [];
        
        if (this.rawData.chart && Array.isArray(this.rawData.chart.production)) {
            for (let i = startIdx; i <= endIdx; i++) {
                const sec = i * bucketSec;
                if (i < this.rawData.chart.production.length) {
                    slicedProduction.push({ x: sec, y: Number(this.rawData.chart.production[i]) });
                    if (this.rawData.chart.ideal && i < this.rawData.chart.ideal.length) {
                        slicedIdeal.push({ x: sec, y: Number(this.rawData.chart.ideal[i]) });
                    }
                }
            }
        }

        const slicedData = {
            production: slicedProduction.map(p => ({ x: p.x, y: p.y })),
            ideal: slicedIdeal.map(p => ({ x: p.x, y: p.y })),
            show_ideal: this.rawData.chart ? !!this.rawData.chart.show_ideal : false,
            process: slicedProcess ? slicedProcess.map(p => ({ x: p.x, y: p.y })) : null,
            xMin: actualStartSec,
            xMax: actualEndSec,
            shiftStartMs: shiftStartMs,
            bucketSec: bucketSec
        };

        this.updateChart(slicedData);
    }

    updateChart(data) {
        if (!this.canvasRef.el) return;

        const isV3 = typeof window.Chart.defaults.plugins !== 'undefined';
        const shiftStartMs = data.shiftStartMs;
        
        const formatTime = (seconds) => {
            if (isNaN(seconds)) return '';
            const d = new Date(shiftStartMs + seconds * 1000);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
        };

        const alignTimeline = (chart) => {
            const chartArea = chart.chartArea;
            const canvas = chart.canvas || (chart.chart && chart.chart.canvas);
            if (!canvas || !chartArea) return;
            const dashboard = canvas.closest('.o_mes_live_dashboard');
            const wrapper = dashboard ? dashboard.querySelector('.mes-timeline-wrapper') : null;
            if (wrapper) {
                wrapper.style.marginLeft = chartArea.left + 'px';
                wrapper.style.width = (chartArea.right - chartArea.left) + 'px';
            }
        };

        const cleanProd = [];
        for (let i = 0; i < data.production.length; i++) {
            cleanProd.push({ x: data.production[i].x, y: data.production[i].y });
        }

        let cleanIdeal = [];
        if (data.ideal && data.ideal.length > 0) {
            for (let i = 0; i < data.ideal.length; i++) {
                cleanIdeal.push({ x: data.ideal[i].x, y: data.ideal[i].y });
            }
        }

        let cleanProcess = [];
        if (data.process && data.process.length > 0) {
            for (let i = 0; i < data.process.length; i++) {
                cleanProcess.push({ x: data.process[i].x, y: data.process[i].y });
            }
        }

        if (this.chartInstance) {
            this.chartInstance.destroy();
            this.chartInstance = null;
        }

        const ctx = this.canvasRef.el.getContext("2d");
        
        const datasets = [{
            label: this.state.selectedCountName || '',
            data: cleanProd,
            xAxisID: 'x',
            yAxisID: 'yCount',
            borderColor: '#28a745',
            backgroundColor: 'rgba(40, 167, 69, 0.15)',
            borderWidth: 2,
            fill: true,
            tension: 0.3, 
            pointRadius: 3,
            pointBackgroundColor: '#28a745',
            order: 2
        }];

        if (data.show_ideal && cleanIdeal.length > 0) {
            datasets.push({
                label: 'Ideal Capacity',
                data: cleanIdeal,
                xAxisID: 'x',
                yAxisID: 'yCount',
                type: 'line',
                borderColor: '#dc3545',
                borderWidth: 2,
                borderDash: [5, 5],
                fill: false,
                pointRadius: 0,
                order: 1
            });
        }

        if (this.state.selectedProcessName && cleanProcess.length > 0) {
            datasets.push({
                label: this.state.selectedProcessName,
                data: cleanProcess,
                xAxisID: 'x',
                yAxisID: 'yProcess',
                borderColor: '#dc3545',
                backgroundColor: 'transparent',
                borderWidth: 2,
                fill: false,
                stepped: true,
                steppedLine: true,
                tension: 0,
                lineTension: 0,
                pointRadius: 3,
                pointBackgroundColor: '#dc3545',
                order: 1
            });
        }

        let scalesConfig = {};
        if (isV3) {
            scalesConfig = {
                x: {
                    type: 'linear',
                    min: data.xMin,
                    max: data.xMax,
                    ticks: { 
                        stepSize: data.bucketSec,
                        maxRotation: 45, 
                        minRotation: 45, 
                        callback: function(value) { return formatTime(value); } 
                    }
                },
                yCount: { type: 'linear', position: 'left', beginAtZero: true },
                yProcess: { type: 'linear', position: 'right', beginAtZero: true, grid: { drawOnChartArea: false } }
            };
        } else {
            scalesConfig = {
                xAxes: [{
                    id: 'x',
                    type: 'linear',
                    ticks: { 
                        min: data.xMin, 
                        max: data.xMax, 
                        stepSize: data.bucketSec,
                        maxRotation: 45, 
                        minRotation: 45, 
                        callback: function(value) { return formatTime(value); } 
                    }
                }],
                yAxes: [
                    { id: 'yCount', type: 'linear', position: 'left', ticks: { beginAtZero: true } },
                    { id: 'yProcess', type: 'linear', position: 'right', ticks: { beginAtZero: true }, gridLines: { drawOnChartArea: false } }
                ]
            };
        }

        let tooltipsConfig = isV3 ? {} : {
            mode: 'index', 
            intersect: false,
            callbacks: {
                title: function(tooltipItems) {
                    if (!tooltipItems.length) return '';
                    return formatTime(tooltipItems[0].xLabel);
                }
            }
        };

        let pluginsConfig = isV3 ? {
            tooltip: {
                mode: 'index',
                intersect: false,
                callbacks: {
                    title: function(tooltipItems) {
                        if (!tooltipItems.length) return '';
                        return formatTime(tooltipItems[0].parsed.x);
                    }
                }
            }
        } : {};

        this.chartInstance = new window.Chart(ctx, {
            type: 'line',
            data: { datasets: datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { 
                    duration: 0,
                    onComplete: function() { alignTimeline(this); },
                    onProgress: function() { alignTimeline(this); }
                }, 
                scales: scalesConfig,
                tooltips: tooltipsConfig,
                plugins: pluginsConfig,
                hover: { mode: 'nearest', intersect: true }
            }
        });
    }
}

MachineLiveCharts.template = "mes_core.MachineLiveChartsTmpl";
registry.category("view_widgets").add("machine_live_charts", { component: MachineLiveCharts });
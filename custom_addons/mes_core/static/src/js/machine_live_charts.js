/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, onWillUnmount, onMounted, useRef, useState, onWillUpdateProps } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";

export class MachineLiveCharts extends Component {
    setup() {
        this.orm = useService("orm");
        this.canvasRef = useRef("chartCanvas"); 
        
        this.chartInst = null;
        this.rawMetric = null;
        this.baseEpochMs = 0;
        this.pollTimer = null;

        this.state = useState({
            error: false,
            visibleTimeline: [],
            zoomLevel: 1,
            panOffset: 0,
            availableCounts: [],
            selectedCountId: false,
            selectedCountName: '',
            availableProcesses: [],
            selectedProcessId: false,
            selectedProcessName: '',
            chartMode: 'total'
        });

        onMounted(async () => {
            await loadJS("/web/static/lib/Chart/Chart.js");
            await this.fetchData(true);
            await this.initPollTask();
        });

        onWillUpdateProps(async (nextProps) => {
            if (nextProps.record.resId !== this.props.record.resId) {
                await this.fetchData(true); 
            }
        });

        onWillUnmount(() => {
            this.stopPollTask();
            this.destroyChart();
        });
    }

    async initPollTask() {
        let intMs = 30000;
        try {
            const confInt = await this.orm.call(
                "ir.config_parameter", 
                "get_param", 
                ["mes_core.dashboard_refresh_interval", "30"]
            );
            intMs = parseInt(confInt || "30", 10) * 1000;
        } catch (err) {
            intMs = 30000;
        }
        this.pollTimer = setInterval(() => {
            this.execSync();
        }, intMs);
    }

    stopPollTask() {
        if (this.pollTimer) {
            clearInterval(this.pollTimer);
            this.pollTimer = null;
        }
    }

    async execSync() {
        const isLocked = this.props.record.isDirty || this.props.record.isSaving;
        await this.fetchData(isLocked);
    }

    destroyChart() {
        if (this.chartInst) {
            this.chartInst.destroy();
            this.chartInst = null;
        }
    }

    parseIsoMs(val) {
        if (!val) return NaN;
        if (typeof val === 'number') return val;
        const normIso = String(val).replace(' ', 'T') + "Z";
        return new Date(normIso).getTime();
    }

    getLineColor(idx) {
        const colors = ['#dc3545', '#007bff', '#fd7e14', '#6f42c1', '#20c997', '#e83e8c', '#17a2b8', '#ffc107'];
        return colors[idx % colors.length];
    }

    setChartMode(mode) {
        if (this.state.chartMode !== mode) {
            this.state.chartMode = mode;
            this.applyZoomAndPan();
        }
    }

    async fetchData(skipLoad = false) {
        if (!this.props.record.resId) {
            this.state.error = "Please save the machine to view live charts.";
            return;
        }
        try {
            await this.orm.call("mrp.workcenter", "action_force_metrics_update", [[this.props.record.resId]]);
            if (!skipLoad && typeof this.props.record.load === 'function') {
                await this.props.record.load();
            }
            const chartRes = await this.orm.call(
                "mrp.workcenter", 
                "get_live_chart_data", 
                [
                    this.props.record.resId, 
                    this.state.selectedCountId || false,
                    this.state.selectedProcessId || false
                ]
            );
            if (chartRes.error) {
                this.state.error = chartRes.error;
                return;
            }
            this.state.error = false;
            this.rawMetric = chartRes;
            const shiftStart = this.rawMetric?.shift_start || "1970-01-01T00:00:00";
            this.baseEpochMs = this.parseIsoMs(shiftStart);
            this.state.availableCounts = chartRes.available_counts || [];
            this.state.selectedCountId = chartRes.selected_count_id;
            this.state.selectedCountName = chartRes.selected_count_name;
            this.state.availableProcesses = chartRes.available_processes || [];
            this.state.selectedProcessId = chartRes.selected_process_id;
            this.state.selectedProcessName = chartRes.selected_process_name;
            await this.applyZoomAndPan(); 
        } catch (err) {
            this.state.error = "Data fetch synchronization failed.";
        }
    }

    async onCountChange(ev) {
        this.state.selectedCountId = parseInt(ev.target.value);
        await this.fetchData(true);
    }

    async onProcessChange(ev) {
        const val = ev.target.value;
        this.state.selectedProcessId = val ? parseInt(val) : false;
        this.state.selectedProcessName = val ? ev.target.options[ev.target.selectedIndex].text : '';
        await this.fetchData(true);
    }

    async onWheelZoom(ev) {
        ev.preventDefault(); 
        const step = 0.5;
        let zoom = parseFloat(this.state.zoomLevel);
        zoom = ev.deltaY < 0 ? Math.min(20, zoom + step) : Math.max(1, zoom - step);
        this.state.zoomLevel = zoom;
        await this.applyZoomAndPan();
    }

    async applyZoomAndPan() {
        await new Promise(res => setTimeout(res, 0));
        if (!this.rawMetric || !this.rawMetric.chart || !this.canvasRef.el) return;
        const scale = parseFloat(this.state.zoomLevel) || 1;
        const pan = parseFloat(this.state.panOffset) || 0;
        const durSec = this.rawMetric.chart_duration_sec || 28800;
        const stepSec = this.rawMetric.chart?.bucket_sec || 900;
        const isRateMode = this.state.chartMode === 'rate';
        const viewDur = durSec / scale;
        const maxShift = durSec - viewDur;
        const startSec = maxShift * (pan / 100);
        const endSec = startSec + viewDur;
        let sIdx = Math.max(0, Math.floor(startSec / stepSec));
        let eIdx = Math.ceil(endSec / stepSec);
        const prodData = this.rawMetric.chart?.production || [];
        if (eIdx - sIdx < 1) {
            eIdx = sIdx + 1;
        }
        const boundStart = sIdx * stepSec;
        const boundEnd = eIdx * stepSec;
        const activeDur = Math.max(1, boundEnd - boundStart);
        this.state.visibleTimeline = [];
        
        const runningBlocksRaw = [];

        if (this.rawMetric.timeline) {
            for (const blk of this.rawMetric.timeline) {
                const bStart = (this.parseIsoMs(blk.start) - this.baseEpochMs) / 1000;
                const bEnd = (this.parseIsoMs(blk.end) - this.baseEpochMs) / 1000;
                
                if (blk.name === 'Running' || blk.color === '#28a745') {
                    runningBlocksRaw.push({ start: bStart, end: bEnd });
                }

                const cStart = Math.max(boundStart, bStart);
                const cEnd = Math.min(boundEnd, bEnd);
                if (cStart < cEnd && !isNaN(cStart) && !isNaN(cEnd)) {
                    this.state.visibleTimeline.push({
                        ...blk,
                        leftPct: ((cStart - boundStart) / activeDur) * 100,
                        widthPct: ((cEnd - cStart) / activeDur) * 100,
                        durationMin: Math.round(blk.duration / 60)
                    });
                }
            }
        }

        runningBlocksRaw.sort((a, b) => a.start - b.start);
        const runningBlocksSec = [];
        for (const rb of runningBlocksRaw) {
            if (runningBlocksSec.length === 0) {
                runningBlocksSec.push({ start: rb.start, end: rb.end });
            } else {
                const last = runningBlocksSec[runningBlocksSec.length - 1];
                if (rb.start <= last.end) {
                    last.end = Math.max(last.end, rb.end);
                } else {
                    runningBlocksSec.push({ start: rb.start, end: rb.end });
                }
            }
        }

        const procList = [];
        if (Array.isArray(this.rawMetric.chart?.processes)) {
            for (const proc of this.rawMetric.chart.processes) {
                if (!Array.isArray(proc.data)) continue;
                const rawPts = [];
                for (const pt of proc.data) {
                    if (pt?.x !== undefined) {
                        const pX = (this.parseIsoMs(pt.x) - this.baseEpochMs) / 1000;
                        if (!isNaN(pX)) {
                            rawPts.push({ x: pX, y: Number(pt.y) });
                        }
                    }
                }
                const visPts = rawPts.filter(pt => pt.x >= boundStart && pt.x <= boundEnd);
                const prePts = rawPts.filter(pt => pt.x < boundStart);
                if (prePts.length > 0) {
                    visPts.unshift({ x: boundStart, y: prePts[prePts.length - 1].y });
                }
                if (visPts.length > 0) {
                    const lastPt = visPts[visPts.length - 1];
                    if (lastPt.x < boundEnd) {
                        visPts.push({ x: boundEnd, y: lastPt.y });
                    }
                }
                procList.push({ name: proc.name, data: visPts });
            }
        }
        
        const outProd = [];
        const outIdeal = [];
        const idlData = this.rawMetric.chart?.ideal || [];
        
        for (let i = sIdx; i <= eIdx; i++) {
            if (i < prodData.length) {
                const pX = i * stepSec;
                let qty = Number(prodData[i]) || 0;
                let idealQty = Number(idlData[i]) || 0;
                
                if (isRateMode) {
                    const bucketStart = pX - stepSec;
                    const bucketEnd = pX;
                    let runSec = 0;

                    for (const rb of runningBlocksSec) {
                        const oStart = Math.max(bucketStart, rb.start);
                        const oEnd = Math.min(bucketEnd, rb.end);
                        if (oStart < oEnd) {
                            runSec += (oEnd - oStart);
                        }
                    }

                    if (runSec > 0) {
                        const runMin = runSec / 60;
                        qty = Number(((qty / runMin) * 60).toFixed(0));
                    } else {
                        qty = 0;
                    }
                    
                    const hrs = stepSec / 3600;
                    idealQty = Number((idealQty / hrs).toFixed(0));
                }

                outProd.push({ x: pX, y: qty });
                if (idlData[i] !== undefined) {
                    outIdeal.push({ x: pX, y: idealQty });
                }
            }
        }

        this.renderChart({
            production: outProd,
            ideal: outIdeal,
            showIdeal: !!this.rawMetric.chart?.show_ideal,
            processes: procList,
            xMin: boundStart,
            xMax: boundEnd,
            stepSec: stepSec,
            isRateMode: isRateMode
        });
    }

    renderChart(cfg) {
        if (!this.canvasRef.el) return;
        const isV3 = typeof window.Chart.defaults.plugins !== 'undefined';
        const fmtTime = (sec) => {
            if (isNaN(sec)) return '';
            const dt = new Date(this.baseEpochMs + sec * 1000);
            const iso = dt.toISOString();
            return `${iso.substring(8, 10)}.${iso.substring(5, 7)}.${iso.substring(0, 4)} ${iso.substring(11, 16)}`;
        };
        const syncTm = (inst) => {
            const area = inst.chartArea;
            const cvs = inst.canvas || inst.chart?.canvas;
            if (!cvs || !area) return;
            const wrap = cvs.closest('.o_form_view')?.querySelector('.mes-timeline-wrapper');
            if (wrap) {
                wrap.style.marginLeft = `${area.left}px`;
                wrap.style.width = `${area.right - area.left}px`;
            }
        };
        this.destroyChart();
        const ctx = this.canvasRef.el.getContext("2d");
        
        const prodLabel = cfg.isRateMode 
            ? `${this.rawMetric.selected_count_name || 'Production'} (Units / Hour)` 
            : `${this.rawMetric.selected_count_name || 'Production'} (Total)`;
            
        const ds = [{
            label: prodLabel,
            data: cfg.production,
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
        if (cfg.showIdeal && cfg.ideal.length > 0) {
            ds.push({
                label: cfg.isRateMode ? 'Ideal Capacity / Hour' : 'Ideal Capacity',
                data: cfg.ideal,
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
        let hasProc = false;
        if (cfg.processes?.length > 0) {
            cfg.processes.forEach((prc, idx) => {
                if (prc.data.length > 0) {
                    hasProc = true;
                    const c = this.getLineColor(idx);
                    ds.push({
                        label: prc.name,
                        data: prc.data,
                        xAxisID: 'x',
                        yAxisID: 'yProcess',
                        borderColor: c,
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        fill: false,
                        stepped: true,
                        steppedLine: true,
                        tension: 0,
                        lineTension: 0,
                        pointRadius: 3,
                        pointBackgroundColor: c,
                        order: 1
                    });
                }
            });
        }
        let ax = {};
        if (isV3) {
            ax = {
                x: {
                    type: 'linear', min: cfg.xMin, max: cfg.xMax,
                    ticks: { stepSize: cfg.stepSec, maxRotation: 45, minRotation: 45, callback: fmtTime }
                },
                yCount: { 
                    type: 'linear', position: 'left', beginAtZero: true,
                    title: { display: true, text: cfg.isRateMode ? 'Units/Hour' : 'Pieces' }
                },
                yProcess: { type: 'linear', position: 'right', beginAtZero: false, display: hasProc, grid: { drawOnChartArea: false } }
            };
        } else {
            ax = {
                xAxes: [{
                    id: 'x', type: 'linear',
                    ticks: { min: cfg.xMin, max: cfg.xMax, stepSize: cfg.stepSec, maxRotation: 45, minRotation: 45, callback: fmtTime }
                }],
                yAxes: [
                    { 
                        id: 'yCount', type: 'linear', position: 'left', ticks: { beginAtZero: true },
                        scaleLabel: { display: true, labelString: cfg.isRateMode ? 'Units/Hour' : 'Pieces' }
                    },
                    { id: 'yProcess', type: 'linear', position: 'right', display: hasProc, gridLines: { drawOnChartArea: false } }
                ]
            };
        }
        const pl = [ { id: 'syncTm', afterLayout: syncTm } ];
        this.chartInst = new window.Chart(ctx, {
            type: 'line', data: { datasets: ds },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 }, 
                scales: ax, 
                tooltips: isV3 ? {} : { mode: 'index', intersect: false, callbacks: { title: (i) => i.length ? fmtTime(i[0].xLabel) : '' } },
                plugins: isV3 ? { tooltip: { mode: 'index', intersect: false, callbacks: { title: (i) => i.length ? fmtTime(i[0].parsed.x) : '' } } } : {},
                hover: { mode: 'nearest', intersect: true }
            },
            plugins: pl
        });
    }
}

MachineLiveCharts.template = "mes_core.MachineLiveChartsTmpl";
registry.category("view_widgets").add("machine_live_charts", { component: MachineLiveCharts });
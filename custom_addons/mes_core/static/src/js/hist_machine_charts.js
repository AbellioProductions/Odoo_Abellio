/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, onWillUnmount, onMounted, useRef, useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";

export class HistMachineCharts extends Component {
    setup() {
        this.orm = useService("orm");
        this.canvasRef = useRef("chartCanvas"); 
        
        this.chartInst = null;
        this.rawMetric = null;
        this.baseEpochMs = 0;

        this.state = useState({
            error: false,
            visibleTimeline: [],
            zoomLevel: 1,
            panOffset: 0,
            chartMode: 'total'
        });

        onMounted(async () => {
            await loadJS("/web/static/lib/Chart/Chart.js");
            await this.fetchData();
        });

        onWillUnmount(() => {
            this.destroyChart();
        });
    }

    destroyChart() {
        if (this.chartInst) {
            this.chartInst.destroy();
            this.chartInst = null;
        }
    }

    parseIsolatedMs(val) {
        if (!val) return NaN;
        if (typeof val === 'number') return val;
        if (val.isLuxonDateTime || (val.toUTC && typeof val.toUTC === 'function')) {
            const rawUtcStr = val.toUTC().toFormat("yyyy-MM-dd HH:mm:ss");
            return new Date(rawUtcStr.replace(' ', 'T') + "Z").getTime();
        }
        if (val._isAMomentObject || (val.utc && typeof val.utc === 'function')) {
            const rawUtcStr = val.utc().format("YYYY-MM-DD HH:mm:ss");
            return new Date(rawUtcStr.replace(' ', 'T') + "Z").getTime();
        }
        if (val instanceof Date) {
            const isoStr = val.toISOString().substring(0, 19);
            return new Date(isoStr.replace(' ', 'T') + "Z").getTime();
        }
        let cleanStr = String(val).trim();
        if (cleanStr.length > 19) {
            cleanStr = cleanStr.substring(0, 19);
        }
        cleanStr = cleanStr.replace(' ', 'T') + "Z";
        return new Date(cleanStr).getTime();
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

    async fetchData() {
        if (!this.props.record.resId) return;
        const res = await this.orm.call(
            "mes.hist.dashboard.wiz", 
            "get_chart_data", 
            [[this.props.record.resId]]
        );
        if (res.error) {
            this.state.error = res.error;
            return;
        }
        this.state.error = false;
        this.rawMetric = res;
        const rawStart = this.rawMetric?.shift_start || "1970-01-01 00:00:00";
        this.baseEpochMs = this.parseIsolatedMs(rawStart);
        await this.applyZoomAndPan(); 
    }

    async onWheelZoom(ev) {
        ev.preventDefault(); 
        const step = 0.5;
        let scale = parseFloat(this.state.zoomLevel);
        scale = ev.deltaY < 0 ? Math.min(20, scale + step) : Math.max(1, scale - step);
        this.state.zoomLevel = scale;
        await this.applyZoomAndPan();
    }

    async applyZoomAndPan() {
        await new Promise(resolve => setTimeout(resolve, 0));
        if (!this.rawMetric || !this.rawMetric.chart || !this.canvasRef.el) return;
        const scale = parseFloat(this.state.zoomLevel) || 1;
        const pan = parseFloat(this.state.panOffset) || 0;
        const totalDurSec = this.rawMetric.chart_duration_sec || 28800;
        const stepSec = this.rawMetric.chart?.bucket_sec || 900;
        const isRateMode = this.state.chartMode === 'rate';
        const viewDurSec = totalDurSec / scale;
        const maxShiftSec = totalDurSec - viewDurSec;
        const viewStartSec = maxShiftSec * (pan / 100);
        const viewEndSec = viewStartSec + viewDurSec;
        let startIdx = Math.max(0, Math.floor(viewStartSec / stepSec));
        let endIdx = Math.ceil(viewEndSec / stepSec);
        const prodSeries = this.rawMetric.chart?.production || [];
        if (endIdx - startIdx < 1) {
            endIdx = startIdx + 1;
        }
        const boundStartSec = startIdx * stepSec;
        const boundEndSec = endIdx * stepSec;
        const activeDurSec = Math.max(1, boundEndSec - boundStartSec);
        this.state.visibleTimeline = [];
        
        const runningBlocksRaw = [];

        if (this.rawMetric.timeline) {
            for (const block of this.rawMetric.timeline) {
                const blockStartSec = (this.parseIsolatedMs(block.start) - this.baseEpochMs) / 1000;
                const blockEndSec = (this.parseIsolatedMs(block.end) - this.baseEpochMs) / 1000;
                
                if (block.name === 'Running' || block.color === '#28a745') {
                    runningBlocksRaw.push({ start: blockStartSec, end: blockEndSec });
                }

                const clampStart = Math.max(boundStartSec, blockStartSec);
                const clampEnd = Math.min(boundEndSec, blockEndSec);
                if (clampStart < clampEnd && !isNaN(clampStart) && !isNaN(clampEnd)) {
                    this.state.visibleTimeline.push({
                        ...block,
                        leftPct: ((clampStart - boundStartSec) / activeDurSec) * 100,
                        widthPct: ((clampEnd - clampStart) / activeDurSec) * 100,
                        durationMin: Math.round(block.duration / 60)
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

        const processEndSec = boundEndSec;
        const procList = [];
        if (Array.isArray(this.rawMetric.chart?.processes)) {
            for (const proc of this.rawMetric.chart.processes) {
                if (!Array.isArray(proc.data)) continue;
                const rawProc = [];
                for (const pt of proc.data) {
                    if (pt?.x !== undefined) {
                        const ptX = (this.parseIsolatedMs(pt.x) - this.baseEpochMs) / 1000;
                        if (!isNaN(ptX)) {
                            rawProc.push({ x: ptX, y: Number(pt.y) });
                        }
                    }
                }
                const slicedProc = rawProc.filter(pt => pt.x >= boundStartSec && pt.x <= processEndSec);
                const prevPts = rawProc.filter(pt => pt.x < boundStartSec);
                if (prevPts.length > 0) {
                    slicedProc.unshift({ x: boundStartSec, y: prevPts[prevPts.length - 1].y });
                }
                if (slicedProc.length > 0) {
                    const lastState = slicedProc[slicedProc.length - 1];
                    if (lastState.x < processEndSec) {
                        slicedProc.push({ x: processEndSec, y: lastState.y });
                    }
                }
                procList.push({ name: proc.name, data: slicedProc });
            }
        }
        
        const outProd = [];
        const outIdeal = [];
        const idealSeries = this.rawMetric.chart?.ideal || [];
        
        for (let i = startIdx; i <= endIdx; i++) {
            if (i < prodSeries.length) {
                const ptX = i * stepSec;
                let qty = Number(prodSeries[i]) || 0;
                let idealQty = Number(idealSeries[i]) || 0;
                
                if (isRateMode) {
                    const bucketStart = ptX - stepSec;
                    const bucketEnd = ptX;
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

                outProd.push({ x: ptX, y: qty });
                if (idealSeries[i] !== undefined) {
                    outIdeal.push({ x: ptX, y: idealQty });
                }
            }
        }

        this.updateChart({
            production: outProd,
            ideal: outIdeal,
            showIdeal: !!this.rawMetric.chart?.show_ideal,
            processes: procList,
            xMin: boundStartSec,
            xMax: boundEndSec,
            stepSec: stepSec,
            isRateMode: isRateMode
        });
    }

    updateChart(plot) {
        if (!this.canvasRef.el) return;
        const isV3 = typeof window.Chart.defaults.plugins !== 'undefined';
        const formatIsolatedTime = (sec) => {
            if (isNaN(sec)) return '';
            const d = new Date(this.baseEpochMs + sec * 1000);
            const iso = d.toISOString();
            const year = iso.substring(0, 4);
            const month = iso.substring(5, 7);
            const day = iso.substring(8, 10);
            const hhmm = iso.substring(11, 16);
            return `${day}.${month}.${year} ${hhmm}`;
        };
        const alignTimeline = (chart) => {
            const area = chart.chartArea;
            const cnv = chart.canvas || chart.chart?.canvas;
            if (!cnv || !area) return;
            const dash = cnv.closest('.o_form_view');
            const wrap = dash?.querySelector('.mes-timeline-wrapper');
            if (wrap) {
                wrap.style.marginLeft = `${area.left}px`;
                wrap.style.width = `${area.right - area.left}px`;
            }
        };
        this.destroyChart();
        const ctx = this.canvasRef.el.getContext("2d");
        
        const prodLabel = plot.isRateMode 
            ? `${this.rawMetric.selected_count_name || 'Production'} (Units / Hour)` 
            : `${this.rawMetric.selected_count_name || 'Production'} (Total)`;
            
        const ds = [{
            label: prodLabel,
            data: plot.production,
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
        if (plot.showIdeal && plot.ideal.length > 0) {
            ds.push({
                label: plot.isRateMode ? 'Ideal Capacity / Hour' : 'Ideal Capacity',
                data: plot.ideal,
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
        let hasProcess = false;
        if (plot.processes?.length > 0) {
            plot.processes.forEach((proc, idx) => {
                if (proc.data.length > 0) {
                    hasProcess = true;
                    const color = this.getLineColor(idx);
                    ds.push({
                        label: proc.name,
                        data: proc.data,
                        xAxisID: 'x',
                        yAxisID: 'yProcess',
                        borderColor: color,
                        backgroundColor: 'transparent',
                        borderWidth: 2,
                        fill: false,
                        stepped: true,
                        steppedLine: true,
                        tension: 0,
                        lineTension: 0,
                        pointRadius: 3,
                        pointBackgroundColor: color,
                        order: 1
                    });
                }
            });
        }
        let axCfg = {};
        if (isV3) {
            axCfg = {
                x: {
                    type: 'linear', min: plot.xMin, max: plot.xMax,
                    ticks: { stepSize: plot.stepSec, maxRotation: 45, minRotation: 45, callback: formatIsolatedTime }
                },
                yCount: { 
                    type: 'linear', position: 'left', beginAtZero: true,
                    title: { display: true, text: plot.isRateMode ? 'Units/Hour' : 'Pieces' }
                },
                yProcess: { 
                    type: 'linear', position: 'right', beginAtZero: false,
                    display: hasProcess, grid: { drawOnChartArea: false } 
                }
            };
        } else {
            axCfg = {
                xAxes: [{
                    id: 'x', type: 'linear',
                    ticks: { min: plot.xMin, max: plot.xMax, stepSize: plot.stepSec, maxRotation: 45, minRotation: 45, callback: formatIsolatedTime }
                }],
                yAxes: [
                    { 
                        id: 'yCount', type: 'linear', position: 'left', ticks: { beginAtZero: true },
                        scaleLabel: { display: true, labelString: plot.isRateMode ? 'Units/Hour' : 'Pieces' }
                    },
                    { id: 'yProcess', type: 'linear', position: 'right', display: hasProcess, gridLines: { drawOnChartArea: false } }
                ]
            };
        }
        const ttCfg = isV3 ? {} : {
            mode: 'index', intersect: false,
            callbacks: { title: (items) => items.length ? formatIsolatedTime(items[0].xLabel) : '' }
        };
        const plCfg = isV3 ? {
            tooltip: {
                mode: 'index', intersect: false,
                callbacks: { title: (items) => items.length ? formatIsolatedTime(items[0].parsed.x) : '' }
            }
        } : {};
        const syncTimelinePlugin = {
            id: 'syncTimeline',
            afterLayout: function(chart) { alignTimeline(chart); }
        };
        this.chartInst = new window.Chart(ctx, {
            type: 'line', data: { datasets: ds },
            options: {
                responsive: true, maintainAspectRatio: false, animation: { duration: 0 }, 
                scales: axCfg, tooltips: ttCfg, plugins: plCfg, hover: { mode: 'nearest', intersect: true }
            },
            plugins: [syncTimelinePlugin]
        });
    }
}

HistMachineCharts.template = "mes_core.HistMachineChartsTmpl";
registry.category("view_widgets").add("hist_machine_charts", { component: HistMachineCharts });
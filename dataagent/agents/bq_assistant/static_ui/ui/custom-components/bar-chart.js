var __esDecorate = (this && this.__esDecorate) || function (ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
    function accept(f) { if (f !== void 0 && typeof f !== "function") throw new TypeError("Function expected"); return f; }
    var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
    var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
    var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
    var _, done = false;
    for (var i = decorators.length - 1; i >= 0; i--) {
        var context = {};
        for (var p in contextIn) context[p] = p === "access" ? {} : contextIn[p];
        for (var p in contextIn.access) context.access[p] = contextIn.access[p];
        context.addInitializer = function (f) { if (done) throw new TypeError("Cannot add initializers after decoration has completed"); extraInitializers.push(accept(f || null)); };
        var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
        if (kind === "accessor") {
            if (result === void 0) continue;
            if (result === null || typeof result !== "object") throw new TypeError("Object expected");
            if (_ = accept(result.get)) descriptor.get = _;
            if (_ = accept(result.set)) descriptor.set = _;
            if (_ = accept(result.init)) initializers.unshift(_);
        }
        else if (_ = accept(result)) {
            if (kind === "field") initializers.unshift(_);
            else descriptor[key] = _;
        }
    }
    if (target) Object.defineProperty(target, contextIn.name, descriptor);
    done = true;
};
var __runInitializers = (this && this.__runInitializers) || function (thisArg, initializers, value) {
    var useValue = arguments.length > 2;
    for (var i = 0; i < initializers.length; i++) {
        value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
    }
    return useValue ? value : void 0;
};
import { Root } from '@a2ui/lit/ui';
import { html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import { Chart, BarController, CategoryScale, LinearScale, BarElement, LineController, LineElement, PointElement, Tooltip, Legend, Title, Filler } from 'chart.js';
Chart.register(BarController, CategoryScale, LinearScale, BarElement, LineController, LineElement, PointElement, Tooltip, Legend, Title, Filler);
const DEFAULT_COLORS = [
    '#4285F4', '#EA4335', '#FBBC04', '#34A853', '#FF6D01',
    '#46BDC6', '#7B61FF', '#F538A0', '#A8DAB5', '#FFD54F',
    '#4DD0E1', '#BA68C8', '#FF8A65', '#81C784', '#64B5F6',
];
let BarChartView = (() => {
    let _classDecorators = [customElement('a2ui-bar-chart')];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Root;
    let _chartTitle_decorators;
    let _chartTitle_initializers = [];
    let _chartTitle_extraInitializers = [];
    let _labels_decorators;
    let _labels_initializers = [];
    let _labels_extraInitializers = [];
    let _values_decorators;
    let _values_initializers = [];
    let _values_extraInitializers = [];
    let _colors_decorators;
    let _colors_initializers = [];
    let _colors_extraInitializers = [];
    let _chartType_decorators;
    let _chartType_initializers = [];
    let _chartType_extraInitializers = [];
    var BarChartView = class extends _classSuper {
        static { _classThis = this; }
        constructor() {
            super(...arguments);
            this.#chartTitle_accessor_storage = __runInitializers(this, _chartTitle_initializers, '');
            this.#labels_accessor_storage = (__runInitializers(this, _chartTitle_extraInitializers), __runInitializers(this, _labels_initializers, []));
            this.#values_accessor_storage = (__runInitializers(this, _labels_extraInitializers), __runInitializers(this, _values_initializers, []));
            this.#colors_accessor_storage = (__runInitializers(this, _values_extraInitializers), __runInitializers(this, _colors_initializers, null));
            this.#chartType_accessor_storage = (__runInitializers(this, _colors_extraInitializers), __runInitializers(this, _chartType_initializers, 'bar'));
            this.chart = (__runInitializers(this, _chartType_extraInitializers), null);
        }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _chartTitle_decorators = [property({ type: Object })];
            _labels_decorators = [property({ type: Object })];
            _values_decorators = [property({ type: Object })];
            _colors_decorators = [property({ type: Object })];
            _chartType_decorators = [property({ type: Object })];
            __esDecorate(this, null, _chartTitle_decorators, { kind: "accessor", name: "chartTitle", static: false, private: false, access: { has: obj => "chartTitle" in obj, get: obj => obj.chartTitle, set: (obj, value) => { obj.chartTitle = value; } }, metadata: _metadata }, _chartTitle_initializers, _chartTitle_extraInitializers);
            __esDecorate(this, null, _labels_decorators, { kind: "accessor", name: "labels", static: false, private: false, access: { has: obj => "labels" in obj, get: obj => obj.labels, set: (obj, value) => { obj.labels = value; } }, metadata: _metadata }, _labels_initializers, _labels_extraInitializers);
            __esDecorate(this, null, _values_decorators, { kind: "accessor", name: "values", static: false, private: false, access: { has: obj => "values" in obj, get: obj => obj.values, set: (obj, value) => { obj.values = value; } }, metadata: _metadata }, _values_initializers, _values_extraInitializers);
            __esDecorate(this, null, _colors_decorators, { kind: "accessor", name: "colors", static: false, private: false, access: { has: obj => "colors" in obj, get: obj => obj.colors, set: (obj, value) => { obj.colors = value; } }, metadata: _metadata }, _colors_initializers, _colors_extraInitializers);
            __esDecorate(this, null, _chartType_decorators, { kind: "accessor", name: "chartType", static: false, private: false, access: { has: obj => "chartType" in obj, get: obj => obj.chartType, set: (obj, value) => { obj.chartType = value; } }, metadata: _metadata }, _chartType_initializers, _chartType_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            BarChartView = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        #chartTitle_accessor_storage;
        get chartTitle() { return this.#chartTitle_accessor_storage; }
        set chartTitle(value) { this.#chartTitle_accessor_storage = value; }
        #labels_accessor_storage;
        get labels() { return this.#labels_accessor_storage; }
        set labels(value) { this.#labels_accessor_storage = value; }
        #values_accessor_storage;
        get values() { return this.#values_accessor_storage; }
        set values(value) { this.#values_accessor_storage = value; }
        #colors_accessor_storage;
        get colors() { return this.#colors_accessor_storage; }
        set colors(value) { this.#colors_accessor_storage = value; }
        #chartType_accessor_storage;
        get chartType() { return this.#chartType_accessor_storage; } // 'bar' or 'line'
        set chartType(value) { this.#chartType_accessor_storage = value; }
        unpack(val) {
            if (!val)
                return val;
            if (val.literalString !== undefined)
                return val.literalString;
            if (val.literalNumber !== undefined)
                return val.literalNumber;
            if (val.literalBoolean !== undefined)
                return val.literalBoolean;
            if (val.literalArray !== undefined) {
                return val.literalArray.map((v) => this.unpack(v));
            }
            return val;
        }
        get unpackedTitle() {
            return this.unpack(this.chartTitle) || '';
        }
        get unpackedLabels() {
            return this.unpack(this.labels) || [];
        }
        get unpackedValues() {
            return (this.unpack(this.values) || []).map(Number);
        }
        get unpackedColors() {
            const c = this.unpack(this.colors);
            return c && c.length > 0 ? c : DEFAULT_COLORS;
        }
        get unpackedChartType() {
            const t = this.unpack(this.chartType);
            return t === 'line' ? 'line' : 'bar';
        }
        static { this.styles = [
            ...Root.styles,
            css `
      :host {
        display: block;
        padding: 16px;
        font-family: 'Roboto', sans-serif;
      }

      .chart-container {
        width: 100%;
        max-width: 600px;
        margin: 0 auto;
        padding: 20px;
        background: #1e1e1e;
        border: 1px solid #444444;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
      }

      .chart-title {
        font-size: 1.2rem;
        font-weight: 500;
        color: #e8eaed;
        margin-bottom: 12px;
      }

      canvas {
        width: 100% !important;
        height: auto !important;
      }
    `
        ]; }
        render() {
            const title = this.unpackedTitle;
            return html `
      ${title ? html `<div class="chart-title">${title}</div>` : ''}
      <div class="chart-container">
        <canvas id="barCanvas"></canvas>
      </div>
    `;
        }
        updated() {
            this.renderChart();
        }
        renderChart() {
            const labels = this.unpackedLabels;
            const values = this.unpackedValues;
            const colors = this.unpackedColors;
            const type = this.unpackedChartType;
            if (!labels.length || !values.length)
                return;
            const canvas = this.renderRoot.querySelector('#barCanvas');
            if (!canvas)
                return;
            if (this.chart) {
                this.chart.destroy();
                this.chart = null;
            }
            const bgColors = labels.map((_, i) => colors[i % colors.length]);
            const borderColors = bgColors;
            const datasetConfig = type === 'line'
                ? {
                    data: values,
                    borderColor: colors[0],
                    backgroundColor: colors[0] + '33', // 20% opacity fill
                    borderWidth: 2.5,
                    pointBackgroundColor: colors[0],
                    pointBorderColor: '#1e1e1e',
                    pointBorderWidth: 2,
                    pointRadius: 4,
                    pointHoverRadius: 6,
                    fill: true,
                    tension: 0.3,
                }
                : {
                    data: values,
                    backgroundColor: bgColors,
                    borderColor: borderColors,
                    borderWidth: 1,
                    borderRadius: 4,
                };
            this.chart = new Chart(canvas, {
                type,
                data: {
                    labels,
                    datasets: [{ ...datasetConfig, label: this.unpackedTitle }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: true,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: '#303030',
                            titleColor: '#e8eaed',
                            bodyColor: '#e0e0e0',
                            borderColor: '#555',
                            borderWidth: 1,
                        },
                    },
                    scales: {
                        x: {
                            ticks: { color: '#9aa0a6', font: { size: 11 } },
                            grid: { color: 'rgba(255,255,255,0.06)' },
                        },
                        y: {
                            ticks: { color: '#9aa0a6', font: { size: 11 } },
                            grid: { color: 'rgba(255,255,255,0.06)' },
                            beginAtZero: true,
                        },
                    },
                },
            });
        }
        disconnectedCallback() {
            super.disconnectedCallback();
            if (this.chart) {
                this.chart.destroy();
                this.chart = null;
            }
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    };
    return BarChartView = _classThis;
})();
export { BarChartView };
//# sourceMappingURL=bar-chart.js.map
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

@customElement('a2ui-bar-chart')
export class BarChartView extends Root {
  @property({ type: Object }) accessor chartTitle: any = '';
  @property({ type: Object }) accessor labels: any = [];
  @property({ type: Object }) accessor values: any = [];
  @property({ type: Object }) accessor colors: any = null;
  @property({ type: Object }) accessor chartType: any = 'bar'; // 'bar' or 'line'

  private chart: Chart | null = null;

  private unpack(val: any): any {
    if (!val) return val;
    if (val.literalString !== undefined) return val.literalString;
    if (val.literalNumber !== undefined) return val.literalNumber;
    if (val.literalBoolean !== undefined) return val.literalBoolean;
    if (val.literalArray !== undefined) {
      return val.literalArray.map((v: any) => this.unpack(v));
    }
    return val;
  }

  get unpackedTitle(): string {
    return this.unpack(this.chartTitle) || '';
  }

  get unpackedLabels(): string[] {
    return this.unpack(this.labels) || [];
  }

  get unpackedValues(): number[] {
    return (this.unpack(this.values) || []).map(Number);
  }

  get unpackedColors(): string[] {
    const c = this.unpack(this.colors);
    return c && c.length > 0 ? c : DEFAULT_COLORS;
  }

  get unpackedChartType(): 'bar' | 'line' {
    const t = this.unpack(this.chartType);
    return t === 'line' ? 'line' : 'bar';
  }

  static styles = [
    ...Root.styles,
    css`
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
  ];

  render() {
    const title = this.unpackedTitle;
    return html`
      ${title ? html`<div class="chart-title">${title}</div>` : ''}
      <div class="chart-container">
        <canvas id="barCanvas"></canvas>
      </div>
    `;
  }

  updated() {
    this.renderChart();
  }

  private renderChart() {
    const labels = this.unpackedLabels;
    const values = this.unpackedValues;
    const colors = this.unpackedColors;
    const type = this.unpackedChartType;

    if (!labels.length || !values.length) return;

    const canvas = this.renderRoot.querySelector('#barCanvas') as HTMLCanvasElement;
    if (!canvas) return;

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
}

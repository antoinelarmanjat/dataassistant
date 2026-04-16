import { Root } from '@a2ui/lit/ui';
export declare class BarChartView extends Root {
    accessor chartTitle: any;
    accessor labels: any;
    accessor values: any;
    accessor colors: any;
    accessor chartType: any;
    private chart;
    private unpack;
    get unpackedTitle(): string;
    get unpackedLabels(): string[];
    get unpackedValues(): number[];
    get unpackedColors(): string[];
    get unpackedChartType(): 'bar' | 'line';
    static styles: import("lit").CSSResultGroup[];
    render(): import("lit").TemplateResult<1>;
    updated(): void;
    private renderChart;
    disconnectedCallback(): void;
}
//# sourceMappingURL=bar-chart.d.ts.map
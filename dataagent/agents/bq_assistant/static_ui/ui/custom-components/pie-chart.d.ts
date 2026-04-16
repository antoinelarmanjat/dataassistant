import { Root } from '@a2ui/lit/ui';
export declare class PieChartView extends Root {
    accessor chartTitle: any;
    accessor labels: any;
    accessor values: any;
    accessor colors: any;
    private chart;
    private unpack;
    get unpackedTitle(): string;
    get unpackedLabels(): string[];
    get unpackedValues(): number[];
    get unpackedColors(): string[];
    static styles: import("lit").CSSResultGroup[];
    render(): import("lit").TemplateResult<1>;
    updated(): void;
    private renderChart;
    disconnectedCallback(): void;
}
//# sourceMappingURL=pie-chart.d.ts.map
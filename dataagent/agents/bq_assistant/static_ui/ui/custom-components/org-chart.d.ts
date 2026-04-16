import { Root } from '@a2ui/lit/ui';
import { v0_8 } from '@a2ui/lit';
import { TemplateResult } from 'lit';
type Action = v0_8.Types.Action;
export interface OrgChartNode {
    title: string;
    name: string;
}
export declare class OrgChart extends Root {
    accessor chain: OrgChartNode[];
    accessor action: Action | null;
    static styles: import("lit").CSSResultGroup[];
    render(): TemplateResult<1>;
    private handleNodeClick;
}
export {};
//# sourceMappingURL=org-chart.d.ts.map
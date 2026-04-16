import { Root } from '@a2ui/lit/ui';
import { v0_8 } from '@a2ui/lit';
type Action = v0_8.Types.Action;
export declare class TableView extends Root {
    accessor tableTitle: any;
    accessor headers: any;
    accessor rows: any;
    accessor action: Action | null;
    private unpack;
    get unpackedTitle(): string;
    get unpackedHeaders(): string[];
    get unpackedRows(): any[][];
    static styles: import("lit").CSSResultGroup[];
    render(): import("lit").TemplateResult<1>;
    private handleCellClick;
}
export {};
//# sourceMappingURL=table-view.d.ts.map
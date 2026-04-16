import { LitElement, HTMLTemplateResult } from "lit";
import { SnackbarAction, SnackbarUUID, SnackType } from "./types/types.js";
import { v0_8 } from "@a2ui/lit";
import "./ui/ui.js";
declare const A2UIContactFinder_base: typeof LitElement;
export declare class A2UIContactFinder extends A2UIContactFinder_base {
    #private;
    connectedCallback(): void;
    accessor theme: v0_8.Types.Theme;
    accessor markdownRenderer: v0_8.Types.MarkdownRenderer;
    accessor renderVersion: number;
    static styles: import("lit").CSSResult[];
    render(): import("lit").TemplateResult<1>;
    updated(changedProperties: Map<string, unknown>): void;
    snackbar(message: string | HTMLTemplateResult, type: SnackType, actions?: SnackbarAction[], persistent?: boolean, id?: `${string}-${string}-${string}-${string}-${string}`, replaceAll?: boolean): `${string}-${string}-${string}-${string}-${string}`;
    unsnackbar(id?: SnackbarUUID): void;
}
export {};
//# sourceMappingURL=contact.d.ts.map
/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Root } from "@a2ui/lit/ui";
export declare class McpApp extends Root {
    static styles: import("lit").CSSResultGroup[];
    accessor resourceUri: string;
    accessor htmlContent: string;
    accessor height: number | undefined;
    accessor allowedTools: string[];
    accessor iframe: HTMLIFrameElement;
    private bridge?;
    render(): import("lit").TemplateResult<1>;
    updated(changedProperties: Map<string, any>): void;
    disconnectedCallback(): void;
    private initializeSandbox;
    private dispatchAgentAction;
}
//# sourceMappingURL=mcp-apps-component.d.ts.map
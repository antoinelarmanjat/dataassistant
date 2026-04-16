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
import { html, css } from "lit";
import { customElement, property, query } from "lit/decorators.js";
import { Root } from "@a2ui/lit/ui";
import { v0_8 } from "@a2ui/lit";
import { AppBridge, PostMessageTransport } from "@modelcontextprotocol/ext-apps/app-bridge";
import { SANDBOX_IFRAME_PATH } from "../shared-constants.js";
let McpApp = (() => {
    let _classDecorators = [customElement("a2ui-mcp-apps-component")];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Root;
    let _resourceUri_decorators;
    let _resourceUri_initializers = [];
    let _resourceUri_extraInitializers = [];
    let _htmlContent_decorators;
    let _htmlContent_initializers = [];
    let _htmlContent_extraInitializers = [];
    let _height_decorators;
    let _height_initializers = [];
    let _height_extraInitializers = [];
    let _allowedTools_decorators;
    let _allowedTools_initializers = [];
    let _allowedTools_extraInitializers = [];
    let _iframe_decorators;
    let _iframe_initializers = [];
    let _iframe_extraInitializers = [];
    var McpApp = class extends _classSuper {
        static { _classThis = this; }
        constructor() {
            super(...arguments);
            this.#resourceUri_accessor_storage = __runInitializers(this, _resourceUri_initializers, "");
            this.#htmlContent_accessor_storage = (__runInitializers(this, _resourceUri_extraInitializers), __runInitializers(this, _htmlContent_initializers, ""));
            this.#height_accessor_storage = (__runInitializers(this, _htmlContent_extraInitializers), __runInitializers(this, _height_initializers, undefined));
            this.#allowedTools_accessor_storage = (__runInitializers(this, _height_extraInitializers), __runInitializers(this, _allowedTools_initializers, []));
            this.#iframe_accessor_storage = (__runInitializers(this, _allowedTools_extraInitializers), __runInitializers(this, _iframe_initializers, void 0));
            this.bridge = __runInitializers(this, _iframe_extraInitializers);
        }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _resourceUri_decorators = [property({ type: String })];
            _htmlContent_decorators = [property({ type: String })];
            _height_decorators = [property({ type: Number })];
            _allowedTools_decorators = [property({ type: Array })];
            _iframe_decorators = [query("iframe")];
            __esDecorate(this, null, _resourceUri_decorators, { kind: "accessor", name: "resourceUri", static: false, private: false, access: { has: obj => "resourceUri" in obj, get: obj => obj.resourceUri, set: (obj, value) => { obj.resourceUri = value; } }, metadata: _metadata }, _resourceUri_initializers, _resourceUri_extraInitializers);
            __esDecorate(this, null, _htmlContent_decorators, { kind: "accessor", name: "htmlContent", static: false, private: false, access: { has: obj => "htmlContent" in obj, get: obj => obj.htmlContent, set: (obj, value) => { obj.htmlContent = value; } }, metadata: _metadata }, _htmlContent_initializers, _htmlContent_extraInitializers);
            __esDecorate(this, null, _height_decorators, { kind: "accessor", name: "height", static: false, private: false, access: { has: obj => "height" in obj, get: obj => obj.height, set: (obj, value) => { obj.height = value; } }, metadata: _metadata }, _height_initializers, _height_extraInitializers);
            __esDecorate(this, null, _allowedTools_decorators, { kind: "accessor", name: "allowedTools", static: false, private: false, access: { has: obj => "allowedTools" in obj, get: obj => obj.allowedTools, set: (obj, value) => { obj.allowedTools = value; } }, metadata: _metadata }, _allowedTools_initializers, _allowedTools_extraInitializers);
            __esDecorate(this, null, _iframe_decorators, { kind: "accessor", name: "iframe", static: false, private: false, access: { has: obj => "iframe" in obj, get: obj => obj.iframe, set: (obj, value) => { obj.iframe = value; } }, metadata: _metadata }, _iframe_initializers, _iframe_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            McpApp = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static { this.styles = [
            ...Root.styles,
            css `
      :host {
        display: block;
        width: 100%;
        border: 1px solid var(--p-60, #eee);
        position: relative;
        overflow: hidden; /* For Aspect Ratio / Container */
        border-radius: 8px;
        background: #fff;
      }
      iframe {
        width: 100%;
        height: 100%;
        border: none;
        background: #f5f5f5;
        transition: height 0.3s ease-out, min-width 0.3s ease-out;
      }
    `,
        ]; }
        #resourceUri_accessor_storage;
        /* --- Properties (Server Contract) --- */
        get resourceUri() { return this.#resourceUri_accessor_storage; }
        set resourceUri(value) { this.#resourceUri_accessor_storage = value; }
        #htmlContent_accessor_storage;
        get htmlContent() { return this.#htmlContent_accessor_storage; }
        set htmlContent(value) { this.#htmlContent_accessor_storage = value; }
        #height_accessor_storage;
        get height() { return this.#height_accessor_storage; }
        set height(value) { this.#height_accessor_storage = value; }
        #allowedTools_accessor_storage;
        get allowedTools() { return this.#allowedTools_accessor_storage; }
        set allowedTools(value) { this.#allowedTools_accessor_storage = value; }
        #iframe_accessor_storage;
        // --- Internal State ---
        get iframe() { return this.#iframe_accessor_storage; }
        set iframe(value) { this.#iframe_accessor_storage = value; }
        render() {
            // Default to aspect ratio if no height. Use 16:9 or 4:3.
            const style = this.height ? `height: ${this.height}px;` : 'aspect-ratio: 4/3;';
            return html `
      <div style="position: relative; width: 100%; ${style}">
        <iframe
          id="mcp-sandbox"
          referrerpolicy="origin"
          sandbox="allow-scripts allow-forms allow-popups allow-modals allow-same-origin"
        ></iframe>
      </div>
    `;
        }
        updated(changedProperties) {
            super.updated(changedProperties);
            if (!this.bridge && this.htmlContent && this.iframe) {
                this.initializeSandbox();
            }
        }
        disconnectedCallback() {
            if (this.bridge) {
                this.bridge.close();
                this.bridge = undefined;
            }
            super.disconnectedCallback();
        }
        async initializeSandbox() {
            if (!this.iframe || !this.htmlContent)
                return;
            // Allow configuring the sandbox URL via env var for production deployment
            // Fall back to the 127.0.0.1 trick for local development to simulate cross-origin isolation
            const meta = import.meta;
            const configuredSandboxUrl = meta && meta.env ? meta.env.VITE_MCP_SANDBOX_URL : undefined;
            const sandboxOrigin = configuredSandboxUrl || `http://127.0.0.1:${window.location.port}${SANDBOX_IFRAME_PATH}`;
            const sandboxUrl = new URL(sandboxOrigin);
            // Set up the bridge. No MCP client needed because A2UI acts as the orchestrator.
            this.bridge = new AppBridge(null, { name: "A2UI Client Host", version: "1.0.0" }, {
                serverTools: {},
                updateModelContext: { text: {} },
            }, {
                hostContext: {
                    theme: "light",
                    platform: "web",
                    displayMode: "inline",
                }
            });
            this.bridge.onsizechange = ({ width, height }) => {
                // Allow the view to dynamically resize the iframe container
                const from = {};
                const to = {};
                if (width !== undefined) {
                    from.minWidth = `${this.iframe.offsetWidth}px`;
                    this.iframe.style.minWidth = to.minWidth = `min(${width}px, 100%)`;
                }
                if (height !== undefined) {
                    from.height = `${this.iframe.offsetHeight}px`;
                    this.iframe.style.height = to.height = `${height}px`;
                }
                this.iframe.animate([from, to], { duration: 300, easing: "ease-out" });
            };
            // Forward Tool Calls to the A2UI Action Dispatch
            this.bridge.oncalltool = async (params) => {
                const actionName = params.name;
                const args = params.arguments || {};
                if (this.allowedTools.includes(actionName)) {
                    this.dispatchAgentAction(actionName, args);
                    return { content: [{ type: "text", text: "Action dispatched to A2UI Agent" }] };
                }
                else {
                    console.warn(`[McpApp] Tool '${actionName}' blocked.`);
                    throw new Error("Tool not allowed");
                }
            };
            this.bridge.onloggingmessage = (params) => {
                console.log(`[MCP Sandbox ${params.level}]:`, params.data);
            };
            // 1. Listen for the Outer Iframe to declare itself ready.
            const readyNotification = "ui/notifications/sandbox-proxy-ready";
            const proxyReady = new Promise((resolve) => {
                const listener = ({ source, data, origin }) => {
                    if (source === this.iframe.contentWindow && origin === sandboxUrl.origin && data?.method === readyNotification) {
                        window.removeEventListener("message", listener);
                        resolve(true);
                    }
                };
                window.addEventListener("message", listener);
            });
            // 2. Load the proxy iframe.
            this.iframe.src = sandboxUrl.href;
            await proxyReady;
            // 3. Connect AppBridge via PostMessage transport.
            // We pass iframe.contentWindow to target just the sandbox proxy.
            await this.bridge.connect(new PostMessageTransport(this.iframe.contentWindow, this.iframe.contentWindow));
            // 4. Send the Inner HTML UI resource to the sandbox to spin up the actual app.
            await this.bridge.sendSandboxResourceReady({
                html: this.htmlContent,
                sandbox: "allow-scripts allow-forms allow-popups allow-modals allow-same-origin"
            });
        }
        dispatchAgentAction(actionName, params) {
            const context = [];
            if (params && typeof params === 'object') {
                for (const [key, value] of Object.entries(params)) {
                    if (typeof value === "string") {
                        context.push({ key, value: { literalString: value } });
                    }
                    else if (typeof value === "number") {
                        context.push({ key, value: { literalNumber: value } });
                    }
                    else if (typeof value === "boolean") {
                        context.push({ key, value: { literalBoolean: value } });
                    }
                    else if (value !== null && typeof value === 'object') {
                        context.push({ key, value: { literalString: JSON.stringify(value) } });
                    }
                }
            }
            const action = {
                name: actionName,
                context,
            };
            const eventPayload = {
                eventType: "a2ui.action",
                action,
                sourceComponentId: this.id,
                dataContextPath: this.dataContextPath,
                sourceComponent: this.component,
            };
            this.dispatchEvent(new v0_8.Events.StateEvent(eventPayload));
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    };
    return McpApp = _classThis;
})();
export { McpApp };
//# sourceMappingURL=mcp-apps-component.js.map
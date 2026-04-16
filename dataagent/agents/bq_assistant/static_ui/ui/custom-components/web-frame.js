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
import { ifDefined } from "lit/directives/if-defined.js";
import { Root } from "@a2ui/lit/ui";
import { v0_8 } from "@a2ui/lit";
let WebFrame = (() => {
    var _a;
    let _classDecorators = [customElement("a2ui-web-frame")];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Root;
    let _url_decorators;
    let _url_initializers = [];
    let _url_extraInitializers = [];
    let _html_decorators;
    let _html_initializers = [];
    let _html_extraInitializers = [];
    let _height_decorators;
    let _height_initializers = [];
    let _height_extraInitializers = [];
    let _interactionMode_decorators;
    let _interactionMode_initializers = [];
    let _interactionMode_extraInitializers = [];
    let _allowedEvents_decorators;
    let _allowedEvents_initializers = [];
    let _allowedEvents_extraInitializers = [];
    let _iframe_decorators;
    let _iframe_initializers = [];
    let _iframe_extraInitializers = [];
    var WebFrame = class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _url_decorators = [property({ type: String })];
            _html_decorators = [property({ type: String })];
            _height_decorators = [property({ type: Number })];
            _interactionMode_decorators = [property({ type: String })];
            _allowedEvents_decorators = [property({ type: Array })];
            _iframe_decorators = [query("iframe")];
            __esDecorate(this, null, _url_decorators, { kind: "accessor", name: "url", static: false, private: false, access: { has: obj => "url" in obj, get: obj => obj.url, set: (obj, value) => { obj.url = value; } }, metadata: _metadata }, _url_initializers, _url_extraInitializers);
            __esDecorate(this, null, _html_decorators, { kind: "accessor", name: "html", static: false, private: false, access: { has: obj => "html" in obj, get: obj => obj.html, set: (obj, value) => { obj.html = value; } }, metadata: _metadata }, _html_initializers, _html_extraInitializers);
            __esDecorate(this, null, _height_decorators, { kind: "accessor", name: "height", static: false, private: false, access: { has: obj => "height" in obj, get: obj => obj.height, set: (obj, value) => { obj.height = value; } }, metadata: _metadata }, _height_initializers, _height_extraInitializers);
            __esDecorate(this, null, _interactionMode_decorators, { kind: "accessor", name: "interactionMode", static: false, private: false, access: { has: obj => "interactionMode" in obj, get: obj => obj.interactionMode, set: (obj, value) => { obj.interactionMode = value; } }, metadata: _metadata }, _interactionMode_initializers, _interactionMode_extraInitializers);
            __esDecorate(this, null, _allowedEvents_decorators, { kind: "accessor", name: "allowedEvents", static: false, private: false, access: { has: obj => "allowedEvents" in obj, get: obj => obj.allowedEvents, set: (obj, value) => { obj.allowedEvents = value; } }, metadata: _metadata }, _allowedEvents_initializers, _allowedEvents_extraInitializers);
            __esDecorate(this, null, _iframe_decorators, { kind: "accessor", name: "iframe", static: false, private: false, access: { has: obj => "iframe" in obj, get: obj => obj.iframe, set: (obj, value) => { obj.iframe = value; } }, metadata: _metadata }, _iframe_initializers, _iframe_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            WebFrame = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static { this.styles = [
            ...Root.styles,
            css `
      :host {
        display: block;
        width: 100%;
        border: 1px solid #eee;
        position: relative;
        overflow: hidden; /* For Aspect Ratio / Container */
      }
      iframe {
        width: 100%;
        height: 100%;
        border: none;
        background: #f5f5f5;
      }
      .controls {
        position: absolute;
        top: 20px;
        right: 20px;
        display: flex;
        gap: 10px;
        z-index: 10;
      }
      .controls button {
        width: 32px;
        height: 32px;
        font-size: 20px;
        cursor: pointer;
        background: white;
        border: 1px solid #ccc;
        border-radius: 4px;
        display: flex;
        align-items: center;
        justify-content: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      }
      .controls button:hover {
        background: #f0f0f0;
      }
    `,
        ]; }
        #url_accessor_storage = __runInitializers(this, _url_initializers, "");
        /* --- Properties (Server Contract) --- */
        get url() { return this.#url_accessor_storage; }
        set url(value) { this.#url_accessor_storage = value; }
        #html_accessor_storage = (__runInitializers(this, _url_extraInitializers), __runInitializers(this, _html_initializers, ""));
        get html() { return this.#html_accessor_storage; }
        set html(value) { this.#html_accessor_storage = value; }
        #height_accessor_storage = (__runInitializers(this, _html_extraInitializers), __runInitializers(this, _height_initializers, undefined));
        get height() { return this.#height_accessor_storage; }
        set height(value) { this.#height_accessor_storage = value; }
        #interactionMode_accessor_storage = (__runInitializers(this, _height_extraInitializers), __runInitializers(this, _interactionMode_initializers, "readOnly"));
        get interactionMode() { return this.#interactionMode_accessor_storage; }
        set interactionMode(value) { this.#interactionMode_accessor_storage = value; }
        #allowedEvents_accessor_storage = (__runInitializers(this, _interactionMode_extraInitializers), __runInitializers(this, _allowedEvents_initializers, []));
        get allowedEvents() { return this.#allowedEvents_accessor_storage; }
        set allowedEvents(value) { this.#allowedEvents_accessor_storage = value; }
        #iframe_accessor_storage = (__runInitializers(this, _allowedEvents_extraInitializers), __runInitializers(this, _iframe_initializers, void 0));
        // --- Internal State ---
        get iframe() { return this.#iframe_accessor_storage; }
        set iframe(value) { this.#iframe_accessor_storage = value; }
        // --- Security Constants ---
        static { this.TRUSTED_DOMAINS = [
            "localhost",
            "127.0.0.1",
            "openstreetmap.org",
            "youtube.com",
            "maps.google.com"
        ]; }
        render() {
            const sandboxAttr = this.#calculateSandbox();
            // Default to aspect ratio if no height. Use 16:9 or 4:3.
            const style = this.height ? `height: ${this.height}px;` : 'aspect-ratio: 4/3;';
            // Determine content: srcdoc (html) vs src (url)
            const srcRaw = this.url;
            // VERY IMPORTANT: If html is empty, do NOT pass it to srcdoc, otherwise it overrides src with blank page.
            const srcDocRaw = this.html || undefined;
            return html `
      <div style="position: relative; width: 100%; ${style}">
        <div class="controls">
          <button @click="${() => this.#zoom(1.2)}">+</button>
          <button @click="${() => this.#zoom(0.8)}">-</button>
        </div>
        <iframe
          src="${srcRaw}"
          srcdoc="${ifDefined(srcDocRaw)}"
          sandbox="${sandboxAttr}"
          referrerpolicy="no-referrer"
        ></iframe>
      </div>
    `;
        }
        #calculateSandbox() {
            // 1. If HTML is provided, it's treated as Trusted (but isolated)
            if (this.html) {
                if (this.interactionMode === 'interactive') {
                    return "allow-scripts allow-forms allow-popups allow-modals";
                }
                return "allow-scripts"; // ReadOnly but scripts allowed for rendering
            }
            // 2. Parse Domain from URL
            try {
                const urlObj = new URL(this.url, window.location.href); // Handle relative URLs too
                const hostname = urlObj.hostname;
                const isTrusted = WebFrame.TRUSTED_DOMAINS.some(d => hostname === d || hostname.endsWith(`.${d}`));
                if (!isTrusted) {
                    // Untrusted: Strict Lockdown
                    return "";
                }
                // Trusted
                // Always allow same-origin for trusted domains to avoid issues with local assets or CORS checks
                if (this.interactionMode === 'interactive') {
                    return "allow-scripts allow-forms allow-popups allow-modals allow-same-origin";
                }
                else {
                    return "allow-scripts allow-same-origin";
                }
            }
            catch (e) {
                // Invalid URL -> Lockdown
                return "";
            }
        }
        // --- Event Bridge ---
        firstUpdated() {
            window.addEventListener("message", this.#onMessage);
        }
        disconnectedCallback() {
            window.removeEventListener("message", this.#onMessage);
            super.disconnectedCallback();
        }
        #onMessage = (__runInitializers(this, _iframe_extraInitializers), (event) => {
            // In production, verify event.origin matches this.src origin (if not opaque).
            const data = event.data;
            // Spec Protocol: { type: 'a2ui_action', action: '...', data: ... }
            if (data && data.type === 'a2ui_action') {
                const { action, data: actionData } = data; // 'data' property in message payload
                // 1. Validate Action
                if (this.allowedEvents.includes(action)) {
                    // 2. Dispatch
                    this.#dispatchAgentAction(action, actionData);
                }
                else {
                    console.warn(`[WebFrame] Action '${action}' blocked. Not in allowedEvents:`, this.allowedEvents);
                }
            }
            // Legacy support for 'emit' temporarily if we want to be safe, but spec implies replacement.
            // I will remove legacy to be strict.
        });
        #dispatchAgentAction(actionName, params) {
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
        // --- Zoom Controls (External) ---
        // Keeps working by sending 'zoom' to iframe.
        // We assume the iframe content knows how to handle 'zoom' message if it supports it.
        #zoom(factor) {
            if (this.iframe && this.iframe.contentWindow) {
                this.iframe.contentWindow.postMessage({ type: 'zoom', payload: { factor } }, '*');
            }
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    };
    return WebFrame = _classThis;
})();
export { WebFrame };
//# sourceMappingURL=web-frame.js.map
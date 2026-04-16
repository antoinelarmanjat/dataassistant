/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import { Root } from '@a2ui/lit/ui';
import { html, css } from 'lit';
import { property } from 'lit/decorators.js';
let PremiumTextField = (() => {
    let _classSuper = Root;
    let _label_decorators;
    let _label_initializers = [];
    let _label_extraInitializers = [];
    let _text_decorators;
    let _text_initializers = [];
    let _text_extraInitializers = [];
    return class PremiumTextField extends _classSuper {
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _label_decorators = [property()];
            _text_decorators = [property()];
            __esDecorate(this, null, _label_decorators, { kind: "accessor", name: "label", static: false, private: false, access: { has: obj => "label" in obj, get: obj => obj.label, set: (obj, value) => { obj.label = value; } }, metadata: _metadata }, _label_initializers, _label_extraInitializers);
            __esDecorate(this, null, _text_decorators, { kind: "accessor", name: "text", static: false, private: false, access: { has: obj => "text" in obj, get: obj => obj.text, set: (obj, value) => { obj.text = value; } }, metadata: _metadata }, _text_initializers, _text_extraInitializers);
            if (_metadata) Object.defineProperty(this, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        #label_accessor_storage = __runInitializers(this, _label_initializers, '');
        get label() { return this.#label_accessor_storage; }
        set label(value) { this.#label_accessor_storage = value; }
        #text_accessor_storage = (__runInitializers(this, _label_extraInitializers), __runInitializers(this, _text_initializers, ''));
        get text() { return this.#text_accessor_storage; }
        set text(value) { this.#text_accessor_storage = value; }
        static { this.styles = [
            ...Root.styles,
            css `
      :host {
        display: block;
        padding: 16px;
        background: #fff;
        border-radius: 12px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.08);
        border: 1px solid #e0e0e0;
        transition: all 0.2s ease;
        font-family: 'Inter', sans-serif;
      }
      :host(:hover) {
        transform: translateY(-2px);
        box-shadow: 0 6px 16px rgba(0,0,0,0.12);
      }
      .input-container {
        position: relative;
        margin-top: 8px;
      }
      input {
        width: 100%;
        padding: 12px 16px;
        font-size: 16px;
        border: 2px solid #e0e0e0;
        border-radius: 8px;
        outline: none;
        transition: border-color 0.2s;
        box-sizing: border-box;
        background: #fafafa;
      }
      input:focus {
        border-color: #6200ee;
        background: #fff;
      }
      label {
        display: block;
        font-size: 14px;
        font-weight: 600;
        color: #333;
        margin-bottom: 4px;
      }
      .hint {
        margin-top: 8px;
        font-size: 12px;
        color: #666;
        display: flex;
        align-items: center;
        gap: 4px;
      }
      .badge {
        background: #6200ee;
        color: white;
        padding: 2px 6px;
        border-radius: 4px;
        font-size: 10px;
        font-weight: bold;
        text-transform: uppercase;
      }
    `
        ]; }
        render() {
            return html `
      <label>${this.label}</label>
      <div class="input-container">
        <input type="text" .value="${this.text}" placeholder="Type here...">
      </div>
      <div class="hint">
        <span class="badge">Custom</span>
        <span>This is a premium override of the standard TextField.</span>
      </div>
    `;
        }
        constructor() {
            super(...arguments);
            __runInitializers(this, _text_extraInitializers);
        }
    };
})();
export { PremiumTextField };
//# sourceMappingURL=premium-text-field.js.map
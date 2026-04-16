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
import { v0_8 } from '@a2ui/lit';
import { html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import { map } from 'lit/directives/map.js';
// Use aliases for convenience
const StateEvent = v0_8.Events.StateEvent;
let OrgChart = (() => {
    let _classDecorators = [customElement('org-chart')];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Root;
    let _chain_decorators;
    let _chain_initializers = [];
    let _chain_extraInitializers = [];
    let _action_decorators;
    let _action_initializers = [];
    let _action_extraInitializers = [];
    var OrgChart = class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _chain_decorators = [property({ type: Array })];
            _action_decorators = [property({ type: Object })];
            __esDecorate(this, null, _chain_decorators, { kind: "accessor", name: "chain", static: false, private: false, access: { has: obj => "chain" in obj, get: obj => obj.chain, set: (obj, value) => { obj.chain = value; } }, metadata: _metadata }, _chain_initializers, _chain_extraInitializers);
            __esDecorate(this, null, _action_decorators, { kind: "accessor", name: "action", static: false, private: false, access: { has: obj => "action" in obj, get: obj => obj.action, set: (obj, value) => { obj.action = value; } }, metadata: _metadata }, _action_initializers, _action_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            OrgChart = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        #chain_accessor_storage = __runInitializers(this, _chain_initializers, []);
        get chain() { return this.#chain_accessor_storage; }
        set chain(value) { this.#chain_accessor_storage = value; }
        #action_accessor_storage = (__runInitializers(this, _chain_extraInitializers), __runInitializers(this, _action_initializers, null));
        get action() { return this.#action_accessor_storage; }
        set action(value) { this.#action_accessor_storage = value; }
        static { this.styles = [
            ...Root.styles,
            css `
    :host {
      display: block;
      padding: 16px;
      font-family: 'Roboto', sans-serif;
    }

    .container {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 16px;
    }

    .node {
      display: flex;
      flex-direction: column;
      align-items: center;
      padding: 12px 24px;
      background: #fff;
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
      min-width: 200px;
      position: relative;
      transition: transform 0.2s, box-shadow 0.2s;
      cursor: pointer;
    }

    .node:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }

    .node:focus {
      outline: 2px solid #1a73e8;
      outline-offset: 2px;
    }

    .node.current {
      background: #e8f0fe;
      border-color: #1a73e8;
      border-width: 2px;
    }

    .title {
      font-size: 0.85rem;
      color: #5f6368;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 4px;
    }

    .name {
      font-size: 1.1rem;
      font-weight: 500;
      color: #202124;
    }

    .arrow {
      color: #9aa0a6;
      font-size: 24px;
      line-height: 1;
    }
  `
        ]; }
        render() {
            let chainData = null;
            let unresolvedChain = this.chain;
            // Resolve "chain" if it is a path object
            const chainAsAny = this.chain;
            if (chainAsAny && typeof chainAsAny === 'object' && 'path' in chainAsAny && chainAsAny.path) {
                if (this.processor) {
                    const resolved = this.processor.getData(this.component, chainAsAny.path, this.surfaceId ?? 'default');
                    if (resolved) {
                        unresolvedChain = resolved;
                    }
                }
            }
            if (Array.isArray(unresolvedChain)) {
                chainData = unresolvedChain;
            }
            else if (unresolvedChain instanceof Map) {
                // Handle Map (values are the nodes)
                const entries = Array.from(unresolvedChain.entries());
                entries.sort((a, b) => parseInt(a[0], 10) - parseInt(b[0], 10));
                chainData = entries.map(entry => entry[1]);
            }
            else if (typeof unresolvedChain === 'object' && unresolvedChain !== null) {
                chainData = Object.values(unresolvedChain);
            }
            // Normalize items: model processor converts nested objects to Maps, so we must convert them back
            chainData = (chainData || []).map(node => {
                // Helper to safely get property regardless of type
                const getVal = (k) => {
                    if (node instanceof Map)
                        return node.get(k);
                    return node?.[k];
                };
                return {
                    title: getVal('title') ?? '',
                    name: getVal('name') ?? '',
                };
            });
            if (!chainData || chainData.length === 0) {
                return html `<div class="empty">No hierarchy data</div>`;
            }
            return html `
      <div class="container">
        ${map(chainData, (node, index) => {
                // Use chainData.length, not this.chain.length
                const isLast = index === (chainData?.length ?? 0) - 1;
                return html `
            <button
              class="node ${isLast ? 'current' : ''}"
              @click=${() => this.handleNodeClick(node)}
              aria-label="Select ${node.name} (${node.title})"
            >
              <span class="title">${node.title}</span>
              <span class="name">${node.name}</span>
            </button>
            ${!isLast ? html `<div class="arrow">↓</div>` : ''}
          `;
            })}
      </div>
    `;
        }
        handleNodeClick(node) {
            if (!this.action)
                return;
            // Create a new action with the node's context merged in
            const newContext = [
                ...(this.action.context || []),
                {
                    key: 'clickedNodeTitle',
                    value: { literalString: node.title }
                },
                {
                    key: 'clickedNodeName',
                    value: { literalString: node.name }
                }
            ];
            const actionWithContext = {
                ...this.action,
                context: newContext
            };
            const evt = new StateEvent({
                eventType: "a2ui.action",
                action: actionWithContext,
                dataContextPath: this.dataContextPath,
                sourceComponentId: this.id,
                sourceComponent: this.component,
            });
            this.dispatchEvent(evt);
        }
        constructor() {
            super(...arguments);
            __runInitializers(this, _action_extraInitializers);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    };
    return OrgChart = _classThis;
})();
export { OrgChart };
//# sourceMappingURL=org-chart.js.map
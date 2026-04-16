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
const StateEvent = v0_8.Events.StateEvent;
let TableView = (() => {
    let _classDecorators = [customElement('table-view')];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Root;
    let _tableTitle_decorators;
    let _tableTitle_initializers = [];
    let _tableTitle_extraInitializers = [];
    let _headers_decorators;
    let _headers_initializers = [];
    let _headers_extraInitializers = [];
    let _rows_decorators;
    let _rows_initializers = [];
    let _rows_extraInitializers = [];
    let _action_decorators;
    let _action_initializers = [];
    let _action_extraInitializers = [];
    var TableView = class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            _tableTitle_decorators = [property({ type: Object })];
            _headers_decorators = [property({ type: Object })];
            _rows_decorators = [property({ type: Object })];
            _action_decorators = [property({ type: Object })];
            __esDecorate(this, null, _tableTitle_decorators, { kind: "accessor", name: "tableTitle", static: false, private: false, access: { has: obj => "tableTitle" in obj, get: obj => obj.tableTitle, set: (obj, value) => { obj.tableTitle = value; } }, metadata: _metadata }, _tableTitle_initializers, _tableTitle_extraInitializers);
            __esDecorate(this, null, _headers_decorators, { kind: "accessor", name: "headers", static: false, private: false, access: { has: obj => "headers" in obj, get: obj => obj.headers, set: (obj, value) => { obj.headers = value; } }, metadata: _metadata }, _headers_initializers, _headers_extraInitializers);
            __esDecorate(this, null, _rows_decorators, { kind: "accessor", name: "rows", static: false, private: false, access: { has: obj => "rows" in obj, get: obj => obj.rows, set: (obj, value) => { obj.rows = value; } }, metadata: _metadata }, _rows_initializers, _rows_extraInitializers);
            __esDecorate(this, null, _action_decorators, { kind: "accessor", name: "action", static: false, private: false, access: { has: obj => "action" in obj, get: obj => obj.action, set: (obj, value) => { obj.action = value; } }, metadata: _metadata }, _action_initializers, _action_extraInitializers);
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            TableView = _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        #tableTitle_accessor_storage = __runInitializers(this, _tableTitle_initializers, '');
        get tableTitle() { return this.#tableTitle_accessor_storage; }
        set tableTitle(value) { this.#tableTitle_accessor_storage = value; }
        #headers_accessor_storage = (__runInitializers(this, _tableTitle_extraInitializers), __runInitializers(this, _headers_initializers, []));
        get headers() { return this.#headers_accessor_storage; }
        set headers(value) { this.#headers_accessor_storage = value; }
        #rows_accessor_storage = (__runInitializers(this, _headers_extraInitializers), __runInitializers(this, _rows_initializers, []));
        get rows() { return this.#rows_accessor_storage; }
        set rows(value) { this.#rows_accessor_storage = value; }
        #action_accessor_storage = (__runInitializers(this, _rows_extraInitializers), __runInitializers(this, _action_initializers, null));
        get action() { return this.#action_accessor_storage; }
        set action(value) { this.#action_accessor_storage = value; }
        unpack(val) {
            if (!val)
                return val;
            if (val.literalString !== undefined)
                return val.literalString;
            if (val.literalNumber !== undefined)
                return val.literalNumber;
            if (val.literalBoolean !== undefined)
                return val.literalBoolean;
            if (val.literalArray !== undefined) {
                return val.literalArray.map((v) => this.unpack(v));
            }
            return val;
        }
        get unpackedTitle() {
            return this.unpack(this.tableTitle) || '';
        }
        get unpackedHeaders() {
            return this.unpack(this.headers) || [];
        }
        get unpackedRows() {
            return this.unpack(this.rows) || [];
        }
        static { this.styles = [
            ...Root.styles,
            css `
      :host {
        display: block;
        padding: 16px;
        font-family: 'Roboto', sans-serif;
      }

      .table-container {
        width: 100%;
        overflow-x: auto;
        border: 1px solid #444444;
        border-radius: 8px;
        background: #1e1e1e;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
      }

      .table-title {
        font-size: 1.2rem;
        font-weight: 500;
        color: #e8eaed;
        margin-bottom: 12px;
      }

      table {
        width: 100%;
        border-collapse: collapse;
        text-align: left;
      }

      th, td {
        padding: 10px 14px;
        border-bottom: 1px solid #333333;
        color: #e0e0e0;
      }

      th {
        background-color: #2a2a2a;
        color: #9aa0a6;
        font-weight: 500;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
      }

      tbody tr:last-child td {
        border-bottom: none;
      }

      tbody tr {
        transition: background-color 0.2s;
      }

      tbody tr:hover {
        background-color: #303030;
      }

      .clickable-cell {
        cursor: pointer;
        transition: background-color 0.2s, box-shadow 0.2s;
      }

      .clickable-cell:hover {
        background-color: #3a5c9a;
        color: #ffffff;
      }
    `
        ]; }
        render() {
            const title = this.unpackedTitle;
            const headers = this.unpackedHeaders;
            const rows = this.unpackedRows;
            return html `
      ${title ? html `<div class="table-title">${title}</div>` : ''}
      <div class="table-container">
        <table>
          ${headers && headers.length > 0 ? html `
            <thead>
              <tr>
                ${map(headers, (header) => html `<th>${header}</th>`)}
              </tr>
            </thead>
          ` : ''}
          <tbody>
            ${map(rows || [], (row, rowIndex) => html `
              <tr>
                ${map(row, (cellValue, colIndex) => html `
                  <td 
                    class="${this.action ? 'clickable-cell' : ''}"
                    @click=${() => this.handleCellClick(rowIndex, colIndex, cellValue)}
                  >
                    ${cellValue}
                  </td>
                `)}
              </tr>
            `)}
            ${(!rows || rows.length === 0) ? html `
              <tr>
                <td colspan="${headers ? headers.length : 1}" style="text-align: center; color: #9aa0a6;">
                  No data available
                </td>
              </tr>
            ` : ''}
          </tbody>
        </table>
      </div>
    `;
        }
        handleCellClick(rowIndex, colIndex, cellValue) {
            if (!this.action)
                return;
            const newContext = [
                ...(this.action.context || []),
                {
                    key: 'clickedRowIndex',
                    value: { literalNumber: rowIndex }
                },
                {
                    key: 'clickedColIndex',
                    value: { literalNumber: colIndex }
                },
                {
                    key: 'clickedCellValue',
                    value: { literalString: String(cellValue) }
                }
            ];
            const headers = this.unpackedHeaders;
            if (headers && headers[colIndex]) {
                newContext.push({
                    key: 'clickedColumnName',
                    value: { literalString: headers[colIndex] }
                });
            }
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
    return TableView = _classThis;
})();
export { TableView };
//# sourceMappingURL=table-view.js.map
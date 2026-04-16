import { Root } from '@a2ui/lit/ui';
import { v0_8 } from '@a2ui/lit';
import { html, css } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import { map } from 'lit/directives/map.js';

const StateEvent = v0_8.Events.StateEvent;
type Action = v0_8.Types.Action;

@customElement('table-view')
export class TableView extends Root {
  @property({ type: Object }) accessor tableTitle: any = '';
  @property({ type: Object }) accessor headers: any = [];
  @property({ type: Object }) accessor rows: any = [];
  @property({ type: Object }) accessor action: Action | null = null;
  
  private unpack(val: any): any {
    if (!val) return val;
    if (val.literalString !== undefined) return val.literalString;
    if (val.literalNumber !== undefined) return val.literalNumber;
    if (val.literalBoolean !== undefined) return val.literalBoolean;
    if (val.literalArray !== undefined) {
      return val.literalArray.map((v: any) => this.unpack(v));
    }
    return val;
  }
  
  get unpackedTitle(): string {
    return this.unpack(this.tableTitle) || '';
  }
  
  get unpackedHeaders(): string[] {
    return this.unpack(this.headers) || [];
  }
  
  get unpackedRows(): any[][] {
    return this.unpack(this.rows) || [];
  }

  static styles = [
    ...Root.styles,
    css`
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
  ];

  render() {
    const title = this.unpackedTitle;
    const headers = this.unpackedHeaders;
    const rows = this.unpackedRows;
    
    return html`
      ${title ? html`<div class="table-title">${title}</div>` : ''}
      <div class="table-container">
        <table>
          ${headers && headers.length > 0 ? html`
            <thead>
              <tr>
                ${map(headers, (header) => html`<th>${header}</th>`)}
              </tr>
            </thead>
          ` : ''}
          <tbody>
            ${map(rows || [], (row, rowIndex) => html`
              <tr>
                ${map(row, (cellValue, colIndex) => html`
                  <td 
                    class="${this.action ? 'clickable-cell' : ''}"
                    @click=${() => this.handleCellClick(rowIndex, colIndex, cellValue)}
                  >
                    ${cellValue}
                  </td>
                `)}
              </tr>
            `)}
            ${(!rows || rows.length === 0) ? html`
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

  private handleCellClick(rowIndex: number, colIndex: number, cellValue: any) {
    if (!this.action) return;

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

    const actionWithContext: Action = {
      ...this.action,
      context: newContext as Action['context']
    };

    const evt = new StateEvent<"a2ui.action">({
      eventType: "a2ui.action",
      action: actionWithContext,
      dataContextPath: this.dataContextPath,
      sourceComponentId: this.id,
      sourceComponent: this.component,
    });
    this.dispatchEvent(evt);
  }
}

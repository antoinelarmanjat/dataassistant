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

import { SignalWatcher } from "@lit-labs/signals";
import { provide } from "@lit/context";
import {
  LitElement,
  html,
  css,
  nothing,
  HTMLTemplateResult,
  unsafeCSS,
} from "lit";
import { unsafeHTML } from "lit/directives/unsafe-html.js";
import { until } from "lit/directives/until.js";
import { customElement, state } from "lit/decorators.js";
import { theme as uiTheme } from "./theme/theme.js";
import { A2UIClient } from "./client.js";
import {
  SnackbarAction,
  SnackbarMessage,
  SnackbarUUID,
  SnackType,
} from "./types/types.js";
import { type Snackbar } from "./ui/snackbar.js";
import { repeat } from "lit/directives/repeat.js";
import { v0_8 } from "@a2ui/lit";
import * as UI from "@a2ui/lit/ui";

// Demo elements.
import "./ui/ui.js";
import { registerContactComponents } from "./ui/custom-components/register-components.js";
import { Context } from "@a2ui/lit/ui";
// @ts-ignore
import { renderMarkdown } from "@a2ui/markdown-it";

// Register custom components for the contact app
registerContactComponents();

@customElement("a2ui-contact")
export class A2UIContactFinder extends SignalWatcher(LitElement) {
  // --- Auth state ---
  @state()
  accessor #authUser: {email: string, name: string, picture: string, credential: string} | null = null;

  @state()
  accessor #authMode: 'loading' | 'iap' | 'oauth' = 'loading';

  @state()
  accessor #dataAuthorized: boolean = false;

  connectedCallback() {
    super.connectedCallback();
    this.#detectAuthMode();
  }

  async #detectAuthMode() {
    try {
      const resp = await fetch('/auth/user');
      const data = await resp.json();
      if (data.mode === 'iap') {
        this.#authMode = 'iap';
        this.#dataAuthorized = data.dataAuthorized === true;
        if (data.authenticated && data.email) {
          this.#authUser = {
            email: data.email,
            name: data.email.split('@')[0],
            picture: '',
            credential: '',  // Not needed — IAP handles auth
          };
          console.log('[Auth] IAP mode — authenticated as:', data.email, '| data authorized:', this.#dataAuthorized);
          this.#showGreeting(data.email.split('@')[0]);
        }
      } else {
        // OAuth mode (local dev) — use Google Sign-In as before
        this.#authMode = 'oauth';
        this.#dataAuthorized = true;  // Local dev uses ADC — always authorized
        this.#initGoogleSignIn();
      }
    } catch (e) {
      // If /auth/user fails (e.g. vite dev server without backend), fall back to OAuth
      console.warn('[Auth] Could not detect auth mode, falling back to OAuth:', e);
      this.#authMode = 'oauth';
      this.#dataAuthorized = true;
      this.#initGoogleSignIn();
    }
  }

  #initGoogleSignIn() {
    // Wait for the GIS library to load
    const tryInit = () => {
      if (typeof (window as any).google === 'undefined' || !(window as any).google.accounts) {
        setTimeout(tryInit, 200);
        return;
      }
      (window as any).google.accounts.id.initialize({
        client_id: '845556473362-hn577kpi7nco8muojdsv09svttdcjd9s.apps.googleusercontent.com',
        callback: (response: any) => this.#handleGoogleSignIn(response),
        auto_select: true,
      });
      // Render the sign-in button inside our shadow DOM
      this.requestUpdate();
    };
    tryInit();
  }

  #showGreeting(firstName: string) {
    this.#chatHistory = [{
      role: 'agent',
      content:
`Hello${firstName ? ' ' + firstName : ''}! 👋

I'm your **Data Assistant**. Here's what I can help you with:

- 📊 **Scan your datasets** in BigQuery
- 🗣️ **Run queries in Natural Language** on your datasets
- 📋 **Display results** as Tables, Pie Charts, or Line/Bar Charts when appropriate
- 💾 **Save, modify, or delete** existing queries
- 📤 **Export results** to Spreadsheets or a Cloud Storage Bucket
- 🗂️ **Import external data** — use a Cloud Storage CSV, Parquet, or Avro file and save it as an external table you can join with your dataset tables
- 🔍 **Search the web** using Google Search to retrieve structured data, then save it as a query you can visualize as a Table, Pie Chart, or Line/Bar Chart

I will keep an up-to-date **semantic understanding** of your datasets and will create a **dedicated workspace** for you, so I learn and improve each time you request a new query.

Type anything to get started — for example, try **"Show my queries"** or **"Scan my datasets"**.`
    }];
  }

  #handleGoogleSignIn(response: any) {
    try {
      // Decode the JWT to get user info (the payload is in part 1)
      const payload = JSON.parse(atob(response.credential.split('.')[1]));
      this.#authUser = {
        email: payload.email,
        name: payload.name || payload.email,
        picture: payload.picture || '',
        credential: response.credential,
      };
      // Pass token to the client for all future requests
      this.#a2uiClient.setAuthToken(response.credential);
      console.log('[Auth] OAuth mode — signed in as:', payload.email);

      // Show greeting as the first chat message
      const firstName = (payload.name || payload.email || '').split(' ')[0];
      this.#showGreeting(firstName);
    } catch (e) {
      console.error('[Auth] Failed to decode credential:', e);
    }
  }

  #handleSignOut() {
    if (this.#authMode === 'oauth') {
      (window as any).google?.accounts?.id?.disableAutoSelect();
      this.#a2uiClient.setAuthToken(null);
    }
    this.#authUser = null;
    this.#chatHistory = [];
    console.log('[Auth] Signed out');
  }

  @provide({ context: UI.Context.themeContext })
  accessor theme: v0_8.Types.Theme = uiTheme;

  @provide({ context: UI.Context.markdown })
  accessor markdownRenderer: v0_8.Types.MarkdownRenderer = async (text, options) => {
    return renderMarkdown(text, options);
  };

  @state()
  accessor #requesting = false;

  @state()
  accessor #error: string | null = null;

  @state()
  accessor renderVersion = 0;

  @state()
  accessor #lastMessages: v0_8.Types.ServerToClientMessage[] = [];

  @state()
  accessor #chatHistory: {role: "user" | "agent", content?: string, isSurface?: boolean, surfaceId?: string}[] = [];

  #dataProcessedDuringLastSend = false;
  #surfaceInteractionCounter = 0;

  @state()
  accessor #statusMessage: string = 'Thinking...';

  static styles = [
    unsafeCSS(v0_8.Styles.structuralStyles),
    css`
      :host {
        display: flex;
        flex-direction: column;
        width: 100%;
        max-width: 100%;
        margin: 0;
        height: 100vh;
        font-size: 14.5px;
        overflow: hidden;
      }

      /* Custom scrollbars — all scrollable areas */
      * {
        scrollbar-width: thin;
        scrollbar-color: rgba(255, 255, 255, 0.15) transparent;
      }

      *::-webkit-scrollbar {
        width: 6px;
        height: 6px;
      }

      *::-webkit-scrollbar-track {
        background: transparent;
      }

      *::-webkit-scrollbar-thumb {
        background: rgba(255, 255, 255, 0.15);
        border-radius: 3px;
      }

      *::-webkit-scrollbar-thumb:hover {
        background: rgba(255, 255, 255, 0.3);
      }

      *::-webkit-scrollbar-corner {
        background: transparent;
      }

      .split-container {
        display: flex;
        flex-direction: row;
        flex: 1;
        overflow: hidden;
        gap: 0;
      }

      .left-panel {
        display: flex;
        flex-direction: column;
        width: 540px;
        min-width: 320px;
        border-right: none;
        overflow: hidden;
        flex-shrink: 0;
      }

      .left-panel-scroll {
        flex: 1;
        overflow-y: auto;
        padding: 16px;
      }

      .resize-handle {
        width: 6px;
        cursor: col-resize;
        background: #2a2a2a;
        flex-shrink: 0;
        position: relative;
        transition: background 0.15s;
        z-index: 10;
      }

      .resize-handle:hover,
      .resize-handle.active {
        background: #4a90d9;
      }

      .resize-handle::after {
        content: '';
        position: absolute;
        top: 50%;
        left: 1px;
        width: 4px;
        height: 32px;
        transform: translateY(-50%);
        border-left: 1px solid #555;
        border-right: 1px solid #555;
      }

      .resize-handle:hover::after,
      .resize-handle.active::after {
        border-left-color: #fff;
        border-right-color: #fff;
      }

      .right-panel {
        display: flex;
        flex-direction: column;
        flex: 1;
        overflow-y: auto;
        padding: 16px 24px;
        background: #181818;
        min-width: 200px;
      }

      .right-panel-empty {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
        color: #555;
        font-size: 15px;
        font-style: italic;
      }

      #surfaces {
        display: flex;
        flex-direction: row;
        gap: 16px;
        width: 100%;
        padding: var(--bb-grid-size-3) 0;
        animation: fadeIn 1s cubic-bezier(0, 0, 0.3, 1) 0.3s backwards;
        align-items: flex-start;

        & a2ui-surface {
          width: 100%;
          flex: 1;
        }
      }

      form {
        display: flex;
        flex-direction: column;
        flex: 1;
        gap: 0;
        align-items: center;
        padding: 10px 16px;

        & > div {
          display: flex;
          flex: 1;
          gap: 0;
          align-items: center;
          width: 100%;
          background: #2a2a2a;
          border: 1px solid #3a3a3a;
          border-radius: 12px;
          padding: 4px 4px 4px 16px;
          transition: border-color 0.2s;

          &:focus-within {
            border-color: #555;
          }

          & > textarea {
            display: block;
            flex: 1;
            border-radius: 0;
            padding: 10px 8px;
            border: none;
            background: transparent;
            color: #e0e0e0;
            font-size: 14px;
            font-family: inherit;
            outline: none;
            box-shadow: none;
            resize: none;
            overflow-y: hidden;
            min-height: 22px;
            max-height: 150px;
            line-height: 1.4;

            &::placeholder {
              color: #777;
            }
          }

          & > button {
            display: flex;
            align-items: center;
            justify-content: center;
            background: #444;
            color: #aaa;
            border: none;
            width: 34px;
            height: 34px;
            border-radius: 8px;
            opacity: 0.5;
            flex-shrink: 0;
            transition: background 0.15s, opacity 0.15s;

            &:not([disabled]) {
              cursor: pointer;
              opacity: 1;
              background: #e0e0e0;
              color: #1e1e1e;
            }

            &:not([disabled]):hover {
              background: #ffffff;
            }
          }
        }
      }

      .chat-history {
        display: flex;
        flex-direction: column;
        gap: 10px;
        width: 100%;
        margin-bottom: 20px;
      }

      .chat-message {
        padding: 6px 12px; /* Very compact padding */
        border-radius: 14px;
        max-width: 85%;
        line-height: 1.3;
        white-space: normal; /* Change from pre-wrap back to normal for markdown rendering */
        font-size: 14px;
      }

      .chat-message p {
        margin: 0;
      }
      
      .chat-message p + p {
        margin-top: 8px;
      }
      
      .chat-message ul, .chat-message ol {
        margin: 8px 0;
        padding-left: 24px;
      }
      
      .chat-message li {
        margin-bottom: 2px;
      }
      
      .chat-message a {
        color: #64b5f6;
      }

      .chat-message.user {
        align-self: flex-end;
        background-color: #2c497d;
        color: #ffffff;
        border-bottom-right-radius: 4px;
      }

      .chat-message.agent {
        align-self: flex-start;
        background-color: #1a1a1a;
        color: #e0e0e0;
        border-bottom-left-radius: 4px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.3);
      }

      .rotate {
        animation: spin 1s linear infinite;
      }

      .pending {
        width: 100%;
        min-height: 200px;
        display: flex;
        align-items: center;
        justify-content: center;
        animation: fadeIn 1s cubic-bezier(0, 0, 0.3, 1) 0.3s backwards;

        & .g-icon {
          margin-right: 8px;
        }
      }

      .error {
        color: var(--e-40);
        background-color: var(--e-95);
        border: 1px solid var(--e-80);
        padding: 16px;
        border-radius: 8px;
      }

      @keyframes fadeIn {
        from {
          opacity: 0;
        }

        to {
          opacity: 1;
        }
      }

      .spinner {
        width: 24px;
        height: 24px;
        border: 3px solid rgba(255, 255, 255, 0.1);
        border-left-color: var(--p-60);
        border-radius: 50%;
        animation: spin 1s linear infinite;
      }

      .rendering-indicator {
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 16px;
        color: var(--p-40);
        font-size: 14px;
        border-top: 1px solid var(--n-90);
        margin-top: 16px;
        width: 100%;

        & .g-icon {
          margin-right: 8px;
          font-size: 16px;
        }
      }

      @keyframes spin {
        to {
          transform: rotate(360deg);
        }
      }

      @keyframes pulse {
        0% {
          opacity: 0.6;
        }
        50% {
          opacity: 1;
        }
        100% {
          opacity: 0.6;
        }
      }

      .table-action-bar {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin-top: 10px;
      }

      .rerun-query-btn {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 8px 18px;
        background: rgba(255, 255, 255, 0.06);
        color: var(--p-40, #a8c7fa);
        border: 1px solid rgba(255, 255, 255, 0.12);
        border-radius: 20px;
        font-size: 13px;
        font-family: inherit;
        cursor: pointer;
        transition: all 0.2s ease;
      }

      .rerun-query-btn:hover:not(:disabled) {
        background: rgba(255, 255, 255, 0.12);
        border-color: var(--p-40, #a8c7fa);
      }

      .rerun-query-btn:disabled {
        opacity: 0.4;
        cursor: not-allowed;
      }

      .text-message {
        background-color: var(--n-95);
        color: var(--n-10);
        padding: 16px;
        border-radius: 8px;
        margin-bottom: 16px;
        font-size: 16px;
        line-height: 1.5;
        white-space: pre-wrap;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
      }
    `,
  ];

  #processor = v0_8.Data.createSignalA2uiMessageProcessor();
  #a2uiClient = new A2UIClient();
  #snackbar: Snackbar | undefined = undefined;
  #pendingSnackbarMessages: Array<{
    message: SnackbarMessage;
    replaceAll: boolean;
  }> = [];

  #startResize = (e: MouseEvent) => {
    e.preventDefault();
    const handle = this.renderRoot.querySelector('#resizeHandle') as HTMLElement;
    const leftPanel = this.renderRoot.querySelector('#leftPanel') as HTMLElement;
    if (!handle || !leftPanel) return;
    
    handle.classList.add('active');
    const startX = e.clientX;
    const startWidth = leftPanel.offsetWidth;
    
    const onMouseMove = (ev: MouseEvent) => {
      const delta = ev.clientX - startX;
      const newWidth = Math.max(320, Math.min(startWidth + delta, window.innerWidth - 200));
      leftPanel.style.width = `${newWidth}px`;
    };
    
    const onMouseUp = () => {
      handle.classList.remove('active');
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  render() {
    return html`
      <header style="position: sticky; top: 0; background: #1e1e1e; z-index: 100; padding: 12px 24px; border-bottom: 1px solid #333; flex-shrink: 0; display: flex; align-items: center; justify-content: space-between;">
        <h1 style="margin: 0; font-size: 1.1rem; text-align: left; color: #ffffff; font-weight: 500;">
          Data Assistant
        </h1>
        ${this.#authUser ? html`
          <div style="display: flex; align-items: center; gap: 10px;">
            ${this.#authUser.picture ? html`<img src="${this.#authUser.picture}" alt="" style="width: 28px; height: 28px; border-radius: 50%;" referrerpolicy="no-referrer">` : nothing}
            <span style="color: #aaa; font-size: 13px;">${this.#authUser.email}</span>
            ${this.#authMode === 'oauth' ? html`
              <button @click=${() => this.#handleSignOut()} style="background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.15); color: #ccc; padding: 4px 12px; border-radius: 14px; font-size: 12px; cursor: pointer; transition: all 0.2s;">Sign out</button>
            ` : html`
              <span style="background: rgba(76,175,80,0.15); color: #81c784; padding: 2px 10px; border-radius: 10px; font-size: 11px;">IAP</span>
            `}
          </div>
        ` : html`
          <span style="color: #888; font-size: 13px;">${this.#authMode === 'loading' ? 'Loading...' : 'Not signed in'}</span>
        `}
      </header>

      ${!this.#authUser ? html`
        <div style="display: flex; flex-direction: column; align-items: center; justify-content: center; flex: 1; gap: 24px; padding: 40px;">
          <div style="text-align: center;">
            <h2 style="color: #fff; margin: 0 0 8px 0; font-weight: 500;">Welcome to Data Assistant</h2>
            ${this.#authMode === 'loading' ? html`
              <p style="color: #888; margin: 0 0 24px 0;">Detecting authentication...</p>
            ` : this.#authMode === 'iap' ? html`
              <p style="color: #888; margin: 0 0 24px 0;">Authenticating via Identity-Aware Proxy...</p>
            ` : html`
              <p style="color: #888; margin: 0 0 24px 0;">Sign in with your Google account to get started</p>
            `}
          </div>
          ${this.#authMode === 'oauth' ? html`<div id="googleSignInButton"></div>` : nothing}
        </div>
      ` : html`
      
      <div class="split-container">
        <!-- LEFT PANEL: Chat conversation -->
        <div class="left-panel" id="leftPanel">
          <div class="left-panel-scroll">
            ${this.#maybeRenderData()}
          </div>
          <div style="flex-shrink: 0; background: #1e1e1e; border-top: 1px solid #333;">
            ${this.#maybeRenderForm()}
          </div>
        </div>
        
        <!-- RESIZE HANDLE -->
        <div class="resize-handle" id="resizeHandle"
          @mousedown=${this.#startResize}></div>
        
        <!-- RIGHT PANEL: Tables -->
        <div class="right-panel">
          ${this.#renderTablesPanel()}
        </div>
      </div>
      
      ${this.#maybeRenderError()}
      `}
    `;
  }

  updated(changedProperties: Map<string, unknown>) {
    super.updated(changedProperties);
    // Render Google Sign-In button in shadow DOM when in OAuth mode and not authenticated
    if (this.#authMode === 'oauth' && !this.#authUser) {
      const container = this.renderRoot.querySelector('#googleSignInButton');
      if (container && typeof (window as any).google !== 'undefined') {
        (window as any).google.accounts.id.renderButton(container, {
          theme: 'filled_black',
          size: 'large',
          shape: 'pill',
          text: 'signin_with',
        });
      }
    }
  }

  #maybeRenderError() {
    if (!this.#error) return nothing;

    return html`<div class="error">${this.#error}</div>`;
  }

  #maybeRenderForm() {
    return html`<form
      @submit=${async (evt: Event) => {
        evt.preventDefault();
        if (!(evt.target instanceof HTMLFormElement)) {
          return;
        }
        const data = new FormData(evt.target);
        const body = (data.get("body") as string ?? '').trim();
        if (!body) {
          return;
        }
        const message: v0_8.Types.A2UIClientEventMessage = {
          request: body,
        };
        this.#chatHistory = [...this.#chatHistory, {role: "user", content: body}];
        
        // Clear and reset textarea
        const textarea = this.renderRoot.querySelector('#body') as HTMLTextAreaElement;
        if (textarea) {
          textarea.value = '';
          textarea.style.height = 'auto';
        }
        (evt.target as HTMLFormElement).reset();
        
        await this.#sendAndProcessMessage(message);
      }}
    >
      <div>
        <textarea
          required
          placeholder="Ask me anything..."
          autocomplete="off"
          id="body"
          name="body"
          rows="1"
          ?disabled=${this.#requesting}
          @keydown=${(e: KeyboardEvent) => {
            if (e.key === 'Enter' && !e.shiftKey) {
              e.preventDefault();
              const form = (e.target as HTMLElement).closest('form');
              if (form) form.requestSubmit();
            }
          }}
          @input=${(e: Event) => {
            const el = e.target as HTMLTextAreaElement;
            el.style.height = 'auto';
            el.style.height = Math.min(el.scrollHeight, 150) + 'px';
          }}
        ></textarea>
        <button type="submit" ?disabled=${this.#requesting}>
          <span class="g-icon filled-heavy">send</span>
        </button>
      </div>
    </form>`;
  }

  #maybeRenderData() {
    if (this.#requesting && this.#chatHistory.length === 0 && this.#processor.getSurfaces().size === 0) {
      return html` <div class="pending">
        <div class="spinner"></div>
        <div class="loading-text">Awaiting an answer...</div>
      </div>`;
    }

    const textMessages = this.#chatHistory.filter(msg => !msg.isSurface);

    if (textMessages.length === 0) {
      return nothing;
    }

    return html`
    <section>
      <div class="chat-history">
        ${textMessages.map(msg => html`
          <div class="chat-message ${msg.role}">
            ${msg.role === 'agent' && msg.content
              ? until(Promise.resolve(renderMarkdown(msg.content)).then(res => unsafeHTML(res)), msg.content)
              : msg.content}
          </div>
        `)}
      </div>

      ${this.#requesting
        ? html`<div class="rendering-indicator">
            <span class="g-icon filled-heavy rotate">progress_activity</span>
            ${this.#statusMessage}
          </div>`
        : nothing}
    </section>`;
  }

  #renderTablesPanel() {
    const surfaceEntries = this.#chatHistory.filter(msg => msg.isSurface && msg.surfaceId);
    
    if (surfaceEntries.length === 0) {
      return html`<div class="right-panel-empty">Query results will appear here</div>`;
    }

    return html`
      ${surfaceEntries.map(msg => {
        const surface = this.#processor.getSurfaces().get(msg.surfaceId!);
        if (!surface) return nothing;
        return html`
          <div style="margin-bottom: 24px; animation: fadeIn 0.5s ease-out;">
            <a2ui-surface
              .surface=${surface}
              @a2uiaction=${async (evt: v0_8.Events.StateEvent<"a2ui.action">) => {
                const [target] = evt.composedPath();
                if (!(target instanceof HTMLElement)) return;
                
                const context: v0_8.Types.A2UIClientEventMessage["userAction"]["context"] = {};
                if (evt.detail.action.context) {
                  for (const item of evt.detail.action.context) {
                    if (item.value.literalBoolean) context[item.key] = item.value.literalBoolean;
                    else if (item.value.literalNumber) context[item.key] = item.value.literalNumber;
                    else if (item.value.literalString) context[item.key] = item.value.literalString;
                    else if (item.value.path) {
                      const path = this.#processor.resolvePath(item.value.path, evt.detail.dataContextPath);
                      context[item.key] = this.#processor.getData(evt.detail.sourceComponent, path, msg.surfaceId!);
                    }
                  }
                }
                
                const message: v0_8.Types.A2UIClientEventMessage = {
                  userAction: {
                    surfaceId: msg.surfaceId!,
                    name: "ACTION: " + evt.detail.action.name,
                    sourceComponentId: target.id,
                    timestamp: new Date().toISOString(),
                    context,
                  },
                };
                await this.#sendAndProcessMessage(message);
              }}
              .surfaceId=${msg.surfaceId}
              .processor=${this.#processor}
              .enableCustomElements=${true}
            ></a2ui-surface>
            <div class="table-action-bar">
              ${[
                { icon: 'replay', label: 'Re-run Query', prefix: 'Re-run the query:' },
                { icon: 'bookmark', label: 'Save Query', prefix: 'Save the query:' },
                { icon: 'table_chart', label: 'Export to Sheets', prefix: 'Export to Google Sheets the query:' },
                { icon: 'cloud_upload', label: 'Export to GCS', prefix: 'Export to Cloud Storage the query:' },
              ].map(action => html`
                <button
                  class="rerun-query-btn"
                  ?disabled=${this.#requesting}
                  @click=${async () => {
                    let title = '';
                    const surface = this.#processor.getSurfaces().get(msg.surfaceId!);
                    if (surface) {
                      for (const [, comp] of (surface.components as Map<string, any>)) {
                        const tableProps = comp?.component?.Table;
                        if (tableProps?.tableTitle?.literalString) {
                          title = tableProps.tableTitle.literalString;
                          break;
                        }
                        const chartProps = comp?.component?.PieChart;
                        if (chartProps?.chartTitle?.literalString) {
                          title = chartProps.chartTitle.literalString;
                          break;
                        }
                        const barChartProps = comp?.component?.BarChart;
                        if (barChartProps?.chartTitle?.literalString) {
                          title = barChartProps.chartTitle.literalString;
                          break;
                        }
                      }
                    }
                    title = title.replace(/\s*\(\d+\s+total\s+rows\)\s*$/i, '').trim();
                    if (!title) title = 'the previous query';
                    
                    const body = `${action.prefix} ${title}`;
                    this.#chatHistory = [...this.#chatHistory, {role: "user", content: body}];
                    const message: v0_8.Types.A2UIClientEventMessage = { request: body };
                    await this.#sendAndProcessMessage(message);
                  }}
                >
                  <span class="g-icon" style="font-size: 16px; margin-right: 6px;">${action.icon}</span>
                  ${action.label}
                </button>
              `)}
            </div>
          </div>`;
      })}
    `;
  }

  async #sendAndProcessMessage(request: v0_8.Types.A2UIClientEventMessage) {
    this.#requesting = true;
    this.#dataProcessedDuringLastSend = false;
    this.#statusMessage = 'Agent is thinking...';
    this.requestUpdate();
    
    // Snapshot surface IDs BEFORE sending so we can detect new ones
    const surfaceIdsBefore = new Set(this.#processor.getSurfaces().keys());
    
    const messages = await this.#sendMessage(request);

    this.#lastMessages = messages;
    
    // Collect text messages
    const newHistoryItems: {role: "agent", content?: string, isSurface?: boolean, surfaceId?: string}[] = [];
    
    for (const msg of messages) {
       const anyMsg = msg as any;
       if (anyMsg.kind === "text" && anyMsg.text) {
            // Skip progress-only messages (emoji-prefixed status updates from the backend)
            // These were already shown in the spinner and should not appear in the chat.
            const text = anyMsg.text.trim();
            const isProgressOnly = (
              text.startsWith('\u{1F50D}') || text.startsWith('\u{1F4DA}') || text.startsWith('\u{1F52C}') ||
              text.startsWith('\u2705') || text.startsWith('\u{1F680}') || text.startsWith('\u{1F504}') ||
              text.startsWith('\u{1F4CA}') || text.startsWith('\u{1F50E}') || text.startsWith('\u{1F9E0}') ||
              text.startsWith('\u{1F4E4}') || text.startsWith('\u{1F4BE}') || text.startsWith('\u{1F310}') ||
              text.startsWith('\u{1F4E5}') || text.startsWith('\u{1F4DD}') || text.startsWith('\u{1F4C2}') ||
              text.startsWith('\u{1F4CB}') || text.startsWith('\u{1F4CC}') || text.startsWith('\u{1F5D1}') ||
              text.startsWith('\u{1F914}')
            ) && text.length < 120;  // Real responses are longer
            if (!isProgressOnly) {
              newHistoryItems.push({ role: "agent", content: anyMsg.text });
            }
       }
    }
    
    // Detect NEW surfaces that appeared in the processor after this round-trip
    const surfaceIdsAfter = new Set(this.#processor.getSurfaces().keys());
    for (const sid of surfaceIdsAfter) {
      if (!surfaceIdsBefore.has(sid)) {
        newHistoryItems.push({ role: "agent", isSurface: true, surfaceId: sid });
      }
    }
    
    // If data was processed but NO new surface IDs appeared, it means an 
    // existing surface was UPDATED (e.g. backend reuses "@default").
    // We still need to show the updated table inline in the chat flow.
    if (this.#dataProcessedDuringLastSend && newHistoryItems.every(i => !i.isSurface) && surfaceIdsAfter.size > 0) {
      for (const sid of surfaceIdsAfter) {
        // Always add — even if the same surfaceId is already in history,
        // because the data inside has changed to a new query result.
        newHistoryItems.push({ role: "agent", isSurface: true, surfaceId: sid });
      }
    }
    
    if (newHistoryItems.length > 0) {
      this.#chatHistory = [...this.#chatHistory, ...newHistoryItems];
    }

    this.renderVersion++; // Force re-render of surfaces
    this.requestUpdate();
  }

  async #sendMessage(
    message: v0_8.Types.A2UIClientEventMessage
  ): Promise<v0_8.Types.ServerToClientMessage[]> {
    try {
      this.#requesting = true;
      const response = await this.#a2uiClient.send(message, (chunkMessages) => {
        // Update status based on incoming chunks
        const textChunks = chunkMessages.filter(m => (m as any).kind === "text");
        const dataMessages = chunkMessages.filter(m => (m as any).kind !== "text");
        
        // Detect phase from text content — prefer rich progress messages from backend
        for (const tc of textChunks) {
          const text = ((tc as any).text || '');
          const textLower = text.toLowerCase();
          // Priority 1: Backend sends rich progress messages with emoji prefixes
          if (text.startsWith('🔍') || text.startsWith('📚') || text.startsWith('🔬') ||
              text.startsWith('✅') || text.startsWith('🚀') || text.startsWith('🔄') ||
              text.startsWith('📊') || text.startsWith('🔎') || text.startsWith('🧠') ||
              text.startsWith('📤') || text.startsWith('💾') || text.startsWith('🌐') ||
              text.startsWith('📥') || text.startsWith('📝') || text.startsWith('📂') ||
              text.startsWith('📋') || text.startsWith('📌') || text.startsWith('🗑') ||
              text.startsWith('🤔')) {
            this.#statusMessage = text;
          }
          // Priority 2: Keyword-based fallback
          else if (textLower.includes('profiling') || textLower.includes('profile')) {
            this.#statusMessage = '📊 Profiling dataset...';
          } else if (textLower.includes('scanning') || textLower.includes('scan')) {
            this.#statusMessage = '🔎 Scanning datasets...';
          } else if (textLower.includes('executing') || textLower.includes('execute')) {
            this.#statusMessage = '🚀 Executing query...';
          } else if (textLower.includes('export') && textLower.includes('sheet')) {
            this.#statusMessage = '📤 Exporting to Sheets...';
          } else if (textLower.includes('analyz')) {
            this.#statusMessage = '🧠 Analyzing data...';
          } else if (textLower.includes('validat') || textLower.includes('dry run')) {
            this.#statusMessage = '✅ Validating query...';
          } else if (textLower.includes('retry') || textLower.includes('retrying')) {
            this.#statusMessage = '🔄 Retrying with adjustments...';
          } else if (textLower.includes('diagnos')) {
            this.#statusMessage = '🔍 Diagnosing query issues...';
          } else {
            this.#statusMessage = 'Agent is thinking...';
          }
        }
        
        if (dataMessages.length > 0) {
          this.#statusMessage = 'Rendering table...';
          try {
            this.#processor.processMessages(dataMessages);
            this.#dataProcessedDuringLastSend = true;
          } catch(e) {
            console.warn('A2UI processing error:', e);
          }
        }
        this.renderVersion++;
        this.requestUpdate();
      });

      this.#requesting = false;

      return response;
    } catch (err) {
      this.snackbar(err as string, SnackType.ERROR);
    } finally {
      this.#requesting = false;
    }

    return [];
  }

  snackbar(
    message: string | HTMLTemplateResult,
    type: SnackType,
    actions: SnackbarAction[] = [],
    persistent = false,
    id = globalThis.crypto.randomUUID(),
    replaceAll = false
  ) {
    if (!this.#snackbar) {
      this.#pendingSnackbarMessages.push({
        message: {
          id,
          message,
          type,
          persistent,
          actions,
        },
        replaceAll,
      });
      return;
    }

    return this.#snackbar.show(
      {
        id,
        message,
        type,
        persistent,
        actions,
      },
      replaceAll
    );
  }

  unsnackbar(id?: SnackbarUUID) {
    if (!this.#snackbar) {
      return;
    }

    this.#snackbar.hide(id);
  }
}

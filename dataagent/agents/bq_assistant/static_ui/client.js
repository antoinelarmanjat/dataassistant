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
import { registerContactComponents } from "./ui/custom-components/register-components.js";
import { componentRegistry } from "@a2ui/lit/ui";
export class A2UIClient {
    #ready = Promise.resolve();
    #authToken = null;
    get ready() {
        return this.#ready;
    }
    setAuthToken(token) {
        this.#authToken = token;
    }
    async send(message, onChunk) {
        const catalog = componentRegistry.getInlineCatalog();
        const finalMessage = {
            ...message,
            metadata: {
                "a2uiClientCapabilities": {
                    "inlineCatalogs": [catalog],
                },
            },
        };
        const headers = {};
        if (this.#authToken) {
            headers['Authorization'] = `Bearer ${this.#authToken}`;
        }
        const response = await fetch("/a2a", {
            body: JSON.stringify(finalMessage),
            method: "POST",
            headers,
        });
        if (!response.ok) {
            const error = (await response.json());
            throw new Error(error.error);
        }
        const contentType = response.headers.get("content-type");
        const messages = [];
        if (contentType?.includes("text/event-stream")) {
            const reader = response.body?.getReader();
            if (!reader)
                throw new Error("No response body");
            const decoder = new TextDecoder();
            let buffer = "";
            while (true) {
                const { done, value } = await reader.read();
                if (done)
                    break;
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n\n");
                buffer = lines.pop() || "";
                for (const line of lines) {
                    if (line.startsWith("data: ")) {
                        const jsonStr = line.replace(/^data:\s*/, "");
                        try {
                            const parsed = JSON.parse(jsonStr);
                            if ("error" in parsed) {
                                throw new Error(parsed.error);
                            }
                            else {
                                const chunkMessages = this.#extractMessages(parsed);
                                if (chunkMessages.length > 0) {
                                    messages.push(...chunkMessages);
                                    onChunk?.(chunkMessages);
                                }
                            }
                        }
                        catch (e) {
                            console.error("Error parsing SSE data:", e, jsonStr);
                        }
                    }
                }
            }
            return messages;
        }
        const data = (await response.json());
        if (data && typeof data === 'object' && "error" in data) {
            throw new Error(data.error);
        }
        else {
            const extracted = this.#extractMessages(data);
            messages.push(...extracted);
            if (messages.length > 0) {
                onChunk?.(messages);
            }
        }
        return messages;
    }
    #extractMessages(data) {
        let items = [];
        if (data.messages && Array.isArray(data.messages)) {
            items = data.messages;
        }
        else {
            items = Array.isArray(data)
                ? data
                : (data.kind === "message" && Array.isArray(data.parts) ? data.parts : [data]);
        }
        const messages = [];
        for (const item of items) {
            if (item.kind === "message" && Array.isArray(item.parts)) {
                for (const part of item.parts) {
                    if (part.data) {
                        messages.push(part.data);
                    }
                }
            }
            else {
                if (item.kind === "text" || item.kind === "json_ui") {
                    messages.push(item);
                    continue;
                }
                if (item.data) {
                    messages.push(item.data);
                }
            }
        }
        return messages;
    }
}
registerContactComponents();
//# sourceMappingURL=client.js.map
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
import { A2AClient } from "@a2a-js/sdk/client";
import { v4 as uuidv4 } from "uuid";
const A2UI_MIME_TYPE = "application/json+a2ui";
const enableStreaming = process.env["ENABLE_STREAMING"] !== "false";
const fetchWithCustomHeader = async (url, init) => {
    const headers = new Headers(init?.headers);
    headers.set("X-A2A-Extensions", "https://a2ui.org/a2a-extension/a2ui/v0.8");
    const newInit = { ...init, headers };
    return fetch(url, newInit);
};
const isJson = (str) => {
    try {
        const parsed = JSON.parse(str);
        return (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed));
    }
    catch (err) {
        console.warn(err);
        return false;
    }
};
let client = null;
// Persistent context ID for session continuity
const sessionContextId = uuidv4();
const createOrGetClient = async () => {
    if (!client) {
        // Create a client pointing to the agent's Agent Card URL.
        client = await A2AClient.fromCardUrl("http://localhost:10005/.well-known/agent-card.json", { fetchImpl: fetchWithCustomHeader });
    }
    return client;
};
export const plugin = () => {
    const proxyToBackend = (path) => {
        return async (req, res, next) => {
            const backendUrl = `http://localhost:10005${req.url}`;
            const authHeader = req.headers['authorization'] || '';
            try {
                const headers = {};
                if (authHeader) headers['Authorization'] = String(authHeader);
                if (req.method === 'POST' || req.method === 'PUT') {
                    headers['Content-Type'] = req.headers['content-type'] || 'application/json';
                }

                let body = '';
                if (req.method === 'POST' || req.method === 'PUT') {
                    await new Promise((resolve) => {
                        req.on('data', (chunk) => { body += chunk.toString(); });
                        req.on('end', resolve);
                    });
                }

                const fetchOpts = { method: req.method, headers };
                if (body) fetchOpts.body = body;

                const backendResponse = await fetch(backendUrl, fetchOpts);
                const data = await backendResponse.text();
                res.statusCode = backendResponse.status;
                res.setHeader('Content-Type', backendResponse.headers.get('content-type') || 'application/json');
                res.end(data);
            } catch (e) {
                console.error(`[proxy] Error proxying ${req.url}:`, e);
                res.statusCode = 502;
                res.setHeader('Content-Type', 'application/json');
                res.end(JSON.stringify({ error: 'Backend unavailable' }));
            }
        };
    };

    return {
        name: "a2a-handler",
        configureServer(server) {
            // Proxy /sessions/* to backend
            server.middlewares.use("/sessions", proxyToBackend("/sessions"));
            // Proxy /auth/* to backend
            server.middlewares.use("/auth", proxyToBackend("/auth"));
            // Proxy /a2a to backend (existing)
            server.middlewares.use("/a2a", async (req, res, next) => {
                if (req.method === "POST") {
                    let originalBody = "";
                    req.on("data", (chunk) => {
                        originalBody += chunk.toString();
                    });
                    req.on("end", async () => {
                        // Forward directly to the backend's /a2a endpoint
                        // which has progress streaming and proper a2ui handling.
                        // Using direct fetch instead of A2AClient SDK avoids routing
                        // through the A2A framework handler (which lacks streaming).
                        const backendUrl = "http://localhost:10005/a2a";
                        // Extract auth header from frontend request
                        const authHeader = req.headers['authorization'] || '';
                        try {
                            const headers = {
                                "Content-Type": "application/json",
                            };
                            if (authHeader) {
                                headers["Authorization"] = String(authHeader);
                            }
                            const backendResponse = await fetch(backendUrl, {
                                method: "POST",
                                headers,
                                body: originalBody,
                            });
                            const contentType = backendResponse.headers.get("content-type") || "";
                            if (contentType.includes("text/event-stream")) {
                                // Stream SSE directly from backend to frontend
                                res.statusCode = 200;
                                res.setHeader("Content-Type", "text/event-stream");
                                res.setHeader("Cache-Control", "no-cache");
                                res.setHeader("Connection", "keep-alive");
                                const reader = backendResponse.body?.getReader();
                                if (reader) {
                                    const decoder = new TextDecoder();
                                    while (true) {
                                        const { done, value } = await reader.read();
                                        if (done)
                                            break;
                                        res.write(decoder.decode(value, { stream: true }));
                                    }
                                }
                                res.end();
                            }
                            else {
                                // Non-streaming response
                                const data = await backendResponse.text();
                                res.statusCode = backendResponse.status;
                                res.setHeader("Content-Type", contentType || "application/json");
                                res.end(data);
                            }
                        }
                        catch (e) {
                            console.error("[a2a-middleware] Error proxying to backend:", e);
                            if (!res.headersSent) {
                                res.statusCode = 500;
                                res.setHeader("Content-Type", "application/json");
                                res.end(JSON.stringify({ error: e.message || String(e) }));
                            }
                            else {
                                res.write(`data: ${JSON.stringify({ error: e.message || String(e) })}\n\n`);
                                res.end();
                            }
                        }
                    });
                    return;
                }
                else {
                    next();
                }
            });
        },
    };
};
//# sourceMappingURL=a2a.js.map
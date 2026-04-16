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

import { componentRegistry } from "@a2ui/lit/ui";
import { OrgChart } from "./org-chart.js";
import { WebFrame } from "./web-frame.js";
import { PremiumTextField } from "./premium-text-field.js";
import { McpApp } from "./mcp-apps-component.js";
import { TableView } from "./table-view.js";
import { PieChartView } from "./pie-chart.js";
import { BarChartView } from "./bar-chart.js";

export function registerContactComponents() {
  // Register OrgChart
  componentRegistry.register("OrgChart", OrgChart, "org-chart", {
    type: "object",
    properties: {
      chain: {
        type: "object",
        properties: {
          path: { type: "string" },
          literalArray: {
            type: "array",
            items: {
              type: "object",
              properties: {
                title: { type: "string" },
                name: { type: "string" },
              },
              required: ["title", "name"],
            },
          },
        },
      },
      action: {
        type: "object",
        properties: {
          name: { type: "string" },
          context: {
            type: "array",
            items: {
              type: "object",
              properties: {
                key: { type: "string" },
                value: {
                  type: "object",
                  properties: {
                    path: { type: "string" },
                    literalString: { type: "string" },
                    literalNumber: { type: "number" },
                    literalBoolean: { type: "boolean" },
                  },
                },
              },
              required: ["key", "value"],
            },
          },
        },
        required: ["name"],
      },
    },
    required: ["chain"],
  });

  // Register PremiumTextField as an override for TextField
  componentRegistry.register(
    "TextField",
    PremiumTextField,
    "premium-text-field"
  );

  // Register McpApp
  componentRegistry.register("McpApp", McpApp, "a2ui-mcp-apps-component", {
    type: "object",
    properties: {
      resourceUri: { type: "string" },
      htmlContent: { type: "string" },
      height: { type: "number" },
      allowedTools: {
        type: "array",
        items: { type: "string" }
      }
    },
  });

  // Register WebFrame
  componentRegistry.register("WebFrame", WebFrame, "a2ui-web-frame", {
    type: "object",
    properties: {
      url: { type: "string" },
      html: { type: "string" },
      height: { type: "number" },
      interactionMode: {
        type: "string",
        enum: ["readOnly", "interactive"]
      },
      allowedEvents: {
        type: "array",
        items: { type: "string" }
      }
    },
  });

  // Register TableView
  componentRegistry.register("Table", TableView, "table-view", {
    type: "object",
    properties: {
      tableTitle: { type: "string" },
      headers: {
        type: "array",
        items: { type: "string" }
      },
      rows: {
        type: "array",
        items: {
          type: "array",
          items: { type: "string" }
        }
      },
      action: {
        type: "object",
        properties: {
          name: { type: "string" },
          context: {
            type: "array",
            items: {
              type: "object",
              properties: {
                key: { type: "string" },
                value: {
                  type: "object",
                  properties: {
                    path: { type: "string" },
                    literalString: { type: "string" },
                    literalNumber: { type: "number" },
                    literalBoolean: { type: "boolean" },
                  },
                },
              },
              required: ["key", "value"],
            },
          },
        },
        required: ["name"],
      },
    },
  });

  // Register PieChart
  componentRegistry.register("PieChart", PieChartView, "a2ui-pie-chart", {
    type: "object",
    properties: {
      chartTitle: { type: "string" },
      labels: {
        type: "array",
        items: { type: "string" }
      },
      values: {
        type: "array",
        items: { type: "number" }
      },
      colors: {
        type: "array",
        items: { type: "string" }
      }
    },
  });

  // Register BarChart (supports both bar and line chart types)
  componentRegistry.register("BarChart", BarChartView, "a2ui-bar-chart", {
    type: "object",
    properties: {
      chartTitle: { type: "string" },
      labels: {
        type: "array",
        items: { type: "string" }
      },
      values: {
        type: "array",
        items: { type: "number" }
      },
      colors: {
        type: "array",
        items: { type: "string" }
      },
      chartType: { type: "string" }
    },
  });

  console.log("Registered Contact App Custom Components");
}

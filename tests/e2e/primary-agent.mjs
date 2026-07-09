#!/usr/bin/env node

import fs from "node:fs";
import readline from "node:readline";

const resultPath = process.env.MJ_E2E_PRIMARY_RESULT;
const logPath = process.env.MJ_E2E_PRIMARY_LOG;
const instructions = process.env.MJ_E2E_CODE_AGENT_INSTRUCTIONS ?? "Return CODEAGENT_E2E_OK";
let promptRequestId = null;
let extensionSentAt = null;

function send(message) {
  process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", ...message })}\n`);
}

function writeResult(message) {
  if (!resultPath) return;
  fs.writeFileSync(
    resultPath,
    JSON.stringify({
      response: message,
      extensionSentAt,
      extensionReceivedAt: Date.now(),
    }),
  );
}

function finishPrimary(text) {
  send({
    method: "session/update",
    params: {
      sessionId: "primary-session",
      update: {
        sessionUpdate: "agent_message_chunk",
        content: { type: "text", text },
      },
    },
  });
  send({ id: promptRequestId, result: { stopReason: "end_turn" } });
  setTimeout(() => process.exit(0), 300);
}

const input = readline.createInterface({ input: process.stdin });
input.on("line", (line) => {
  if (logPath) fs.appendFileSync(logPath, `${line}\n`);
  const message = JSON.parse(line);
  if (message.method === "initialize") {
    const advertised = message.params?.clientCapabilities?._meta?.mj?.codeAgent;
    if (advertised?.method !== "_mj/codeAgent" || advertised?.version !== 1) {
      send({ id: message.id, error: { code: -32602, message: "missing codeAgent capability" } });
      return;
    }
    send({
      id: message.id,
      result: {
        protocolVersion: 1,
        agentCapabilities: {},
        agentInfo: { name: "e2e-primary", version: "1" },
      },
    });
    return;
  }
  if (message.method === "session/new") {
    send({ id: message.id, result: { sessionId: "primary-session" } });
    return;
  }
  if (message.method === "session/prompt") {
    promptRequestId = message.id;
    extensionSentAt = Date.now();
    send({
      id: "delegate-1",
      method: "_mj/codeAgent",
      params: { instructions },
    });
    return;
  }
  if (message.id === "delegate-1") {
    writeResult(message);
    if (message.result?.message) {
      finishPrimary(`PRIMARY RECEIVED: ${message.result.message}`);
    } else {
      finishPrimary(`PRIMARY CANCELLED: ${message.error?.data ?? message.error?.message ?? "error"}`);
    }
  }
});

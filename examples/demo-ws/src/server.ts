/**
 * Demo server (WebSocket) - Bun HTTP + native WebSocket upgrade.
 *
 *   POST /run?scope=<scopeId>&fail=<bool>     → kicks off a workflow in the background
 *   WS   /traces/user/:scopeId                  → batched trace events for that scope
 *
 * Run with `bun run server`. Frontend on :5174 proxies to here.
 */
import type { ServerWebSocket } from "bun";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";

import { LiveTraceLayer, TraceSinkLive, liveTraceLogger } from "livetrace";
import { WSTransportLayer, getWsBroker } from "livetrace/transports/ws";
import type { TraceScope } from "livetrace/types";

import { runWorkflow } from "./workflow.js";

const TraceLive = LiveTraceLayer.pipe(
    Layer.provide(TraceSinkLive({ flushIntervalMs: 100 })),
    Layer.provide(WSTransportLayer),
);

const LoggerLive = Logger.layer([liveTraceLogger], { mergeWithExisting: true });
const Runtime = Layer.merge(TraceLive, LoggerLive);

interface SocketData {
    scope: TraceScope;
    unsub: () => void;
}

const PORT = Number(process.env.PORT ?? 8788);

Bun.serve<SocketData>({
    port: PORT,
    fetch(req, server) {
        const url = new URL(req.url);

        if (req.method === "OPTIONS") {
            return new Response(null, {
                headers: {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type",
                },
            });
        }

        if (url.pathname === "/run" && req.method === "POST") {
            const scope = url.searchParams.get("scope") ?? "demo-user";
            const fail = url.searchParams.get("fail") === "true";
            const docId = url.searchParams.get("doc") ?? `report-${Date.now() % 10000}.pdf`;

            void Effect.runPromise(
                runWorkflow({ docId, scopeId: scope, fail }).pipe(
                    Effect.provide(Runtime),
                    Effect.scoped,
                    Effect.catchCause(() => Effect.void),
                ),
            );

            return new Response(JSON.stringify({ ok: true, traceScope: scope, docId }), {
                headers: {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
            });
        }

        const match = url.pathname.match(/^\/traces\/(team|org|user)\/(.+)$/);
        if (match) {
            const [, type, id] = match;
            const scope: TraceScope = { type: type as "user", id: id! };

            const ok = server.upgrade(req, {
                data: { scope, unsub: () => void 0 },
            });
            if (ok) return undefined;
            return new Response("upgrade failed", { status: 426 });
        }

        return new Response("not found", { status: 404 });
    },

    websocket: {
        open(ws: ServerWebSocket<SocketData>) {
            ws.send(JSON.stringify({ _hello: true }));
            const unsub = getWsBroker().subscribe(ws.data.scope, (events) => {
                try {
                    ws.send(JSON.stringify(events));
                } catch {
                    /* socket gone */
                }
            });
            ws.data.unsub = unsub;
        },
        message() {
            // demo is one-way; ignore inbound
        },
        close(ws) {
            ws.data.unsub?.();
        },
    },
});

// eslint-disable-next-line no-console
console.log(`[demo-ws] server http://localhost:${PORT}  (ws /traces/...)`);

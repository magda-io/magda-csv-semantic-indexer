/**
 * Minimal Magda registry API for local dev: hooks + empty record pages.
 */
import http from "node:http";
import { URL } from "node:url";

const PORT = Number(process.env.MOCK_REGISTRY_PORT || 6101);
const hooks = new Map();

const server = http.createServer((req, res) => {
    const url = new URL(req.url || "/", `http://127.0.0.1:${PORT}`);
    let body = "";
    req.on("data", (c) => {
        body += c;
    });
    req.on("end", () => {
        res.setHeader("Content-Type", "application/json");
        try {
            const pathNorm = url.pathname.replace(/\/$/, "") || "/";

            if (
                req.method === "GET" &&
                (pathNorm === "/v0" || pathNorm === "/")
            ) {
                res.statusCode = 200;
                res.end(
                    JSON.stringify({
                        service: "mock-magda-registry",
                        ok: true,
                        routes: [
                            "GET /v0/hooks/:id",
                            "PUT /v0/hooks/:id",
                            "POST /v0/hooks/:id/ack",
                            "GET /v0/records",
                        ],
                    }),
                );
                return;
            }

            const hookPath = url.pathname.match(/^\/v0\/hooks\/([^/]+)$/);
            const ackPath = url.pathname.match(/^\/v0\/hooks\/([^/]+)\/ack$/);

            if (req.method === "GET" && hookPath) {
                const id = decodeURIComponent(hookPath[1]);
                const h = hooks.get(id);
                if (!h) {
                    res.statusCode = 404;
                    res.end(JSON.stringify({ message: "not found" }));
                    return;
                }
                res.statusCode = 200;
                res.end(JSON.stringify(h));
                return;
            }

            if (req.method === "PUT" && hookPath) {
                const id = decodeURIComponent(hookPath[1]);
                const hook = JSON.parse(body || "{}");
                hooks.set(id, hook);
                res.statusCode = 200;
                res.end(JSON.stringify(hook));
                return;
            }

            if (req.method === "POST" && ackPath) {
                res.statusCode = 200;
                res.end(JSON.stringify({ lastEventIdReceived: null }));
                return;
            }

            if (req.method === "GET" && url.pathname === "/v0/records") {
                res.statusCode = 200;
                res.end(
                    JSON.stringify({
                        records: [],
                        hasMore: false,
                    }),
                );
                return;
            }

            res.statusCode = 404;
            res.end(
                JSON.stringify({
                    error: "mock registry: not implemented",
                    path: url.pathname,
                }),
            );
        } catch (e) {
            res.statusCode = 500;
            res.end(JSON.stringify({ error: String(e) }));
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Mock Magda registry at http://127.0.0.1:${PORT}/v0`);
});

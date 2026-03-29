/**
 * Minimal Magda embedding API stub (OpenAI-style /v1/embeddings).
 */
import http from "node:http";
import { URL } from "node:url";

const PORT = Number(process.env.MOCK_EMBEDDING_PORT || 3000);
const DIM = 768;

function fakeEmbedding() {
    return Array.from({ length: DIM }, () => 0);
}

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
                (pathNorm === "/" || pathNorm === "/v1/embeddings")
            ) {
                res.statusCode = 200;
                res.end(
                    JSON.stringify({
                        service: "mock-magda-embedding-api",
                        ok: true,
                        usage:
                            "POST JSON to /v1/embeddings with body { \"input\": \"text\" } or { \"input\": [\"a\",\"b\"] }",
                    }),
                );
                return;
            }

            if (
                req.method === "POST" &&
                pathNorm === "/v1/embeddings"
            ) {
                const parsed = JSON.parse(body || "{}");
                const input = parsed.input;
                const data = Array.isArray(input)
                    ? input.map(() => ({ embedding: fakeEmbedding() }))
                    : [{ embedding: fakeEmbedding() }];
                res.statusCode = 200;
                res.end(JSON.stringify({ data }));
                return;
            }
            res.statusCode = 404;
            res.end(JSON.stringify({ error: "not found" }));
        } catch (e) {
            res.statusCode = 500;
            res.end(JSON.stringify({ error: String(e) }));
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Mock embedding API at http://127.0.0.1:${PORT}/v1/embeddings`);
});

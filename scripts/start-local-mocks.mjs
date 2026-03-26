import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

function start(rel) {
    return spawn(process.execPath, [path.join(root, rel)], {
        stdio: "inherit",
    });
}

const registry = start("scripts/mock-registry.mjs");
const embedding = start("scripts/mock-embedding.mjs");

function shutdown() {
    registry.kill("SIGTERM");
    embedding.kill("SIGTERM");
}

process.on("SIGINT", () => {
    shutdown();
    process.exit(0);
});
process.on("SIGTERM", () => {
    shutdown();
    process.exit(0);
});

await Promise.all([
    new Promise((resolve) => registry.on("exit", resolve)),
    new Promise((resolve) => embedding.on("exit", resolve)),
]);

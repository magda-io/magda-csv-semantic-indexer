import { parse } from "csv-parse";
import { Readable } from "stream";
import type { ReadableStream as WebReadableStream } from "stream/web";
import path from "path";
import fse from "fs-extra";
import { __dirname as getCurDirPath } from "@magda/esm-utils";

const __dirname = getCurDirPath();
const CONNECTION_TIMEOUT_MS = 120_000;

function toNodeReadable(body: unknown): NodeJS.ReadableStream {
    return typeof (body as NodeJS.ReadableStream)?.pipe === "function"
        ? (body as NodeJS.ReadableStream)
        : Readable.fromWeb(body as WebReadableStream<Uint8Array>);
}

let cachedPackageInfo: { name: string; version: string } | null = null;

async function getPackageInfo() {
    if (!cachedPackageInfo) {
        cachedPackageInfo = await fse.readJSON(path.resolve(__dirname, "../../package.json"), {
            encoding: "utf-8",
        });
    }
    return cachedPackageInfo;
}

export async function getColumnsFromTSVStream(
    downloadURL: string,
    timeout: number = CONNECTION_TIMEOUT_MS,
): Promise<string[]> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    const pkg = await getPackageInfo();
    try {
        const res = await fetch(downloadURL, {
            redirect: "follow",
            headers: {
                "User-Agent": `${pkg?.name}/${pkg?.version}`,
            },
            signal: controller.signal,
        });
        if (!res.ok || !res.body) {
            throw new Error(`HTTP ${res.status} fetching ${downloadURL}`);
        }
        const parser = parse({
            bom: true,
            trim: true,
            skip_empty_lines: true,
            relax_column_count: true,
            delimiter: "\t",
            from_line: 1,
            to_line: 1,
            columns: false,
        });
        const stream = toNodeReadable(res.body);
        return await new Promise<string[]>((resolve, reject) => {
            let done = false;
            const finish = (cols: string[]) => {
                if (!done) {
                    done = true;
                    resolve(cols);
                }
            };
            const fail = (err: Error) => {
                if (!done) {
                    done = true;
                    reject(err);
                }
            };
            stream
                .pipe(parser)
                .on("data", (row: string[]) => {
                    finish(row.map((c) => c.trim()).filter(Boolean));
                })
                .on("error", (error) => {
                    if ((error as Error).name === "AbortError") return;
                    fail(new Error(`TSV parse error: ${error.message}`));
                })
                .on("end", () => finish([]));
        });
    } catch (error) {
        if (error instanceof Error && error.name === "AbortError") {
            throw new Error("Timeout when fetching TSV");
        }
        throw new Error(
            `Failed to fetch TSV: ${error instanceof Error ? error.message : String(error)}`,
        );
    } finally {
        clearTimeout(timeoutId);
    }
}

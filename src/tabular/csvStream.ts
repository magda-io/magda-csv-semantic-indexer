import { parse } from "csv-parse";
import fse from "fs-extra";
import path from "path";
import { Readable } from "stream";
import type { ReadableStream as WebReadableStream } from "stream/web";
import { __dirname as getCurDirPath } from "@magda/esm-utils";

const __dirname = getCurDirPath();
const CONNECTION_TIMEOUT_SEC = 120;

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

export async function getColumnsFromCSVStream(
    downloadURL: string,
    timeout: number = CONNECTION_TIMEOUT_SEC * 1000,
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
            delimiter: [",", ";", "\t", "|"],
            from_line: 1,
            to_line: 1,
            columns: false,
        });

        const csvStream = toNodeReadable(res.body);

        const cols = await new Promise<string[]>((resolve, reject) => {
            let resolved = false;

            const handleResolve = (columns: string[]) => {
                if (!resolved) {
                    resolved = true;
                    resolve(columns);
                }
            };

            const handleReject = (error: Error) => {
                if (!resolved) {
                    resolved = true;
                    reject(new Error(`CSV parsing error: ${error.message}`));
                }
            };

            csvStream
                .pipe(parser)
                .on("data", (row: string[]) => {
                    const columns = row.map((col) => col.trim()).filter(Boolean);
                    handleResolve(columns);
                })
                .on("error", (error) => {
                    if (error.name === "AbortError") {
                        return;
                    }
                    handleReject(error);
                })
                .on("end", () => {
                    handleResolve([]);
                });
        });
        return cols;
    } catch (error) {
        if (error instanceof Error && error.name === "AbortError") {
            throw new Error("Timeout when fetching CSV");
        }
        throw new Error(
            `Failed to fetch CSV: ${error instanceof Error ? error.message : String(error)}`,
        );
    } finally {
        clearTimeout(timeoutId);
    }
}

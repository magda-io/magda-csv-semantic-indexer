import {
    CreateEmbeddingText,
    Record,
    AuthorizedRegistryClient as Registry
} from "@magda/semantic-indexer-sdk";
import { openSync, readSync, closeSync, existsSync } from "fs";
import { basename } from "path";
import { toYaml } from "./utils/toYaml.js";
import { parse } from "csv-parse";
import fse from "fs-extra";
import path from "path";
import { Readable } from "stream";
import type { ReadableStream as WebReadableStream } from "stream/web";
import { __dirname as getCurDirPath } from "@magda/esm-utils";

const __dirname = getCurDirPath();

export function readFirstBytes(path: string, bytes: number): string {
    const fd = openSync(path, "r");
    const buf = Buffer.alloc(bytes);
    const len = readSync(fd, buf, 0, bytes, 0);
    closeSync(fd);
    return buf.toString("utf8", 0, len);
}

export async function getDatasetRecord(
    distributionId: string,
    registry: Registry
): Promise<Record | null> {
    try {
        const result = await registry.getRecords<Record>(
            ["dataset-distributions"],
            undefined,
            undefined,
            true,
            undefined,
            ["dataset-distributions.distributions:<|" + distributionId]
        );

        if ("records" in result) {
            return (result.records[0]) || null;
        }
        return null;
    } catch (e) {
        console.error(`Unexpected error when getting dataset record`, e);
        return null;
    }
}

export function formatTemporal(tc?: { start?: string; end?: string }): string {
    if (!tc) return "";

    const toDate = (s?: string) => (s ? new Date(s.replace(" ", "T")) : undefined);
    const fmt = (d: Date) =>
        d.toLocaleDateString("en-AU", { day: "numeric", month: "short", year: "numeric" });

    const start = toDate(tc.start);
    const end = toDate(tc.end);

    if (start && end) {
        return `from ${fmt(start)} to ${fmt(end)}`;
    }

    if (start) return `from ${fmt(start)}`;
    if (end) return `up to ${fmt(end)}`;
    return "";
}

const CONNECTION_TIMEOUT = 120;

function toNodeReadable(body: any): NodeJS.ReadableStream {
    return typeof body?.pipe === "function"
        ? body
        : Readable.fromWeb(body as WebReadableStream<Uint8Array>);
}

let cachedPackageInfo: { name: string; version: string } | null = null;

async function getPackageInfo() {
    if (!cachedPackageInfo) {
        cachedPackageInfo = await fse.readJSON(path.resolve(__dirname, "../package.json"), {
            encoding: "utf-8"
        });
    }
    return cachedPackageInfo;
}

export async function getColumnsFromCSVStream(downloadURL: string, timeout: number = CONNECTION_TIMEOUT * 1000): Promise<string[]> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    const pkg = await getPackageInfo();

    try {
        const res = await fetch(downloadURL, {
            redirect: "follow",
            headers: {
                "User-Agent": `${pkg?.name}/${pkg?.version}`
            },
            signal: controller.signal
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
            columns: false
        });

        const csvStream: NodeJS.ReadableStream = toNodeReadable(res.body);

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
                    const columns = row.map(col => col.trim()).filter(Boolean);
                    handleResolve(columns);
                })
                .on("error", (error) => {
                    if (error.name === 'AbortError') {
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
        if (error instanceof Error && error.name === 'AbortError') {
            throw new Error('Timeout when fetching CSV');
        }
        throw new Error(`Failed to fetch CSV: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
        clearTimeout(timeoutId);
    }
}

export const createEmbeddingText: CreateEmbeddingText = async ({
    record,
    format,
    filePath,
    url,
    readonlyRegistry
}) => {
    const dist = record.aspects?.["dcat-distribution-strings"] ?? {};
    let title = (dist.title ?? "").trim();
    let description = (dist.description ?? "").trim();
    const licence = (dist.license ?? "").trim();
    const fileName = url ? basename(url.split("?")[0]) : "";
    let temporalCoverage = "";
    let publisher = "";
    let themes = [];
    let keywords = [];
    let languages = [];

    if (readonlyRegistry) {
        const dataset = await getDatasetRecord(
            record.id,
            readonlyRegistry
        );

        const dsStr = dataset?.aspects?.["dcat-dataset-strings"];

        if (dsStr) {
            if (!title && dsStr.title) title = dsStr.title.trim();
            if (!description && dsStr.description) description = dsStr.description.trim();
            if (!publisher && dsStr.publisher) publisher = dsStr.publisher.trim();

            themes = Array.isArray(dsStr.themes) ? dsStr.themes.filter(Boolean) : [];
            keywords = Array.isArray(dsStr.keywords) ? dsStr.keywords.filter(Boolean) : [];
            languages = Array.isArray(dsStr.languages) ? dsStr.languages.filter(Boolean) : [];

            const tc = dsStr.temporal;
            if (tc?.start || tc?.end) {
                temporalCoverage = formatTemporal(tc);
            }
        }
    }

    let columns: string[] = [];
    if (filePath && existsSync(filePath)) {
        const header = readFirstBytes(filePath, 64 * 1024).split(/\r?\n/)[0] ?? "";
        columns = header.split(",").map(c => c.trim()).filter(Boolean);
    } else if (url) {
        try {
            columns = await getColumnsFromCSVStream(url);
        } catch (e) {
            if (e instanceof Error) {
                console.warn("Failed to get columns from CSV file", e.message);
            }
            columns = [];
        }
    }

    const yamlText = toYaml({
        Title: title || undefined,
        Format: format || "CSV",
        "File name": fileName || undefined,
        "Temporal coverage": temporalCoverage || undefined,
        Licence: licence || undefined,
        Publisher: publisher || undefined,
        Themes: themes || undefined,
        Keywords: keywords || undefined,
        Languages: languages || undefined,
        Description: description || undefined,
        "Column names": columns || undefined
    });

    return { text: yamlText };
};

import { existsSync } from "fs";
import { inferTabularKind } from "./inferFormat.js";
import type { TabularTableDescriptor } from "./types.js";
import {
    readExcelDescriptorsFromBuffer,
    readExcelDescriptorsFromPath,
} from "./excelDescriptors.js";
import { fetchBinary } from "./fetchBinary.js";
import { getColumnsFromTSVStream } from "./tsvStream.js";
import { readFirstBytes } from "./readFirstBytes.js";
import { getColumnsFromCSVStream } from "./csvStream.js";

export async function resolveTabularDescriptors(
    filePath: string | undefined,
    url: string | undefined,
    declaredFormat: string | undefined,
): Promise<TabularTableDescriptor[]> {
    const kind = inferTabularKind(declaredFormat, filePath, url);

    if (kind === "xlsx" || kind === "xls") {
        if (filePath && existsSync(filePath)) {
            return readExcelDescriptorsFromPath(filePath);
        }
        if (url) {
            try {
                const buf = await fetchBinary(url);
                return readExcelDescriptorsFromBuffer(buf);
            } catch {
                return [];
            }
        }
        return [];
    }

    if (kind === "tsv") {
        if (filePath && existsSync(filePath)) {
            const line =
                readFirstBytes(filePath, 64 * 1024).split(/\r?\n/)[0] ?? "";
            const columns = line
                .split("\t")
                .map((c) => c.trim())
                .filter(Boolean);
            return [{ sheetName: "", columns }];
        }
        if (url) {
            const columns = await getColumnsFromTSVStream(url);
            return [{ sheetName: "", columns }];
        }
        return [];
    }

    if (kind === "csv" || kind === null) {
        if (filePath && existsSync(filePath)) {
            const line =
                readFirstBytes(filePath, 64 * 1024).split(/\r?\n/)[0] ?? "";
            const columns = line
                .split(",")
                .map((c) => c.trim())
                .filter(Boolean);
            return [{ sheetName: "", columns }];
        }
        if (url) {
            try {
                const columns = await getColumnsFromCSVStream(url);
                return [{ sheetName: "", columns }];
            } catch {
                return [];
            }
        }
    }

    return [];
}

import fs from "fs";
import * as XLSX from "xlsx";
import type { TabularTableDescriptor } from "./types.js";

function dedupeColumnNames(headers: string[]): string[] {
    const counts = new Map<string, number>();
    return headers.map((h) => {
        const next = (counts.get(h) ?? 0) + 1;
        counts.set(h, next);
        if (next === 1) return h;
        return `${h}_${next}`;
    });
}

function visibleSheetNames(workbook: XLSX.WorkBook): string[] {
    const names = workbook.SheetNames || [];
    const meta = workbook.Workbook?.Sheets;
    if (!meta || meta.length !== names.length) {
        return names;
    }
    return names.filter((_n, i) => {
        const s = meta[i] as { Hidden?: number | boolean } | undefined;
        if (!s) return true;
        return s.Hidden !== 1 && s.Hidden !== true;
    });
}

function headerRowFromSheet(
    sheet: XLSX.WorkSheet,
    rowIndex: number,
): string[] {
    const ref = sheet["!ref"];
    if (!ref) {
        return [];
    }
    const range = XLSX.utils.decode_range(ref);
    if (rowIndex < range.s.r || rowIndex > range.e.r) {
        return [];
    }
    const headers: string[] = [];
    for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({ r: rowIndex, c });
        const cell = sheet[addr] as XLSX.CellObject | undefined;
        const raw =
            cell?.w ??
            (cell?.v !== undefined && cell?.v !== null ? String(cell.v) : "");
        headers.push(raw.trim());
    }
    while (headers.length > 0 && headers[headers.length - 1] === "") {
        headers.pop();
    }
    const nonEmpty = headers.filter(Boolean);
    return dedupeColumnNames(nonEmpty);
}

export function readExcelDescriptorsFromBuffer(buf: Buffer): TabularTableDescriptor[] {
    const workbook = XLSX.read(buf, { type: "buffer", cellDates: true });
    const out: TabularTableDescriptor[] = [];
    for (const sheetName of visibleSheetNames(workbook)) {
        const sheet = workbook.Sheets[sheetName];
        if (!sheet) continue;
        const columns = headerRowFromSheet(
            sheet,
            sheet["!ref"] ? XLSX.utils.decode_range(sheet["!ref"]).s.r : 0,
        );
        if (columns.length === 0) {
            continue;
        }
        out.push({ sheetName, columns });
    }
    return out;
}

export function readExcelDescriptorsFromPath(filePath: string): TabularTableDescriptor[] {
    const buf = fs.readFileSync(filePath);
    return readExcelDescriptorsFromBuffer(buf);
}

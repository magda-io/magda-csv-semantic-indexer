import type { TabularKind } from "./types.js";

function extOf(pathOrUrl: string): string {
    const base = pathOrUrl.split("?")[0].split("#")[0];
    const i = base.lastIndexOf(".");
    return i >= 0 ? base.slice(i).toLowerCase() : "";
}

/**
 * Infer tabular file kind from declared DCAT/Media type text and path or URL.
 */
export function inferTabularKind(
    declaredFormat: string | undefined,
    filePath: string | undefined,
    url: string | undefined,
): TabularKind | null {
    const fmt = (declaredFormat ?? "").toLowerCase();
    const pathOrUrl = (url || filePath || "").trim();
    const ext = pathOrUrl ? extOf(pathOrUrl) : "";

    if (ext === ".tsv" || fmt.includes("tsv") || fmt.includes("tab-separated")) {
        return "tsv";
    }
    if (ext === ".xlsx" || ext === ".xlsm" || fmt.includes("xlsx")) {
        return "xlsx";
    }
    if (
        ext === ".xls" ||
        (fmt.includes("xls") && !fmt.includes("xlsx"))
    ) {
        return "xls";
    }
    if (
        fmt.includes("excel") ||
        fmt.includes("spreadsheetml") ||
        fmt.includes("spreadsheet")
    ) {
        return "xlsx";
    }
    if (ext === ".csv" || fmt.includes("csv")) {
        return "csv";
    }
    return null;
}

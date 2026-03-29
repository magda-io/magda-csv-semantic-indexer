import {
    CreateEmbeddingText,
    Record as MagdaRecord,
    AuthorizedRegistryClient as Registry,
} from "@magda/semantic-indexer-sdk";
import { basename } from "path";
import { toYaml } from "./utils/toYaml.js";
import { resolveTabularDescriptors } from "./tabular/resolveDescriptors.js";
import type { TabularTableDescriptor } from "./tabular/types.js";

export { readFirstBytes } from "./tabular/readFirstBytes.js";
export { getColumnsFromCSVStream } from "./tabular/csvStream.js";

export async function getDatasetRecord(
    distributionId: string,
    registry: Registry,
): Promise<MagdaRecord | null> {
    try {
        const result = await registry.getRecords<MagdaRecord>(
            ["dataset-distributions"],
            undefined,
            undefined,
            true,
            undefined,
            ["dataset-distributions.distributions:<|" + distributionId],
        );

        if ("records" in result) {
            return result.records[0] || null;
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

function applyYamlTabularFields(
    payload: { [key: string]: unknown },
    descriptors: TabularTableDescriptor[],
): void {
    if (descriptors.length === 0) {
        return;
    }
    if (descriptors.length === 1) {
        const d = descriptors[0];
        if (d.columns.length > 0) {
            payload["Column names"] = d.columns;
        }
        if (d.sheetName) {
            payload["Sheet"] = d.sheetName;
        }
        return;
    }
    payload["Tables"] = descriptors.map((d) => ({
        sheet: d.sheetName,
        columns: d.columns,
    }));
}

export const createEmbeddingText: CreateEmbeddingText = async ({
    record,
    format,
    filePath,
    url,
    readonlyRegistry,
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
        const dataset = await getDatasetRecord(record.id, readonlyRegistry);

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

    const descriptors = await resolveTabularDescriptors(
        filePath ?? undefined,
        url ?? undefined,
        format ?? undefined,
    );

    const payload: { [key: string]: unknown } = {
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
    };

    applyYamlTabularFields(payload, descriptors);

    const yamlText = toYaml(payload);

    return { text: yamlText };
};

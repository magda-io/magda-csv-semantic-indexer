import path from "path";
import fse from "fs-extra";
import { __dirname as getCurDirPath } from "@magda/esm-utils";

const __dirname = getCurDirPath();
const DEFAULT_TIMEOUT_MS = 120_000;

let cachedPackageInfo: { name: string; version: string } | null = null;

async function getPackageInfo() {
    if (!cachedPackageInfo) {
        cachedPackageInfo = await fse.readJSON(path.resolve(__dirname, "../../package.json"), {
            encoding: "utf-8",
        });
    }
    return cachedPackageInfo;
}

export async function fetchBinary(
    downloadURL: string,
    timeoutMs: number = DEFAULT_TIMEOUT_MS,
): Promise<Buffer> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
    const pkg = await getPackageInfo();
    try {
        const res = await fetch(downloadURL, {
            redirect: "follow",
            headers: {
                "User-Agent": `${pkg?.name}/${pkg?.version}`,
            },
            signal: controller.signal,
        });
        if (!res.ok) {
            throw new Error(`HTTP ${res.status} fetching ${downloadURL}`);
        }
        const ab = await res.arrayBuffer();
        return Buffer.from(ab);
    } catch (e) {
        if (e instanceof Error && e.name === "AbortError") {
            throw new Error("Timeout when downloading file");
        }
        throw e;
    } finally {
        clearTimeout(timeoutId);
    }
}

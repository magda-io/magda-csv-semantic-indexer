import { expect } from "chai";
import { inferTabularKind } from "../tabular/inferFormat.js";

describe("inferTabularKind", () => {
    it("detects TSV from extension and format hints", () => {
        expect(inferTabularKind(undefined, "/a/b.tsv", undefined)).to.equal("tsv");
        expect(inferTabularKind("Tab-separated values", undefined, undefined)).to.equal(
            "tsv",
        );
    });

    it("detects Excel formats", () => {
        expect(inferTabularKind(undefined, "c:\\data.xlsx", undefined)).to.equal("xlsx");
        expect(inferTabularKind(undefined, "macro.xlsm", undefined)).to.equal("xlsx");
        expect(inferTabularKind(undefined, "legacy.xls", undefined)).to.equal("xls");
        expect(inferTabularKind("Microsoft Excel", undefined, undefined)).to.equal("xlsx");
        expect(
            inferTabularKind(
                undefined,
                undefined,
                "https://example.org/file.xls?x=1",
            ),
        ).to.equal("xls");
    });

    it("detects CSV", () => {
        expect(inferTabularKind("CSV", undefined, undefined)).to.equal("csv");
        expect(inferTabularKind(undefined, "/tmp/x.csv", undefined)).to.equal("csv");
    });

    it("returns null when unknown", () => {
        expect(inferTabularKind(undefined, undefined, undefined)).to.equal(null);
        expect(inferTabularKind("JSON", "/no/ext", undefined)).to.equal(null);
    });
});

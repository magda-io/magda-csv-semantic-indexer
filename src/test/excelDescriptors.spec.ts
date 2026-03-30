import { expect } from "chai";
import * as XLSX from "xlsx";
import { readExcelDescriptorsFromBuffer } from "../tabular/excelDescriptors.js";

describe("readExcelDescriptorsFromBuffer", () => {
    it("reads multiple sheets with header rows", () => {
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(
            wb,
            XLSX.utils.aoa_to_sheet([
                ["Region", "Sales"],
                ["AU", 100],
            ]),
            "Q1",
        );
        XLSX.utils.book_append_sheet(
            wb,
            XLSX.utils.aoa_to_sheet([["Code"], ["X"]]),
            "Refs",
        );
        const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" }) as Buffer;
        const tables = readExcelDescriptorsFromBuffer(buf);
        expect(tables).to.have.length(2);
        expect(tables[0]).to.deep.include({
            sheetName: "Q1",
            columns: ["Region", "Sales"],
        });
        expect(tables[1]).to.deep.include({
            sheetName: "Refs",
            columns: ["Code"],
        });
    });

    it("stabilises duplicate header labels with numeric suffixes", () => {
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(
            wb,
            XLSX.utils.aoa_to_sheet([["id", "name", "name", "total"]]),
            "Dupes",
        );
        const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" }) as Buffer;
        const tables = readExcelDescriptorsFromBuffer(buf);
        expect(tables[0].columns).to.deep.equal(["id", "name", "name_2", "total"]);
    });

    it("omits sheets whose first row has no header cells", () => {
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(
            wb,
            XLSX.utils.aoa_to_sheet([
                ["", "", ""],
                ["x", "y"],
            ]),
            "NoHeaders",
        );
        XLSX.utils.book_append_sheet(
            wb,
            XLSX.utils.aoa_to_sheet([["A", "B"], [1, 2]]),
            "HasHeaders",
        );
        const buf = XLSX.write(wb, { type: "buffer", bookType: "xlsx" }) as Buffer;
        const tables = readExcelDescriptorsFromBuffer(buf);
        expect(tables).to.have.length(1);
        expect(tables[0]).to.deep.include({
            sheetName: "HasHeaders",
            columns: ["A", "B"],
        });
    });
});

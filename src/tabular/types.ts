export type TabularKind = "csv" | "tsv" | "xlsx" | "xls";

export type TabularTableDescriptor = {
    sheetName: string;
    columns: string[];
};

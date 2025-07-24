import { expect } from "chai";
import { parse as parseYaml } from "yaml";
import * as fs from "fs";
import { join, basename } from "path";
import { tmpdir } from "os";
import { createEmbeddingText, formatTemporal } from "../createEmbeddingText.js";

describe("createEmbeddingText", () => {
    let tempFile: string;

    before(() => {
        tempFile = join(tmpdir(), "file.csv");
        fs.writeFileSync(tempFile, "colA,colB,colC\n1,2,3\n4,5,6");
    });

    after(() => fs.unlinkSync(tempFile));

    it("Should generate embedding text correctly", async () => {
        const mockRegistry = {
            getRecords: async () => ({
                records: [
                    {
                        aspects: {
                            "dcat-dataset-strings": {
                                title: "Dataset Title",
                                description: "Dataset Desc",
                                publisher: "Publisher",
                                themes: ["Theme1", "Theme2"],
                                keywords: ["Keyword1", "Keyword2"],
                                languages: ["en", "zh"],
                                temporal: {
                                    start: "2014-07-01 00:00:00",
                                    end: "2023-06-30 00:00:00"
                                }
                            }
                        }
                    }
                ]
            })
        };

        const record = {
            id: "dist-1",
            aspects: { "dcat-distribution-strings": { license: "MIT" } },
            name: "dist-1",
            sourceTag: "src",
            tenantId: 0
        };

        const { text } = await createEmbeddingText({
            record,
            format: "CSV",
            filePath: tempFile,
            url: "http://example.com/data.csv",
            readonlyRegistry: mockRegistry as any
        });

        const yamlObj = parseYaml(text);

        const expected = {
            Title: "Dataset Title",
            Format: "CSV",
            Description: "Dataset Desc",
            "File name": "data.csv",
            "Temporal coverage": "from 1 July 2014 to 30 June 2023",
            Licence: "MIT",
            Publisher: "Publisher",
            Themes: ["Theme1", "Theme2"],
            Keywords: ["Keyword1", "Keyword2"],
            Languages: ["en", "zh"],
            Columns: ["colA", "colB", "colC"]
        };

        expect(yamlObj).to.deep.equal(expected);
    });

    it("should not add to embedding text if fields are missing in the record", async () => {
        const mockRegistry = {
            getRecords: async () => ({
                records: [
                    {
                        aspects: {
                            "dcat-dataset-strings": {
                                title: "Dataset Title"
                            }
                        }
                    }
                ]
            })
        };

        const record = {
            id: "dist-1",
            aspects: {
                "dcat-distribution-strings": {
                    // missing most of the fields
                }
            },
            name: "dist-1",
            sourceTag: "src",
            tenantId: 0
        };

        const { text } = await createEmbeddingText({
            record,
            format: "CSV",
            filePath: tempFile,
            url: "",
            readonlyRegistry: mockRegistry as any
        });

        const yamlObj = parseYaml(text);

        const expected = {
            Title: "Dataset Title",
            Format: "CSV",
            Columns: ["colA", "colB", "colC"]
        };

        expect(yamlObj).to.deep.equal(expected);
    });

    it("should fallback to dataset's title/description if missing from distribution record", async () => {
        const mockEmpty = {
            getRecords: async () => ({
                records: [
                    {
                        aspects: {
                            "dcat-dataset-strings": {
                                title: "Dataset Title",
                                description: "Dataset Desc"
                            }
                        }
                    }
                ]
            })
        };

        const distributionRecord = {
            id: "dist-2",
            aspects: {
                "dcat-distribution-strings": {}
            },
            name: "dist-2",
            sourceTag: "src",
            tenantId: 0
        };

        const { text } = await createEmbeddingText({
            record: distributionRecord,
            format: "CSV",
            filePath: tempFile,
            url: "http://example.com/data.csv",
            readonlyRegistry: mockEmpty as any
        });

        const yamlObj = parseYaml(text);

        const expected = {
            Title: "Dataset Title",
            Format: "CSV",
            "File name": "data.csv",
            Description: "Dataset Desc",
            Columns: ["colA", "colB", "colC"]
        };

        expect(yamlObj).to.deep.equal(expected);
    });

    it("should handle temporal coverage with only start date", async () => {
        const testCases = [
            {
                temporal: {
                    start: "2020-01-01 00:00:00",
                    end: "2022-03-05 00:00:00"
                },
                expected: "from 1 Jan 2020 to 5 Mar 2022"
            },
            {
                temporal: {
                    start: "2020-01-01 00:00:00",
                    end: undefined
                },
                expected: "from 1 Jan 2020"
            },
            {
                temporal: {
                    start: undefined,
                    end: "2020-01-01 00:00:00"
                },
                expected: "up to 1 Jan 2020"
            },
            {
                temporal: {
                    start: undefined,
                    end: undefined
                },
                expected: ""
            }
        ];

        testCases.forEach(({ temporal, expected }) => {
            const result = formatTemporal(temporal);
            expect(result).to.equal(expected);
        });
    });

    it("should correctly handle description with newlines", async () => {
        const mockRegistry = {
            getRecords: async () => ({
                records: [
                    {
                        aspects: {
                            "dcat-dataset-strings": {
                                title: "Dataset Title",
                                description: "First line\nSecond line",
                                publisher: "Publisher"
                            }
                        }
                    }
                ]
            })
        };

        const record = {
            id: "dist-3",
            aspects: { "dcat-distribution-strings": {} },
            name: "dist-3",
            sourceTag: "src",
            tenantId: 0
        };

        const { text } = await createEmbeddingText({
            record,
            format: "CSV",
            filePath: tempFile,
            url: "http://example.com/data.csv",
            readonlyRegistry: mockRegistry as any
        });

        const yamlObj = parseYaml(text);

        expect(yamlObj.Description).to.equal("First line\nSecond line");

    });
});

import { expect } from "chai";
import { Chunker } from "../chunker.js";

describe("Chunker", () => {
    it("should chunk yaml text with proper overlapping", async () => {
        const text = `Title: Dataset Title
Publisher: Example Publisher
Themes: [Environment, Climate]
Keywords: [weather, rainfall]
Languages: [en, zh]
Description: This is a long description for testing chunker logic
Columns:
  - column1
  - column2
  - column3
  - column4
  - column5
`;
        const chunker = new Chunker(10, 3);
        const chunks = await chunker.chunk(text);

        expect(chunks).to.not.be.empty;
        
        chunks.forEach((chunk) => {
            expect(chunk.length).to.equal(chunk.text.length);
            expect(text.slice(chunk.position, chunk.position + chunk.length))
                .to.equal(chunk.text);
        });

        const reconstructed = chunks.map(chunk => 
            chunk.text.slice(chunk.overlap)
        ).join('');
        expect(reconstructed).to.equal(text);
    });

    it("should chunk yaml text with newline characters in description", async () => {
        const text = `
Title: Another Dataset
Description: This is a very long description\nthat will be chunked into\nmultiple chunks.
Publisher: Test Publisher
Themes: [Data, Analysis]
Columns:
  - column1
  - column2
  - column3
  - column4
  - column5
`;
        const chunker = new Chunker(40, 8);
        const chunks = await chunker.chunk(text);

        expect(chunks).to.not.be.empty;
        chunks.forEach((chunk) => {
            expect(chunk.length).to.equal(chunk.text.length);
            expect(text.slice(chunk.position, chunk.position + chunk.length))
                .to.equal(chunk.text);
        });

        const reconstructed = chunks.map(chunk => 
            chunk.text.slice(chunk.overlap)
        ).join('');
        expect(reconstructed).to.equal(text);
    });

    it("should handle text smaller than chunk size", async () => {
        const text = `Title: Small Dataset`;
        const chunker = new Chunker(50, 5);
        const chunks = await chunker.chunk(text);
        
        expect(chunks).to.deep.equal([{
            text,
            length: text.length,
            position: 0,
            overlap: 0
        }]);
    });

    it("should throw error when overlap >= chunk size", () => {
        expect(() => new Chunker(20, 25)).to.throw("Overlap must be smaller than chunk size");
    });

    it("should handle empty text", async () => {
        const chunker = new Chunker(50, 5);
        const chunks = await chunker.chunk("");
        expect(chunks).to.be.empty;
    });
});

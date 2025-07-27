import {
    ChunkResult,
} from "@magda/semantic-indexer-sdk";
import { RecursiveCharacterTextSplitter } from "@langchain/textsplitters";

export class Chunker {
    private chunkSize: number;
    private overlap: number;
    private splitter: RecursiveCharacterTextSplitter;

    constructor(chunkSize: number, overlap: number) {
        if (overlap >= chunkSize) {
            throw new Error("Overlap must be smaller than chunk size");
        }
        this.chunkSize = chunkSize * 4;
        this.overlap = overlap * 4;
        this.splitter = new RecursiveCharacterTextSplitter({
            separators: [
                "\nColumns",
                "\nDescription",
                "\nTitle",
                "\nKeywords",
                "\nThemes",
                "\nTemporal coverage",
                "\nLanguages",
                "\nPublisher",
                "\n",
                " ",
                ""
            ],
            chunkSize: this.chunkSize,
            chunkOverlap: this.overlap,
            keepSeparator: true,
            stripWhitespace: false,
        });
    }

    async chunk(text: string): Promise<ChunkResult[]> {
        if (!text) {
            return [];
        }

        if (text.length <= this.chunkSize) {
            return [{
                text: text,
                position: 0,
                length: text.length,
                overlap: 0
            }];
        }

        const chunks = await this.splitter.splitTextWithMetadata(text);

        for (let i = 1; i < chunks.length; i++) {
            if (chunks[i].overlap === 0 && chunks[i].text.trim() === '') {
                chunks[i-1].text = chunks[i-1].text + chunks[i].text;
                chunks[i-1].length = chunks[i-1].length + chunks[i].length;
                chunks.splice(i, 1);
                i--;
            }
        }

        return chunks;
    }
}
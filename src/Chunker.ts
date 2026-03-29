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
                "\nTables:",
                "\nSheet:",
                "\nColumn names:",
                "\nDescription:",
                "\nTitle:",
                "\nKeywords:",
                "\nThemes:",
                "\nTemporal coverage:",
                "\nLanguages:",
                "\nPublisher:",
                "\n",
                " ",
                ""
            ],
            chunkSize: this.chunkSize,
            chunkOverlap: this.overlap,
            keepSeparator: true,
        });
    }

    private async splitToChunkResults(text: string): Promise<ChunkResult[]> {
        const parts = await this.splitter.splitText(text);
        const chunks: ChunkResult[] = [];
        let indexPrevChunk = -1;
        for (const chunkText of parts) {
            const position = text.indexOf(chunkText, indexPrevChunk + 1);
            if (position === -1) {
                throw new Error(
                    `Chunk not found in source text after index ${indexPrevChunk}`,
                );
            }
            chunks.push({
                text: chunkText,
                position,
                length: chunkText.length,
                overlap: 0,
            });
            indexPrevChunk = position;
        }
        if (chunks.length && chunks[0].position > 0) {
            const pre = text.slice(0, chunks[0].position);
            const h = chunks[0];
            h.text = pre + h.text;
            h.position = 0;
            h.length = h.text.length;
        }
        for (let i = 1; i < chunks.length; i++) {
            const prev = chunks[i - 1];
            const prevEnd = prev.position + prev.length;
            const cur = chunks[i];
            if (cur.position > prevEnd) {
                const gap = text.slice(prevEnd, cur.position);
                cur.text = gap + cur.text;
                cur.position = prevEnd;
                cur.length = cur.text.length;
            }
            cur.overlap = prevEnd - cur.position;
        }
        const last = chunks[chunks.length - 1];
        if (last) {
            const end = last.position + last.length;
            if (end < text.length) {
                last.text += text.slice(end);
                last.length = last.text.length;
            }
        }
        return chunks;
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

        const chunks = await this.splitToChunkResults(text);

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
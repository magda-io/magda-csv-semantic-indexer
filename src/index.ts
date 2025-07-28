import semanticIndexer, {
    SemanticIndexerOptions,
    commonYargs,
    ChunkStrategyType,
} from "@magda/semantic-indexer-sdk";
import { csvSemanticIndexerArgs } from "./csvSemanticIndexerArgs.js";
import { createEmbeddingText } from "./createEmbeddingText.js";
import { Chunker } from "./Chunker.js";

const port = csvSemanticIndexerArgs.port;
const args = commonYargs(port, `http://localhost:${port}`);

const chunker = new Chunker(csvSemanticIndexerArgs.chunkSizeLimit, csvSemanticIndexerArgs.overlap);

const chunkStrategy: ChunkStrategyType = async (text: string) => {
    return await chunker.chunk(text);
};

const options: SemanticIndexerOptions = {
    argv: args,
    id: csvSemanticIndexerArgs.id,
    itemType: "storageObject",
    formatTypes: ["csv"],
    autoDownloadFile: false,
    createEmbeddingText: createEmbeddingText,
    chunkStrategy: chunkStrategy,
};

semanticIndexer(options);
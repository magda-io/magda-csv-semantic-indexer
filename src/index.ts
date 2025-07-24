import semanticIndexer, {
    SemanticIndexerOptions,
    commonYargs,
} from "@magda/semantic-indexer-sdk";
import { csvSemanticIndexerArgs } from "./csvSemanticIndexerArgs.js";
import { createEmbeddingText } from "./createEmbeddingText.js";

const port = csvSemanticIndexerArgs.port;
const args = commonYargs(port, `http://localhost:${port}`);

const options: SemanticIndexerOptions = {
    argv: args,
    id: csvSemanticIndexerArgs.id,
    itemType: "storageObject",
    formatTypes: ["csv"],
    autoDownloadFile: true,
    createEmbeddingText: createEmbeddingText
};

semanticIndexer(options);
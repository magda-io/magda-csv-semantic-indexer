import yargs from "yargs";

export const csvSemanticIndexerArgs = yargs(process.argv.slice(2))
    .help()
    .option('id', {
        type: 'string',
        default: 'csv-semantic-indexer',
        description: 'Semantic indexer service ID'
    })
    .option('chunkSizeLimit', {
        type: 'number',
        default: 512,
        description: 'Maximum size limit for text chunks'
    })
    .option('overlap', {
        type: 'number',
        default: 64,
        description: 'Number of overlapping characters between chunks'
    })
    .option('port', {
        type: 'number',
        default: process.env.PORT ? parseInt(process.env.PORT) : 6305,
        description: 'Service port'
    })
    .parseSync();
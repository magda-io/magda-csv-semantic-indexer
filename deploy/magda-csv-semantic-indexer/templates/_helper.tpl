{{- define "magda-csv-semantic-indexer.semanticIndexer.values" }}
  {{- $semanticIndexer := get .Values "semanticIndexer" | default dict }}
  {{- $globalSemanticIndexer := get .Values.global.searchEngine "semanticIndexer" | default dict }}
  {{- $defaultConfig := .Values.defaultSemanticIndexerConfig }}

  {{- $id := .Values.semanticIndexer.id | default $defaultConfig.id }}
  {{- $indexVersion := .Values.semanticIndexer.indexVersion | default (get $globalSemanticIndexer "indexVersion") | default $defaultConfig.indexVersion }}
  {{- $actualIndexName := .Values.semanticIndexer.indexName | default (get $globalSemanticIndexer "indexName") | default $defaultConfig.indexName }}
  {{- $chunkSizeLimit := .Values.semanticIndexer.chunkSizeLimit | default $defaultConfig.chunkSizeLimit }}
  {{- $overlap := .Values.semanticIndexer.overlap | default $defaultConfig.overlap }}
  {{- $bulkEmbeddingsSize := .Values.semanticIndexer.bulkEmbeddingsSize | default $defaultConfig.bulkEmbeddingsSize }}
  {{- $bulkIndexSize := .Values.semanticIndexer.bulkIndexSize | default $defaultConfig.bulkIndexSize }}

  {{- $_ := set $semanticIndexer "id" $id }}
  {{- $_ := set $semanticIndexer "numberOfShards" (get $globalSemanticIndexer "numberOfShards") }}
  {{- $_ := set $semanticIndexer "numberOfReplicas" (get $globalSemanticIndexer "numberOfReplicas") }}
  {{- $_ := set $semanticIndexer "knnVectorFieldConfig" (get $globalSemanticIndexer "knnVectorFieldConfig") }}

  {{- $_ := set $semanticIndexer "indexName" $actualIndexName }}
  {{- $_ := set $semanticIndexer "indexVersion" $indexVersion }}
  {{- $_ := set $semanticIndexer "chunkSizeLimit" $chunkSizeLimit }}
  {{- $_ := set $semanticIndexer "overlap" $overlap }}
  {{- $_ := set $semanticIndexer "bulkEmbeddingsSize" $bulkEmbeddingsSize }}
  {{- $_ := set $semanticIndexer "bulkIndexSize" $bulkIndexSize }}

  {{- $semanticIndexer | mustToRawJson }}
{{- end }}
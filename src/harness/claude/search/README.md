## Concept Atlas
```mermaid
mindmap
  root((transcript search))
    What is searched
      passages turns each transcript entry into its passages by rule from the entry's own structure and never by hand
      every string an entry holds is searched except machine values such as ids digests signatures timestamps base64 and one-word labels outside a message's content and except the toolUseResult and mcpMeta copies of what the message already holds
      a passage's kind is the entry type and for a message the content block type as Claude Code writes them such as user.text assistant.thinking assistant.tool_use user.tool_result attachment and queue-operation
      prose kinds are searched with English stemming and tool calls and their output as written so code paths and identifiers keep their spelling
      text longer than 2000 characters is split at the widest boundary it can and never between the halves of a surrogate pair
      NUL and unpaired surrogates become U+FFFD as the session store's storable does for its doc
    Where it lives
      schema keeps passages occurrences indexed embeddings and search_state beside the store's entries in claude_sessions
      each distinct text is one passage however often it is written and every place it is written is an occurrence with the entry's session subpath time and kind
      occurrences go with their entry when the SDK deletes it and passages no entry holds any more are dropped hourly
      passages carry a stored tsvector with a GIN index and a trigram GIN index and embeddings an HNSW index for cosine distance
    Indexer
      indexer reads entries not yet in indexed into passages and occurrences a batch of 200 per transaction holding shared locks so the SDK cannot delete them meanwhile
      appends commit out of seq order so it looks for unmarked entries and settled_seq only spares it old ones moving past entries older than ten minutes and stopping at the first not indexed
      a first start reads every stored entry and afterwards new entries are searchable within seconds
    Embedder
      embedder embeds passages of the conversation and its tool calls with pgrag's bge-small-en-v1.5 in the compute sixteen at a time and embeds tool output and harness text not at all
      it spends at most half its time embedding so the compute's CPU stays free
      without the model it embeds nothing and waits and a passage is recorded as unembeddable only when the model embeds others and fails on it alone
    Search
      claude_sessions.search is SQL any client calls such as the PostgreSQL platform skill
      it fuses full-text trigram and nearest-embedding matches by reciprocal rank weights them by kind conversation first tools next the harness's own text last and with rerank reorders them with pgrag's jina reranker
      filters by kinds sessions and time and each passage comes once at its most telling occurrence with how often it occurs and a highlighted snippet
      without pgrag's models it searches by words alone
    Isolation
      nothing here is on the SDK's path so an append never waits on search and if the indexer or embedder falls behind or fails it catches up when it can
```

## Preference Atlas
```mermaid
sequenceDiagram
  participant SDK as Agent SDK mirror
  participant Store as session-store
  participant Indexer as search/indexer
  participant Embedder as search/embedder
  participant Neon as Neon compute
  SDK->>Store: append entries
  Store->>Neon: insert entries
  Indexer->>Neon: read entries not yet indexed
  Indexer->>Neon: upsert passages and insert occurrences and indexed in one transaction
  Embedder->>Neon: embed passages wanting one with pgrag
  Neon-->>Embedder: embeddings or the model unavailable
  Note over Neon: claude_sessions.search fuses words trigrams and meaning
```

## Avoidance Atlas
```mermaid
stateDiagram-v2
  [*] --> Indexing
  Indexing --> OnTheSdkPath: search written in the append's transaction
  OnTheSdkPath --> MirrorBatchesDropped
  Indexing --> HighWaterMark: indexing past the highest seq seen
  HighWaterMark --> EntriesCommittedLateNeverIndexed
  Indexing --> HandLabels: kinds or importance decided by hand at index time
  HandLabels --> SearchMissesWhatNobodyLabelled
  Indexing --> MarkedWhileModelAway: passages marked unembeddable while the model is down
  MarkedWhileModelAway --> PassagesNeverEmbedded
  Indexing --> RuleDerivedPassages
  RuleDerivedPassages --> [*]
```

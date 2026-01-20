# FullTextIndex

Full-text search with tokenization, stemming, and BM25 relevance scoring.

## Overview

FullTextIndex provides text search capabilities using an inverted index structure. It supports multiple tokenization strategies, boolean queries (AND/OR), phrase search with position tracking, and BM25-based relevance ranking.

**Algorithms**:
- **Inverted Index**: Term → Document mapping for O(1) term lookup
- **BM25 Scoring**: Probabilistic relevance model (Robertson & Zaragoza)
- **Phrase Search**: Position-aware matching for exact sequences

**Storage Layout**:
```
[indexSubspace]/terms/[term]/[primaryKey] = Tuple(tf) or Tuple(pos1, pos2, ...)
[indexSubspace]/docs/[primaryKey] = Tuple(uniqueTermCount, docLength)
[indexSubspace]/stats/N = Int64 (total documents)
[indexSubspace]/stats/totalLength = Int64 (sum of document lengths)
[indexSubspace]/df/[term] = Int64 (document frequency)
```

## Use Cases

### 1. Article Search (Content Management)

**Scenario**: Search articles by content with relevance ranking.

```swift
@Persistable
struct Article {
    var id: String = ULID().ulidString
    var title: String = ""
    var content: String = ""
    var publishedAt: Date = Date()

    #Index<Article>(
        type: FullTextIndexKind(
            fields: [\.content],
            tokenizer: .simple,
            storePositions: true  // Enable phrase search
        )
    )
}

// Search for articles containing "machine learning"
let results = try await context.search(Article.self)
    .text(\.content)
    .query("machine learning")
    .matchMode(.all)  // AND query
    .limit(20)
    .execute()

// Phrase search (exact sequence)
let phraseResults = try await context.search(Article.self)
    .text(\.content)
    .query("machine learning")
    .matchMode(.phrase)
    .execute()
```

**Performance**: O(t) term lookups + O(d) document fetches, where t = terms, d = matching documents.

### 2. Product Search (E-commerce)

**Scenario**: Search products with multi-field indexing.

```swift
@Persistable
struct Product {
    var id: String = ULID().uuidString
    var name: String = ""
    var description: String = ""
    var category: String = ""

    // Index both name and description
    #Index<Product>(
        type: FullTextIndexKind(
            fields: [\.name, \.description],
            tokenizer: .simple
        )
    )
}

// Search across name and description
let results = try await context.search(Product.self)
    .text(\.name, \.description)
    .query("wireless bluetooth headphones")
    .matchMode(.any)  // OR query
    .execute()
```

### 3. Log Search (Observability)

**Scenario**: Search application logs with n-gram tokenization.

```swift
@Persistable
struct LogEntry {
    var id: String = ULID().ulidString
    var timestamp: Date = Date()
    var message: String = ""
    var level: String = ""

    #Index<LogEntry>(
        type: FullTextIndexKind(
            fields: [\.message],
            tokenizer: .ngram,
            ngramSize: 3  // Trigrams for partial matching
        )
    )
}

// Search logs with partial term matching
let results = try await context.search(LogEntry.self)
    .text(\.message)
    .query("err")  // Matches "error", "errors", etc.
    .execute()
```

### 4. Multi-language Search

**Scenario**: Search content with language-specific stemming.

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: String = ""
    var language: String = "en"

    #Index<Document>(
        type: FullTextIndexKind(
            fields: [\.content],
            tokenizer: .stem  // Snowball stemmer
        )
    )
}

// Stemmed search: "running" matches "run", "runs", "runner"
let results = try await context.search(Document.self)
    .text(\.content)
    .query("running")
    .execute()
```

### 5. Highlighted Search Results

**Scenario**: Display search results with highlighted matches.

```swift
// Search with highlighting
let results = try await context.search(Article.self)
    .text(\.content)
    .query("database performance")
    .highlight(config: HighlightConfig(
        preTag: "<mark>",
        postTag: "</mark>",
        maxFragments: 3,
        fragmentSize: 150
    ))
    .executeWithHighlights()

for (article, highlights) in results {
    print("Title: \(article.title)")
    for highlight in highlights {
        print("  ... \(highlight) ...")
    }
}
```

### 6. BM25 Relevance Ranking

**Scenario**: Rank search results by relevance using BM25.

```swift
// Search with BM25 scoring
let scored = try await context.search(Article.self)
    .text(\.content)
    .query("machine learning neural networks")
    .bm25(params: BM25Parameters(k1: 1.2, b: 0.75))
    .executeWithScores()

for (article, score) in scored {
    print("\(article.title): \(String(format: "%.2f", score))")
}
```

**BM25 Formula**:
```
BM25(D,Q) = Σ IDF(qi) × (tf(qi,D) × (k1+1)) / (tf(qi,D) + k1 × (1-b + b × |D|/avgdl))
```

## Design Patterns

### Tokenization Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `.simple` | Whitespace + punctuation split | General text |
| `.stem` | Porter stemmer (English) | Morphological matching |
| `.ngram` | Character n-grams | Partial matching, typos |
| `.keyword` | Entire value as single token | Exact match (categories) |

**Configuration**:
```swift
// Simple tokenization (default)
FullTextIndexKind(fields: [\.content], tokenizer: .simple)

// Stemming
FullTextIndexKind(fields: [\.content], tokenizer: .stem)

// N-gram (trigrams)
FullTextIndexKind(fields: [\.content], tokenizer: .ngram, ngramSize: 3)

// Keyword (no tokenization)
FullTextIndexKind(fields: [\.category], tokenizer: .keyword)
```

### BM25 Parameter Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `k1` | 1.2 | Term frequency saturation |
| `b` | 0.75 | Document length normalization |

**Tuning Guide**:
```
k1 = 1.2: Good for most use cases
k1 = 0.5-1.0: Reduce repetition bias
k1 = 1.5-2.0: Emphasize term frequency

b = 0.75: Standard length normalization
b = 0.0: No length normalization
b = 1.0: Full length normalization
```

### Position Storage (Phrase Search)

Enable position storage for phrase queries:

```swift
// With positions (required for phrase search)
#Index<Article>(
    type: FullTextIndexKind(
        fields: [\.content],
        storePositions: true  // Store term positions
    )
)

// Phrase search requires positions
let results = try await context.search(Article.self)
    .text(\.content)
    .query("machine learning")
    .matchMode(.phrase)
    .execute()
```

**Trade-off**: Position storage increases index size but enables phrase queries.

### Sparse Index (Optional Text Fields)

FullTextIndex supports sparse index behavior for optional fields:

```swift
@Persistable
struct Document {
    var id: String = ULID().ulidString
    var content: String? = nil  // Optional - not all docs have content

    #Index<Document>(
        type: FullTextIndexKind(
            fields: [\.content]
        )
    )
}

// Documents with nil content are NOT indexed
// Only documents with content appear in search results
```

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| Simple tokenization | ✅ Complete | Whitespace + punctuation |
| Stemming (Porter) | ✅ Complete | English stemmer |
| N-gram tokenization | ✅ Complete | Configurable size |
| Term search | ✅ Complete | O(1) lookup |
| Boolean AND/OR | ✅ Complete | Multi-term queries |
| Phrase search | ✅ Complete | Requires storePositions |
| BM25 scoring | ✅ Complete | Full implementation |
| Highlighting | ✅ Complete | Configurable fragments |
| Fuzzy matching | ✅ Complete | Levenshtein distance |
| Sparse index (nil) | ✅ Complete | nil values not indexed |
| Multi-field index | ✅ Complete | Combined text fields |
| Faceted search | ❌ Not implemented | Term aggregation |
| Autocomplete | ⚠️ Partial | Via n-gram |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Term search | O(1 + d) | d = matching documents |
| AND query (t terms) | O(t + d × t) | Incremental intersection |
| OR query (t terms) | O(t × d) | Union of all matches |
| Phrase search | O(t + d × p) | p = position verification |
| Insert/Update | O(w) | w = words in document |
| Delete | O(w) | w = words in document |
| BM25 scoring | O(d × t) | Score each match |

### FDB Considerations

- **Term key limit**: 10KB max (terms truncated at 8KB)
- **Transaction limit**: 10MB writes, batch large documents
- **Atomic counters**: BM25 stats use atomic add operations

## Benchmark Results

Run with: `swift test --filter FullTextIndexPerformanceTests`

### Indexing

| Documents | Avg Words | Insert Time | Throughput |
|-----------|-----------|-------------|------------|
| 100 | 50 | ~200ms | ~500/s |
| 1,000 | 50 | ~2s | ~500/s |
| 10,000 | 50 | ~20s | ~500/s |

### Search

| Documents | Terms | Match Mode | Latency (p50) |
|-----------|-------|------------|---------------|
| 1,000 | 1 | - | ~5ms |
| 1,000 | 3 | AND | ~15ms |
| 1,000 | 3 | OR | ~20ms |
| 10,000 | 1 | - | ~10ms |
| 10,000 | 3 | AND | ~30ms |

### BM25 Scoring

| Documents | Query Terms | Scoring Overhead |
|-----------|-------------|-----------------|
| 1,000 | 3 | ~50ms |
| 10,000 | 3 | ~200ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [BM25 Paper](https://www.staff.city.ac.uk/~sbrp622/papers/foundations_bm25_review.pdf) - Robertson & Zaragoza
- [Inverted Index](https://en.wikipedia.org/wiki/Inverted_index) - Wikipedia
- [Porter Stemmer](https://tartarus.org/martin/PorterStemmer/) - Original Algorithm
- [Lucene Scoring](https://lucene.apache.org/core/9_0_0/core/org/apache/lucene/search/similarities/BM25Similarity.html) - Reference Implementation

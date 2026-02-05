# Benchmark Baseline

**Date**: 2026-02-05
**Optimizations Applied**: Parallel FDB reads, FieldMap caching, Varint optimization, Zero-copy Skip List

## Results

### RankIndex Performance Tests (11 tests)

**Latest Run (2026-02-05 15:45)**:

| Test | Duration | Throughput |
|------|----------|------------|
| Bulk insert 100 items | 13.25 ms | 7,549 ops/s |
| Bulk insert 1,000 items | 76.35 ms | 13,098 ops/s |
| Bulk insert 2,000 items | 155.84 ms | 12,834 ops/s |
| Update 100 items | 7.66 ms | 13,058 ops/s |
| Delete 100 items | 6.91 ms | 14,475 ops/s |
| Top-10 query (500 items) | 8.94 ms | - |
| Top-50 query (500 items) | 6.98 ms | - |
| Top-100 query (500 items) | 7.50 ms | - |
| Top-250 query (500 items) | 7.87 ms | - |
| Top-100 query (2,000 items) | 27.02 ms | - |
| Count query (O(1)) | 0.52 ms avg | - |
| Rank lookup (rank 0) | 0.62 ms | - |
| Rank lookup (rank 250) | 1.71 ms | - |
| Rank lookup (rank 499) | 2.75 ms | - |
| Percentile 50% | 7.36 ms | - |
| Percentile 95% | 6.25 ms | - |
| Ties handling Top-20 | 1.84 ms | - |

### AggregationIndex Performance Tests (10 tests)

| Test | Duration | Throughput |
|------|----------|------------|
| COUNT bulk insert 1000 items | 17.58 ms | 56,879 items/s |
| SUM bulk insert 1000 items | 9.51 ms | 105,105 items/s |
| AVG bulk insert 1000 items | 14.56 ms | 68,686 items/s |
| MIN bulk insert 1000 items | 75.39 ms | 13,264 items/s |
| MAX bulk insert 1000 items | 70.35 ms | 14,214 items/s |
| Large scale COUNT (5000 items) | 82.81 ms | 60,382 items/s |
| COUNT delete 300 items | 6.71 ms | 44,705 items/s |
| MIN query (50 items) | 26.68 ms | 1,874 items/s |
| MAX query (50 items) | 26.87 ms | 1,861 items/s |
| Composite grouping insert | 10.57 ms | 94,611 items/s |

### FullTextIndex Performance Tests (11 tests)

| Test | Duration | Throughput |
|------|----------|------------|
| Bulk insert 100 docs | 42.87 ms | 2,332 docs/s |
| Insert (10 words/doc) | 0.25 ms/doc | - |
| Insert (50 words/doc) | 0.44 ms/doc | - |
| Insert (100 words/doc) | 0.54 ms/doc | - |
| Insert (200 words/doc) | 0.69 ms/doc | - |
| Single term search | 2.95 ms avg | 339/s |
| Boolean AND query | 7.63 ms avg | - |
| Boolean OR query | 7.24 ms avg | - |
| Phrase search | 8.78 ms avg | - |
| BM25 scoring | 24.20 ms avg | - |
| Update | 7.20 ms avg | - |
| Delete | 6.89 ms avg | - |
| Search (50 docs) | 1.63 ms avg | - |
| Search (100 docs) | 1.95 ms avg | - |
| Search (200 docs) | 2.86 ms avg | - |

### VectorIndex Performance Tests (9 tests)

| Test | Duration | Throughput |
|------|----------|------------|
| Flat scan 100 vectors (128d) | 9.85 ms avg | 102/s |
| Flat scan 500 vectors (128d) | 45.81 ms avg | - |
| Flat scan 100 vectors (64d) | 5.58 ms avg | - |
| Flat scan 100 vectors (256d) | 18.08 ms avg | - |
| Flat scan 100 vectors (384d) | 26.36 ms avg | - |
| Insert 100 vectors | 17.19 ms | 5,818 ops/s |
| Update | 5.97 ms avg | - |
| Delete | 7.93 ms avg | - |
| Cosine metric | 9.59 ms avg | - |
| Euclidean metric | 7.76 ms avg | - |
| Dot product metric | 7.67 ms avg | - |

### PermutedIndex Performance Tests (11 tests)

| Test | Duration | Throughput |
|------|----------|------------|
| Bulk insert 100 items | 12.09 ms | 8,272 ops/s |
| Bulk insert 1000 items | 85.27 ms | 11,728 ops/s |
| Prefix query (city) | 1.33-1.84 ms | - |
| Prefix query (city+country) | 1.40-1.58 ms | - |
| Exact match | 0.69-1.19 ms | - |
| Scan all (100 items) | 2.13 ms | - |
| Scan all (500 items) | 9.33 ms | - |
| Update 100 items | 9.72 ms | 10,283 ops/s |
| Delete 100 items | 9.17 ms | 10,902 ops/s |

### BitmapIndex Performance Tests (18 tests)

| Test | Duration | Throughput |
|------|----------|------------|
| Insert 100 records | 47.84 ms | - |
| Insert 1000 records | 418.13 ms | - |
| Add 10,000 values | 851.95 ms | 11,738 ops/s |
| Contains 10,000 lookups | 1.78 ms | 5,609,050 ops/s |
| AND sparse bitmaps (100 iter) | 153.12 ms | 1.53 ms/op |
| Serialize (100 iter) | 18.18 ms | 0.18 ms/op |
| Deserialize (100 iter) | 38.16 ms | 0.38 ms/op |
| Get bitmap (100 iter) | 63.35 ms | 0.63 ms/op |
| Get count (100 iter) | 71.51 ms | 0.72 ms/op |

## Applied Optimizations

### Phase 2 (2026-02-05)
1. **HNSW Vector Search parallelization**: TaskGroup for label→PK conversion (10-30× expected)
2. **fetchByIds parallelization**: TaskGroup for batch ID fetching (10-30× expected)
3. **FieldMap caching**: Pre-computed fieldMapByName/fieldMapByNumber in TypeCatalog (30-50% expected)
4. **Varint encoding optimization**: reserveCapacity(10) for varint arrays (20-30% expected)

### Phase 1 (2026-02-05)
1. **Zero-copy Skip List operations**: Direct byte comparison using `lexicographicallyPrecedes()`
2. **Tuple pack/unpack cycle elimination**: Convert Tuple to array directly
3. **extractPrimaryKey() consolidation**: Shared utility method in SkipListSubspaces
4. **Direct ByteConversion calls**: Removed wrapper methods

## Environment

- Platform: macOS (Darwin 25.2.0)
- Swift: 6.x
- FoundationDB: Local instance

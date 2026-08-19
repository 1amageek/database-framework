# VectorIndex

`VectorIndex` executes vector similarity declarations.

```swift
#Index(.vector(
    name: "documents_by_embedding",
    embedding: \Document.embedding,
    dimensions: 384,
    metric: .cosine
))
```

Dimensions and metric are logical schema semantics. `VectorIndexConfiguration`
selects an execution algorithm such as Flat, HNSW, IVF, or PQ for the declared
index without changing that schema meaning.

```swift
let configuration = VectorIndexConfiguration(
    indexName: "documents_by_embedding",
    algorithm: .hnsw(.default)
)
```

Polymorphic declarations use the same algebra with a protocol property name:

```swift
@PolymorphicIndex(.vector(
    name: "Entity_embedding",
    embedding: "embedding",
    dimensions: 384,
    metric: .cosine
))
```

Algorithm-specific finalization completes before the physical generation may
transition to `readable`.

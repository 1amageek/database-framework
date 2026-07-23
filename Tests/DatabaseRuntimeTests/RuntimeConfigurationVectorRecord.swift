import Core
import DatabaseValue
import VectorIndex

@Persistable
struct RuntimeConfigurationVectorRecord {
    #Index(
        VectorIndexKind<RuntimeConfigurationVectorRecord>(
            embedding: \.embedding,
            dimensions: 3
        ),
        name: "RuntimeConfigurationVectorRecord_embedding"
    )

    var embedding: [Float]
}

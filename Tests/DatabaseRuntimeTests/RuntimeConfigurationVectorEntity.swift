import Core
import DatabaseValue
import VectorIndex

@Persistable
struct RuntimeConfigurationVectorEntity {
    #Index(
        VectorIndexKind<RuntimeConfigurationVectorEntity>(
            embedding: \.embedding,
            dimensions: 3
        ),
        name: "RuntimeConfigurationVectorEntity_embedding"
    )

    var embedding: [Float]
}

import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationVectorEntity {
    #Index(
        .vector(
            name: "RuntimeConfigurationVectorEntity_embedding",
            embedding: \RuntimeConfigurationVectorEntity.embedding,
            dimensions: 3
        ))

    var id: String = ""
    var embedding: Vector
}

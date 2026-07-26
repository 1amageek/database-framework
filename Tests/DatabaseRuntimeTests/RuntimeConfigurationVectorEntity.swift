import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationVectorEntity {
    #Index(
        .vector(dimensions: 3),
        embedding: \RuntimeConfigurationVectorEntity.embedding,
        name: "RuntimeConfigurationVectorEntity_embedding"
    )

    var id: String = ""
    var embedding: Vector
}

import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationUniqueVectorEntity {
    #Index(
        .vector(dimensions: 3),
        embedding: \RuntimeConfigurationUniqueVectorEntity.embedding,
        unique: true,
        name: "RuntimeConfigurationUniqueVectorEntity_embedding"
    )

    var id: String = ""
    var embedding: Vector
}

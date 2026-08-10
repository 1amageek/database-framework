#if DATABASE_SERVER_TEST_VECTOR_INDEXES
import DatabaseKit
import DatabaseTypes

@Persistable
struct DatabaseCompositionVectorEntity {
    #Directory<DatabaseCompositionVectorEntity>(
        "test",
        "database-composition-vector"
    )

    var id: String
    var title: String
    var embedding: Vector

    #Index(
        .vector(dimensions: 2, metric: .euclidean),
        embedding: \DatabaseCompositionVectorEntity.embedding,
        name: "DatabaseCompositionVectorEntity_embedding"
    )
}
#endif

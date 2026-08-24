#if POSTGRESQL
import DatabaseKit
import DatabaseRuntime
import FullTextIndex
import TestSupport
import Testing

@Persistable
private struct PostgreSQLFusionArticle {
    #Directory<PostgreSQLFusionArticle>("test", "postgresql", "fusion")
    #Index(
        .text(
            name: "postgresql_fusion_body",
            fields: [\PostgreSQLFusionArticle.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String
}

@Suite(
    "PostgreSQL Fusion Tests",
    .serialized,
    .heartbeat,
    .enabled(if: PostgreSQLScenarioCoordinator.isConfigured)
)
struct PostgreSQLFusionTests {
    @Test("Full-text Fusion uses the PostgreSQL transaction path")
    func fullTextFusionUsesPostgreSQL() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let schema = try Schema(
                entities: [try PostgreSQLFusionArticle.schemaEntity],
                version: Schema.Version(1, 0, 0)
            )
            let container = try await PostgreSQLScenarioCoordinator.shared
                .makeContainer(
                    schema: schema,
                    entityRuntimes: [
                        try DatabaseFrameworkRuntime.entity(
                            PostgreSQLFusionArticle.self
                        ),
                    ]
                )
            defer { await container.shutdown() }
            let context = container.testBaseContext()
            try context.insert(
                PostgreSQLFusionArticle(
                    id: "matching",
                    body: "swift database fusion"
                )
            )
            try context.insert(
                PostgreSQLFusionArticle(
                    id: "unrelated",
                    body: "unrelated document"
                )
            )
            try await context.save()

            let query = FusionQuery<PostgreSQLFusionArticle> {
                Search(PostgreSQLFusionArticle.fields.body)
                    .terms(["fusion"])
            }
            let response = try await context.execute(query)

            #expect(response.results.map(\.item.id) == ["matching"])
            #expect(response.continuation == nil)
        }
    }
}
#endif

#if !os(WASI)
#if FOUNDATION_DB
// IndexRebuildConsistencyTests.swift
// Verifies that all IndexKind implementations produce identical subspace layouts
// between save-time and rebuild-time, and that rebuilt indexes remain queryable.
//
// These tests detect the EntityIndexBuilder.buildEntityIndex() subspace bug where
// rebuild path uses [I] instead of [I]/[indexName], as well as AdminContext
// state visibility issues.

import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import FullTextIndex
@testable import GraphIndex
@testable import ScalarIndex
@testable import AggregationIndex

private enum IndexRebuildConsistencyError: Error {
    case missingIndex(entity: String)
}

// MARK: - Test Models

@Persistable
struct RebuildScalarUser {
    #Directory<RebuildScalarUser>("test", "rebuild", "scalar")
    var id: String = UUID().uuidString
    var email: String = ""
    var city: String = ""
    #Index(.scalar, fields: [\RebuildScalarUser.email])
}

@Persistable
struct RebuildTripleStatement {
    #Directory<RebuildTripleStatement>("test", "rebuild", "triple")
    var id: String = UUID().uuidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""
    #Index(
        .propertyGraph(strategy: .tripleStore),
        from: \RebuildTripleStatement.subject,
        edge: \RebuildTripleStatement.predicate,
        to: \RebuildTripleStatement.object
    )
}

@Persistable
struct RebuildEdge {
    #Directory<RebuildEdge>("test", "rebuild", "edge")
    var id: String = UUID().uuidString
    var source: String = ""
    var relation: String = ""
    var target: String = ""
    #Index(
        .propertyGraph(strategy: .adjacency),
        from: \RebuildEdge.source,
        edge: \RebuildEdge.relation,
        to: \RebuildEdge.target
    )
}

@Persistable
struct RebuildArticle {
    #Directory<RebuildArticle>("test", "rebuild", "fulltext")
    var id: String = UUID().uuidString
    var title: String = ""
    var content: String = ""
    #Index(.fullText(tokenizer: .simple), fields: [\RebuildArticle.content])
}

@Persistable
struct RebuildCountItem {
    #Directory<RebuildCountItem>("test", "rebuild", "count")
    var id: String = UUID().uuidString
    var category: String = ""
    var value: Int64 = 0
    #Index(.count, groupBy: [\RebuildCountItem.category])
}

// MARK: - Test Suite

@Suite("Index Rebuild Consistency Tests", .foundationDBScenario, .serialized, .heartbeat)
struct IndexRebuildConsistencyTests {

    // MARK: - Setup

    private func setupContainer(_ types: [any Persistable.Type]) async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        var entities: [Schema.Entity] = []
        entities.reserveCapacity(types.count)
        for type in types {
            entities.append(try type.schemaEntity)
        }
        let schema = try Schema(
            entities: entities,
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            for: schema,
            configuration: .init(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(persistableTypes: [RebuildScalarUser.self, RebuildTripleStatement.self, RebuildEdge.self, RebuildArticle.self, RebuildCountItem.self]),
            security: .disabled
            )
    }

    private func cleanup(container: DBContainer, path: [String]) async throws {
        try await container.engine.removeNamespace(path: path)
        // Reinitialize indexes after directory removal.
        try await container.ensureIndexesReady()
    }

    /// Returns every key below the `[I]/[indexName]` FoundationDB range.
    private func getIndexKeys<T: Persistable>(
        for type: T.Type,
        container: DBContainer
    ) async throws -> Set<[UInt8]> {
        let typeSubspace = try await container.resolveDirectory(for: type)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)
        guard let indexName = try type.indexDescriptors.first?.name else {
            return []
        }
        let namedSubspace = indexSubspace.subspace(indexName)
        return try await container.engine.withTransaction { tx -> Set<[UInt8]> in
            let (begin, end) = namedSubspace.range()
            var keys = Set<[UInt8]>()
            for (key, _) in try await tx.collectRange(from: .firstGreaterOrEqual(begin), to: .firstGreaterOrEqual(end), snapshot: true) {
                keys.insert(Array(key))
            }
            return keys
        }
    }

    private func getIndexName<T: Persistable>(
        for type: T.Type
    ) throws -> String {
        guard let name = try type.indexDescriptors.first?.name else {
            throw IndexRebuildConsistencyError.missingIndex(
                entity: type.persistableType
            )
        }
        return name
    }

    // MARK: - A. Subspace Layout Tests

    @Test("ScalarIndex: rebuild produces same subspace layout as save-time")
    func scalarSubspaceLayout() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildScalarUser.self])
            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])


            let context = container.newContext()
            var u1 = RebuildScalarUser(); u1.email = "a@test.com"
            var u2 = RebuildScalarUser(); u2.email = "b@test.com"
            var u3 = RebuildScalarUser(); u3.email = "c@test.com"
            try context.insert(u1); try context.insert(u2); try context.insert(u3)
            try await context.save()

            let saveKeys = try await getIndexKeys(for: RebuildScalarUser.self, container: container)
            #expect(saveKeys.count == 3, "ScalarIndex: 3 users → 3 keys, got \(saveKeys.count)")

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildScalarUser.self),
                progress: nil
            )

            let rebuildKeys = try await getIndexKeys(for: RebuildScalarUser.self, container: container)
            #expect(saveKeys.count == rebuildKeys.count,
                "ScalarIndex: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
            #expect(saveKeys == rebuildKeys, "ScalarIndex: rebuild keys must match save-time keys")

            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        }
    }

    @Test("GraphIndex tripleStore: rebuild produces same subspace layout as save-time")
    func graphTripleStoreSubspaceLayout() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildTripleStatement.self])
            try await cleanup(container: container, path: ["test", "rebuild", "triple"])


            let context = container.newContext()
            var s1 = RebuildTripleStatement(); s1.subject = "A"; s1.predicate = "p"; s1.object = "B"
            var s2 = RebuildTripleStatement(); s2.subject = "C"; s2.predicate = "q"; s2.object = "D"
            try context.insert(s1); try context.insert(s2)
            try await context.save()

            let saveKeys = try await getIndexKeys(for: RebuildTripleStatement.self, container: container)
            #expect(saveKeys.count == 6, "tripleStore: 2 entities × 3 = 6 keys, got \(saveKeys.count)")

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildTripleStatement.self),
                progress: nil
            )

            let rebuildKeys = try await getIndexKeys(for: RebuildTripleStatement.self, container: container)
            #expect(saveKeys.count == rebuildKeys.count,
                "tripleStore: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
            #expect(saveKeys == rebuildKeys, "tripleStore: rebuild keys must match save-time keys")

            try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        }
    }

    @Test("GraphIndex adjacency: rebuild produces same subspace layout as save-time")
    func graphAdjacencySubspaceLayout() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildEdge.self])
            try await cleanup(container: container, path: ["test", "rebuild", "edge"])


            let context = container.newContext()
            var e1 = RebuildEdge(); e1.source = "Alice"; e1.relation = "knows"; e1.target = "Bob"
            var e2 = RebuildEdge(); e2.source = "Bob"; e2.relation = "knows"; e2.target = "Carol"
            try context.insert(e1); try context.insert(e2)
            try await context.save()

            let saveKeys = try await getIndexKeys(for: RebuildEdge.self, container: container)
            #expect(saveKeys.count == 4, "adjacency: 2 entities × 2 = 4 keys, got \(saveKeys.count)")

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildEdge.self),
                progress: nil
            )

            let rebuildKeys = try await getIndexKeys(for: RebuildEdge.self, container: container)
            #expect(saveKeys.count == rebuildKeys.count,
                "adjacency: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
            #expect(saveKeys == rebuildKeys, "adjacency: rebuild keys must match save-time keys")

            try await cleanup(container: container, path: ["test", "rebuild", "edge"])
        }
    }

    @Test("FullTextIndex: rebuild produces same subspace layout as save-time")
    func fullTextSubspaceLayout() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildArticle.self])
            try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])


            let context = container.newContext()
            var a1 = RebuildArticle(); a1.title = "T1"; a1.content = "hello world"
            var a2 = RebuildArticle(); a2.title = "T2"; a2.content = "hello swift"
            try context.insert(a1); try context.insert(a2)
            try await context.save()

            let saveKeys = try await getIndexKeys(for: RebuildArticle.self, container: container)
            #expect(saveKeys.count > 0, "FullText: should have index entries, got \(saveKeys.count)")

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildArticle.self),
                progress: nil
            )

            let rebuildKeys = try await getIndexKeys(for: RebuildArticle.self, container: container)
            #expect(saveKeys.count == rebuildKeys.count,
                "FullText: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
            #expect(saveKeys == rebuildKeys, "FullText: rebuild keys must match save-time keys")

            try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
        }
    }

    @Test("CountIndex: rebuild produces same subspace layout as save-time")
    func countSubspaceLayout() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildCountItem.self])
            try await cleanup(container: container, path: ["test", "rebuild", "count"])


            let context = container.newContext()
            var c1 = RebuildCountItem(); c1.category = "electronics"; c1.value = 100
            var c2 = RebuildCountItem(); c2.category = "electronics"; c2.value = 200
            var c3 = RebuildCountItem(); c3.category = "books"; c3.value = 50
            try context.insert(c1); try context.insert(c2); try context.insert(c3)
            try await context.save()

            let saveKeys = try await getIndexKeys(for: RebuildCountItem.self, container: container)
            #expect(saveKeys.count == 2, "Count: 2 groups → 2 keys, got \(saveKeys.count)")

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildCountItem.self),
                progress: nil
            )

            let rebuildKeys = try await getIndexKeys(for: RebuildCountItem.self, container: container)
            #expect(saveKeys.count == rebuildKeys.count,
                "Count: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
            #expect(saveKeys == rebuildKeys, "Count: rebuild keys must match save-time keys")

            try await cleanup(container: container, path: ["test", "rebuild", "count"])
        }
    }

    // MARK: - B. Round-trip Query Tests

    @Test("ScalarIndex: rebuild entries are queryable")
    func scalarRoundTrip() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildScalarUser.self])
            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])


            let context = container.newContext()
            var u1 = RebuildScalarUser(); u1.email = "alice@test.com"; u1.city = "Tokyo"
            try context.insert(u1)
            try await context.save()

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildScalarUser.self),
                progress: nil
            )

            let all = try await context.fetch(RebuildScalarUser.self).execute()
            #expect(all.count == 1, "ScalarIndex round-trip: expected 1 user, got \(all.count)")
            #expect(all.first?.email == "alice@test.com")

            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        }
    }

    @Test("GraphIndex tripleStore: rebuild entries are queryable")
    func graphTripleStoreRoundTrip() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildTripleStatement.self])
            try await cleanup(container: container, path: ["test", "rebuild", "triple"])


            let context = container.newContext()
            var s1 = RebuildTripleStatement()
            s1.subject = "Alice"; s1.predicate = "knows"; s1.object = "Bob"
            var s2 = RebuildTripleStatement()
            s2.subject = "Alice"; s2.predicate = "knows"; s2.object = "Carol"
            var s3 = RebuildTripleStatement()
            s3.subject = "Bob"; s3.predicate = "knows"; s3.object = "Dave"
            try context.insert(s1); try context.insert(s2); try context.insert(s3)
            try await context.save()

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildTripleStatement.self),
                progress: nil
            )

            let results = try await context.graph(RebuildTripleStatement.self)
                .defaultIndex()
                .from("Alice")
                .edge("knows")
                .execute()

            #expect(results.count == 2,
                "After rebuild: Alice should know 2 people, got \(results.count)")
            let friends = Set(results.map(\.to))
            #expect(friends.contains("Bob"))
            #expect(friends.contains("Carol"))

            let reverseResults = try await context.graph(RebuildTripleStatement.self)
                .defaultIndex()
                .edge("knows")
                .to("Bob")
                .execute()

            #expect(reverseResults.count == 1,
                "After rebuild: 1 person should know Bob, got \(reverseResults.count)")
            #expect(reverseResults.first?.from == "Alice")

            try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        }
    }

    @Test("GraphIndex adjacency: rebuild entries are queryable via graph builder")
    func graphAdjacencyRoundTrip() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildEdge.self])
            try await cleanup(container: container, path: ["test", "rebuild", "edge"])


            let context = container.newContext()
            var e1 = RebuildEdge(); e1.source = "Alice"; e1.relation = "follows"; e1.target = "Bob"
            var e2 = RebuildEdge(); e2.source = "Alice"; e2.relation = "follows"; e2.target = "Carol"
            try context.insert(e1); try context.insert(e2)
            try await context.save()

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildEdge.self),
                progress: nil
            )

            let outgoing = try await context.graph(RebuildEdge.self)
                .defaultIndex()
                .from("Alice")
                .execute()

            #expect(outgoing.count == 2,
                "After rebuild: Alice should have 2 outgoing edges, got \(outgoing.count)")
            let targets = Set(outgoing.map(\.to))
            #expect(targets.contains("Bob"))
            #expect(targets.contains("Carol"))

            try await cleanup(container: container, path: ["test", "rebuild", "edge"])
        }
    }

    @Test("FullTextIndex: rebuild entries are searchable")
    func fullTextRoundTrip() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildArticle.self])
            try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])


            let context = container.newContext()
            var a1 = RebuildArticle(); a1.title = "Swift Guide"; a1.content = "swift programming language"
            var a2 = RebuildArticle(); a2.title = "Rust Guide"; a2.content = "rust programming language"
            try context.insert(a1); try context.insert(a2)
            try await context.save()

            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildArticle.self),
                progress: nil
            )

            let swiftResults = try await context.search(RebuildArticle.self)
                .fullText(RebuildArticle.fields.content)
                .terms(["swift"])
                .execute()

            #expect(swiftResults.count == 1,
                "After rebuild: 'swift' should match 1 article, got \(swiftResults.count)")

            let commonResults = try await context.search(RebuildArticle.self)
                .fullText(RebuildArticle.fields.content)
                .terms(["programming"])
                .execute()

            #expect(commonResults.count == 2,
                "After rebuild: 'programming' should match 2 articles, got \(commonResults.count)")

            try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
        }
    }

    @Test("CountIndex: rebuild entries are queryable")
    func countRoundTrip() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildCountItem.self])
            try await cleanup(container: container, path: ["test", "rebuild", "count"])


            let context = container.newContext()
            var c1 = RebuildCountItem(); c1.category = "electronics"; c1.value = 100
            var c2 = RebuildCountItem(); c2.category = "electronics"; c2.value = 200
            var c3 = RebuildCountItem(); c3.category = "books"; c3.value = 50
            try context.insert(c1); try context.insert(c2); try context.insert(c3)
            try await context.save()

            let admin = container.newAdminContext()
            let indexName = try getIndexName(for: RebuildCountItem.self)
            try await admin.rebuildIndex(indexName, progress: nil)

            let stats = try await admin.indexStatistics(indexName)
            #expect(stats.entryCount == 2,
                "After rebuild: 2 groups should yield 2 entries, got \(stats.entryCount)")

            try await cleanup(container: container, path: ["test", "rebuild", "count"])
        }
    }

    // MARK: - C. AdminContext State Visibility

    @Test("AdminContext state changes visible to IndexLifecycleStore")
    func adminContextStateVisibleToFDBDataStore() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildScalarUser.self])
            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])


            let context = container.newContext()
            var u1 = RebuildScalarUser(); u1.email = "test@test.com"
            try context.insert(u1)
            try await context.save()

            let admin = container.newAdminContext()
            let indexName = try getIndexName(for: RebuildScalarUser.self)
            try await admin.rebuildIndex(indexName, progress: nil)

            // Read state via IndexLifecycleStore (entity root subspace)
            let entitySubspace = try await container.resolveDirectory(for: RebuildScalarUser.self)
            let lifecycleStore = IndexLifecycleStore(container: container, subspace: entitySubspace)
            let state = try await lifecycleStore.state(of: indexName)

            #expect(state == .readable,
                "IndexLifecycleStore should see index as readable after rebuild, got \(state)")

            try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        }
    }

    // MARK: - D. Rebuild + CRUD Consistency

    @Test("Insert after rebuild correctly maintains index")
    func insertAfterRebuildMaintainsIndex() async throws {
        try await FoundationDBScenarioEnvironment.shared.withSerializedAccess {
            let container = try await setupContainer([RebuildTripleStatement.self])
            try await cleanup(container: container, path: ["test", "rebuild", "triple"])


            let context = container.newContext()
            var s1 = RebuildTripleStatement()
            s1.subject = "Alice"; s1.predicate = "knows"; s1.object = "Bob"
            try context.insert(s1)
            try await context.save()

            // Rebuild
            let admin = container.newAdminContext()
            try await admin.rebuildIndex(
                try getIndexName(for: RebuildTripleStatement.self),
                progress: nil
            )

            // Insert AFTER rebuild
            var s2 = RebuildTripleStatement()
            s2.subject = "Alice"; s2.predicate = "knows"; s2.object = "Carol"
            try context.insert(s2)
            try await context.save()

            // Query should find BOTH pre- and post-rebuild data
            let results = try await context.graph(RebuildTripleStatement.self)
                .defaultIndex()
                .from("Alice")
                .edge("knows")
                .execute()

            #expect(results.count == 2,
                "After rebuild + insert: expected 2 friends, got \(results.count)")
            let friends = Set(results.map(\.to))
            #expect(friends.contains("Bob"), "Pre-rebuild data must survive")
            #expect(friends.contains("Carol"), "Post-rebuild data must be indexed")

            try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        }
    }
}
#endif

#endif

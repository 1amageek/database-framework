// IndexRebuildConsistencyTests.swift
// Verifies that all IndexKind implementations produce identical subspace layouts
// between save-time and rebuild-time, and that rebuilt indexes remain queryable.
//
// These tests detect the EntityIndexBuilder.buildEntityIndex() subspace bug where
// rebuild path uses [I] instead of [I]/[indexName], as well as AdminContext
// state visibility issues.

import Testing
import Foundation
import FoundationDB
import Core
import FullText
import Graph
@testable import DatabaseEngine
@testable import FullTextIndex
@testable import GraphIndex
@testable import ScalarIndex
@testable import AggregationIndex

// MARK: - Test Models

@Persistable
struct RebuildScalarUser {
    #Directory<RebuildScalarUser>("test", "rebuild", "scalar")
    var id: String = ULID().ulidString
    var email: String = ""
    var city: String = ""
    #Index(ScalarIndexKind<RebuildScalarUser>(fields: [\.email]))
}

@Persistable
struct RebuildTripleStatement {
    #Directory<RebuildTripleStatement>("test", "rebuild", "triple")
    var id: String = ULID().ulidString
    var subject: String = ""
    var predicate: String = ""
    var object: String = ""
    #Index(GraphIndexKind<RebuildTripleStatement>(
        from: \.subject, edge: \.predicate, to: \.object,
        strategy: .tripleStore
    ))
}

@Persistable
struct RebuildEdge {
    #Directory<RebuildEdge>("test", "rebuild", "edge")
    var id: String = ULID().ulidString
    var source: String = ""
    var relation: String = ""
    var target: String = ""
    #Index(GraphIndexKind<RebuildEdge>(
        from: \.source, edge: \.relation, to: \.target,
        strategy: .adjacency
    ))
}

@Persistable
struct RebuildArticle {
    #Directory<RebuildArticle>("test", "rebuild", "fulltext")
    var id: String = ULID().ulidString
    var title: String = ""
    var content: String = ""
    #Index(FullTextIndexKind<RebuildArticle>(
        fields: [\.content],
        tokenizer: .simple
    ))
}

@Persistable
struct RebuildCountItem {
    #Directory<RebuildCountItem>("test", "rebuild", "count")
    var id: String = ULID().ulidString
    var category: String = ""
    var value: Int = 0
    #Index(CountIndexKind<RebuildCountItem>(groupBy: [\.category]))
}

// MARK: - Test Suite

@Suite("Index Rebuild Consistency Tests", .serialized)
struct IndexRebuildConsistencyTests {

    // MARK: - Setup

    private func setupContainer(_ types: [any Persistable.Type]) async throws -> FDBContainer {
        try await FDBTestEnvironment.shared.ensureInitialized()
        let database = try FDBClient.openDatabase()
        let schema = Schema(types, version: Schema.Version(1, 0, 0))
        for type in types {
            IndexBuilderRegistry.shared.register(type)
        }
        return FDBContainer(
            database: database,
            schema: schema,
            security: .disabled
        )
    }

    private func cleanup(container: FDBContainer, path: [String]) async throws {
        let directoryLayer = DirectoryLayer(database: container.database)
        try? await directoryLayer.remove(path: path)
    }

    private func setIndexStatesToReadable<T: Persistable>(
        for type: T.Type,
        container: FDBContainer
    ) async throws {
        let subspace = try await container.resolveDirectory(for: type)
        let stateManager = IndexStateManager(container: container, subspace: subspace)

        for descriptor in type.indexDescriptors {
            let maxAttempts = 3
            for attempt in 1...maxAttempts {
                let currentState = try await stateManager.state(of: descriptor.name)
                switch currentState {
                case .disabled:
                    do {
                        try await stateManager.enable(descriptor.name)
                        try await stateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .writeOnly:
                    do {
                        try await stateManager.makeReadable(descriptor.name)
                        break
                    } catch let error as IndexStateError {
                        if case .invalidTransition = error, attempt < maxAttempts {
                            continue
                        }
                        throw error
                    }
                case .readable:
                    break
                }
            }
        }
    }

    /// FDB range scan: [I]/[indexName] 以下の全キーを取得
    private func getIndexKeys<T: Persistable>(
        for type: T.Type,
        container: FDBContainer
    ) async throws -> Set<[UInt8]> {
        let typeSubspace = try await container.resolveDirectory(for: type)
        let indexSubspace = typeSubspace.subspace(SubspaceKey.indexes)
        guard let indexName = type.indexDescriptors.first?.name else {
            return []
        }
        let namedSubspace = indexSubspace.subspace(indexName)
        return try await container.database.withTransaction { tx -> Set<[UInt8]> in
            let (begin, end) = namedSubspace.range()
            var keys = Set<[UInt8]>()
            for try await (key, _) in tx.getRange(begin: begin, end: end, snapshot: true) {
                keys.insert(Array(key))
            }
            return keys
        }
    }

    private func getIndexName<T: Persistable>(for type: T.Type) -> String? {
        type.indexDescriptors.first?.name
    }

    // MARK: - A. Subspace Layout Tests

    @Test("ScalarIndex: rebuild produces same subspace layout as save-time")
    func scalarSubspaceLayout() async throws {
        let container = try await setupContainer([RebuildScalarUser.self])
        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        try await setIndexStatesToReadable(for: RebuildScalarUser.self, container: container)

        let context = container.newContext()
        var u1 = RebuildScalarUser(); u1.email = "a@test.com"
        var u2 = RebuildScalarUser(); u2.email = "b@test.com"
        var u3 = RebuildScalarUser(); u3.email = "c@test.com"
        context.insert(u1); context.insert(u2); context.insert(u3)
        try await context.save()

        let saveKeys = try await getIndexKeys(for: RebuildScalarUser.self, container: container)
        #expect(saveKeys.count == 3, "ScalarIndex: 3 users → 3 keys, got \(saveKeys.count)")

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildScalarUser.self)!, progress: nil)

        let rebuildKeys = try await getIndexKeys(for: RebuildScalarUser.self, container: container)
        #expect(saveKeys.count == rebuildKeys.count,
            "ScalarIndex: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
        #expect(saveKeys == rebuildKeys, "ScalarIndex: rebuild keys must match save-time keys")

        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
    }

    @Test("GraphIndex tripleStore: rebuild produces same subspace layout as save-time")
    func graphTripleStoreSubspaceLayout() async throws {
        let container = try await setupContainer([RebuildTripleStatement.self])
        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        try await setIndexStatesToReadable(for: RebuildTripleStatement.self, container: container)

        let context = container.newContext()
        var s1 = RebuildTripleStatement(); s1.subject = "A"; s1.predicate = "p"; s1.object = "B"
        var s2 = RebuildTripleStatement(); s2.subject = "C"; s2.predicate = "q"; s2.object = "D"
        context.insert(s1); context.insert(s2)
        try await context.save()

        let saveKeys = try await getIndexKeys(for: RebuildTripleStatement.self, container: container)
        // tripleStore: 2 statements × 3 orderings (spo, pos, osp) = 6
        #expect(saveKeys.count == 6, "tripleStore: 2 records × 3 = 6 keys, got \(saveKeys.count)")

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildTripleStatement.self)!, progress: nil)

        let rebuildKeys = try await getIndexKeys(for: RebuildTripleStatement.self, container: container)
        #expect(saveKeys.count == rebuildKeys.count,
            "tripleStore: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
        #expect(saveKeys == rebuildKeys, "tripleStore: rebuild keys must match save-time keys")

        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
    }

    @Test("GraphIndex adjacency: rebuild produces same subspace layout as save-time")
    func graphAdjacencySubspaceLayout() async throws {
        let container = try await setupContainer([RebuildEdge.self])
        try await cleanup(container: container, path: ["test", "rebuild", "edge"])
        try await setIndexStatesToReadable(for: RebuildEdge.self, container: container)

        let context = container.newContext()
        var e1 = RebuildEdge(); e1.source = "Alice"; e1.relation = "knows"; e1.target = "Bob"
        var e2 = RebuildEdge(); e2.source = "Bob"; e2.relation = "knows"; e2.target = "Carol"
        context.insert(e1); context.insert(e2)
        try await context.save()

        let saveKeys = try await getIndexKeys(for: RebuildEdge.self, container: container)
        // adjacency: 2 edges × 2 orderings (out, in) = 4
        #expect(saveKeys.count == 4, "adjacency: 2 records × 2 = 4 keys, got \(saveKeys.count)")

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildEdge.self)!, progress: nil)

        let rebuildKeys = try await getIndexKeys(for: RebuildEdge.self, container: container)
        #expect(saveKeys.count == rebuildKeys.count,
            "adjacency: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
        #expect(saveKeys == rebuildKeys, "adjacency: rebuild keys must match save-time keys")

        try await cleanup(container: container, path: ["test", "rebuild", "edge"])
    }

    @Test("FullTextIndex: rebuild produces same subspace layout as save-time")
    func fullTextSubspaceLayout() async throws {
        let container = try await setupContainer([RebuildArticle.self])
        try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
        try await setIndexStatesToReadable(for: RebuildArticle.self, container: container)

        let context = container.newContext()
        var a1 = RebuildArticle(); a1.title = "T1"; a1.content = "hello world"
        var a2 = RebuildArticle(); a2.title = "T2"; a2.content = "hello swift"
        context.insert(a1); context.insert(a2)
        try await context.save()

        let saveKeys = try await getIndexKeys(for: RebuildArticle.self, container: container)
        #expect(saveKeys.count > 0, "FullText: should have index entries, got \(saveKeys.count)")

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildArticle.self)!, progress: nil)

        let rebuildKeys = try await getIndexKeys(for: RebuildArticle.self, container: container)
        #expect(saveKeys.count == rebuildKeys.count,
            "FullText: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
        #expect(saveKeys == rebuildKeys, "FullText: rebuild keys must match save-time keys")

        try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
    }

    @Test("CountIndex: rebuild produces same subspace layout as save-time")
    func countSubspaceLayout() async throws {
        let container = try await setupContainer([RebuildCountItem.self])
        try await cleanup(container: container, path: ["test", "rebuild", "count"])
        try await setIndexStatesToReadable(for: RebuildCountItem.self, container: container)

        let context = container.newContext()
        var c1 = RebuildCountItem(); c1.category = "electronics"; c1.value = 100
        var c2 = RebuildCountItem(); c2.category = "electronics"; c2.value = 200
        var c3 = RebuildCountItem(); c3.category = "books"; c3.value = 50
        context.insert(c1); context.insert(c2); context.insert(c3)
        try await context.save()

        let saveKeys = try await getIndexKeys(for: RebuildCountItem.self, container: container)
        // Count: 2 groups (electronics, books) → 2 keys
        #expect(saveKeys.count == 2, "Count: 2 groups → 2 keys, got \(saveKeys.count)")

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildCountItem.self)!, progress: nil)

        let rebuildKeys = try await getIndexKeys(for: RebuildCountItem.self, container: container)
        #expect(saveKeys.count == rebuildKeys.count,
            "Count: save=\(saveKeys.count) vs rebuild=\(rebuildKeys.count)")
        #expect(saveKeys == rebuildKeys, "Count: rebuild keys must match save-time keys")

        try await cleanup(container: container, path: ["test", "rebuild", "count"])
    }

    // MARK: - B. Round-trip Query Tests

    @Test("ScalarIndex: rebuild entries are queryable")
    func scalarRoundTrip() async throws {
        let container = try await setupContainer([RebuildScalarUser.self])
        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        try await setIndexStatesToReadable(for: RebuildScalarUser.self, container: container)

        let context = container.newContext()
        var u1 = RebuildScalarUser(); u1.email = "alice@test.com"; u1.city = "Tokyo"
        context.insert(u1)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildScalarUser.self)!, progress: nil)

        let all = try await context.fetch(RebuildScalarUser.self).execute()
        #expect(all.count == 1, "ScalarIndex round-trip: expected 1 user, got \(all.count)")
        #expect(all.first?.email == "alice@test.com")

        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
    }

    @Test("GraphIndex tripleStore: rebuild entries are queryable via SPARQL")
    func graphTripleStoreRoundTrip() async throws {
        let container = try await setupContainer([RebuildTripleStatement.self])
        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        try await setIndexStatesToReadable(for: RebuildTripleStatement.self, container: container)

        let context = container.newContext()
        var s1 = RebuildTripleStatement()
        s1.subject = "Alice"; s1.predicate = "knows"; s1.object = "Bob"
        var s2 = RebuildTripleStatement()
        s2.subject = "Alice"; s2.predicate = "knows"; s2.object = "Carol"
        var s3 = RebuildTripleStatement()
        s3.subject = "Bob"; s3.predicate = "knows"; s3.object = "Dave"
        context.insert(s1); context.insert(s2); context.insert(s3)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildTripleStatement.self)!, progress: nil)

        // Forward query: Alice knows ?friend
        let results = try await context.sparql(RebuildTripleStatement.self)
            .defaultIndex()
            .where("Alice", "knows", "?friend")
            .select("?friend")
            .execute()

        #expect(results.count == 2,
            "After rebuild: Alice should know 2 people, got \(results.count)")
        let friends = results.nonNilValues(for: "?friend")
        #expect(friends.contains(.string("Bob")))
        #expect(friends.contains(.string("Carol")))

        // Reverse query (uses different index ordering: POS)
        let reverseResults = try await context.sparql(RebuildTripleStatement.self)
            .defaultIndex()
            .where("?person", "knows", "Bob")
            .select("?person")
            .execute()

        #expect(reverseResults.count == 1,
            "After rebuild: 1 person should know Bob, got \(reverseResults.count)")
        #expect(reverseResults.first?.string("?person") == "Alice")

        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
    }

    @Test("GraphIndex adjacency: rebuild entries are queryable via graph builder")
    func graphAdjacencyRoundTrip() async throws {
        let container = try await setupContainer([RebuildEdge.self])
        try await cleanup(container: container, path: ["test", "rebuild", "edge"])
        try await setIndexStatesToReadable(for: RebuildEdge.self, container: container)

        let context = container.newContext()
        var e1 = RebuildEdge(); e1.source = "Alice"; e1.relation = "follows"; e1.target = "Bob"
        var e2 = RebuildEdge(); e2.source = "Alice"; e2.relation = "follows"; e2.target = "Carol"
        context.insert(e1); context.insert(e2)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildEdge.self)!, progress: nil)

        // Graph query: outgoing edges from Alice
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

    @Test("FullTextIndex: rebuild entries are searchable")
    func fullTextRoundTrip() async throws {
        let container = try await setupContainer([RebuildArticle.self])
        try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
        try await setIndexStatesToReadable(for: RebuildArticle.self, container: container)

        let context = container.newContext()
        var a1 = RebuildArticle(); a1.title = "Swift Guide"; a1.content = "swift programming language"
        var a2 = RebuildArticle(); a2.title = "Rust Guide"; a2.content = "rust programming language"
        context.insert(a1); context.insert(a2)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildArticle.self)!, progress: nil)

        // Search for unique term
        let swiftResults = try await context.search(RebuildArticle.self)
            .fullText(\.content)
            .terms(["swift"])
            .execute()

        #expect(swiftResults.count == 1,
            "After rebuild: 'swift' should match 1 article, got \(swiftResults.count)")

        // Search for common term
        let commonResults = try await context.search(RebuildArticle.self)
            .fullText(\.content)
            .terms(["programming"])
            .execute()

        #expect(commonResults.count == 2,
            "After rebuild: 'programming' should match 2 articles, got \(commonResults.count)")

        try await cleanup(container: container, path: ["test", "rebuild", "fulltext"])
    }

    @Test("CountIndex: rebuild entries are queryable")
    func countRoundTrip() async throws {
        let container = try await setupContainer([RebuildCountItem.self])
        try await cleanup(container: container, path: ["test", "rebuild", "count"])
        try await setIndexStatesToReadable(for: RebuildCountItem.self, container: container)

        let context = container.newContext()
        var c1 = RebuildCountItem(); c1.category = "electronics"; c1.value = 100
        var c2 = RebuildCountItem(); c2.category = "electronics"; c2.value = 200
        var c3 = RebuildCountItem(); c3.category = "books"; c3.value = 50
        context.insert(c1); context.insert(c2); context.insert(c3)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildCountItem.self)!, progress: nil)

        let stats = try await admin.indexStatistics(getIndexName(for: RebuildCountItem.self)!)
        #expect(stats.entryCount == 2,
            "After rebuild: 2 groups should yield 2 entries, got \(stats.entryCount)")

        try await cleanup(container: container, path: ["test", "rebuild", "count"])
    }

    // MARK: - C. AdminContext State Visibility

    @Test("AdminContext state changes visible to IndexStateManager")
    func adminContextStateVisibleToFDBDataStore() async throws {
        let container = try await setupContainer([RebuildScalarUser.self])
        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
        try await setIndexStatesToReadable(for: RebuildScalarUser.self, container: container)

        let context = container.newContext()
        var u1 = RebuildScalarUser(); u1.email = "test@test.com"
        context.insert(u1)
        try await context.save()

        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildScalarUser.self)!, progress: nil)

        // Read state via IndexStateManager (entity root subspace)
        let entitySubspace = try await container.resolveDirectory(for: RebuildScalarUser.self)
        let stateManager = IndexStateManager(container: container, subspace: entitySubspace)
        let state = try await stateManager.state(of: getIndexName(for: RebuildScalarUser.self)!)

        #expect(state == .readable,
            "IndexStateManager should see index as readable after rebuild, got \(state)")

        try await cleanup(container: container, path: ["test", "rebuild", "scalar"])
    }

    // MARK: - D. Rebuild + CRUD Consistency

    @Test("Insert after rebuild correctly maintains index")
    func insertAfterRebuildMaintainsIndex() async throws {
        let container = try await setupContainer([RebuildTripleStatement.self])
        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
        try await setIndexStatesToReadable(for: RebuildTripleStatement.self, container: container)

        let context = container.newContext()
        var s1 = RebuildTripleStatement()
        s1.subject = "Alice"; s1.predicate = "knows"; s1.object = "Bob"
        context.insert(s1)
        try await context.save()

        // Rebuild
        let admin = container.newAdminContext()
        try await admin.rebuildIndex(getIndexName(for: RebuildTripleStatement.self)!, progress: nil)

        // Insert AFTER rebuild
        var s2 = RebuildTripleStatement()
        s2.subject = "Alice"; s2.predicate = "knows"; s2.object = "Carol"
        context.insert(s2)
        try await context.save()

        // Query should find BOTH pre- and post-rebuild data
        let results = try await context.sparql(RebuildTripleStatement.self)
            .defaultIndex()
            .where("Alice", "knows", "?friend")
            .select("?friend")
            .execute()

        #expect(results.count == 2,
            "After rebuild + insert: expected 2 friends, got \(results.count)")
        let friends = results.nonNilValues(for: "?friend")
        #expect(friends.contains(.string("Bob")), "Pre-rebuild data must survive")
        #expect(friends.contains(.string("Carol")), "Post-rebuild data must be indexed")

        try await cleanup(container: container, path: ["test", "rebuild", "triple"])
    }
}

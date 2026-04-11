#if SQLITE
import Testing
import Foundation
import Core
import Database
import DatabaseServer
import DatabaseClientProtocol
import QueryIR

@Persistable
private struct RPCPerson {
    #Directory<RPCPerson>("test", "server", "people")

    var id: String = UUID().uuidString
    var name: String = ""
    var age: Int = 0
}

@Persistable
private struct RPCNote {
    #Directory<RPCNote>("test", "server", "notes")

    var id: String = UUID().uuidString
    var userID: String = ""
    var title: String = ""
}

@Persistable
private struct RPCEdge {
    #Directory<RPCEdge>("test", "server", "edges")

    var id: String = UUID().uuidString
    var from: String = ""
    var target: String = ""
    var label: String = ""
    var since: Int = 0

    #Index(
        GraphIndexKind<RPCEdge>(
            from: \.from,
            edge: \.label,
            to: \.target,
            graph: nil,
            strategy: .tripleStore
        ),
        storedFields: [\RPCEdge.since],
        name: "rpc_social_graph"
    )
}

private struct RPCTenantOrder: Persistable {
    typealias ID = String

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var userID: String = ""
    var status: String = ""

    static var persistableType: String { "RPCTenantOrder" }
    static var allFields: [String] { ["id", "tenantID", "userID", "status"] }
    static var directoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Field<RPCTenantOrder>(\.tenantID), Path("tenant-orders")]
    }
    static var directoryLayer: DirectoryLayer { .partition }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "tenantID": return tenantID
        case "userID": return userID
        case "status": return status
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<RPCTenantOrder, Value>) -> String {
        switch keyPath {
        case \RPCTenantOrder.id: return "id"
        case \RPCTenantOrder.tenantID: return "tenantID"
        case \RPCTenantOrder.userID: return "userID"
        case \RPCTenantOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<RPCTenantOrder>) -> String {
        switch keyPath {
        case \RPCTenantOrder.id: return "id"
        case \RPCTenantOrder.tenantID: return "tenantID"
        case \RPCTenantOrder.userID: return "userID"
        case \RPCTenantOrder.status: return "status"
        default: return "\(keyPath)"
        }
    }
}

@Suite("Canonical Query RPC Tests", .serialized)
struct CanonicalQueryRPCTests {
    @Test("canonical query RPC executes subquery sources")
    func subquerySource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var alice = RPCPerson()
        alice.id = "person-alice"
        alice.name = "Alice"
        alice.age = 42
        context.insert(alice)

        var bob = RPCPerson()
        bob.id = "person-bob"
        bob.name = "Bob"
        bob.age = 19
        context.insert(bob)

        var carol = RPCPerson()
        carol.id = "person-carol"
        carol.name = "Carol"
        carol.age = 34
        context.insert(carol)

        try await context.save()

        let adults = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("id"))),
                ProjectionItem(.column(ColumnRef("name"))),
                ProjectionItem(.column(ColumnRef("age"))),
            ]),
            source: .table(TableRef(table: "RPCPerson")),
            filter: .greaterThanOrEqual(
                .column(ColumnRef("age")),
                .literal(.int(30))
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "adults", column: "name")), alias: "name"),
            ]),
            source: .subquery(adults, alias: "adults"),
            orderBy: [
                SortKey(.column(ColumnRef(table: "adults", column: "name")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let names = response.rows.compactMap { $0.fields["name"]?.stringValue }

        #expect(names == ["Alice", "Carol"])
    }

    @Test("canonical query RPC executes join sources")
    func joinSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var alice = RPCPerson()
        alice.id = "join-alice"
        alice.name = "Alice"
        alice.age = 42
        context.insert(alice)

        var bob = RPCPerson()
        bob.id = "join-bob"
        bob.name = "Bob"
        bob.age = 29
        context.insert(bob)

        var note1 = RPCNote()
        note1.id = "note-1"
        note1.userID = alice.id
        note1.title = "Architecture"
        context.insert(note1)

        var note2 = RPCNote()
        note2.id = "note-2"
        note2.userID = alice.id
        note2.title = "Vector"
        context.insert(note2)

        var note3 = RPCNote()
        note3.id = "note-3"
        note3.userID = bob.id
        note3.title = "Ignored"
        context.insert(note3)

        try await context.save()

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef(table: "RPCPerson", alias: "p")),
            right: .table(TableRef(table: "RPCNote", alias: "n")),
            condition: .on(
                .equal(
                    .column(ColumnRef(table: "p", column: "id")),
                    .column(ColumnRef(table: "n", column: "userID"))
                )
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "p", column: "name")), alias: "person"),
                ProjectionItem(.column(ColumnRef(table: "n", column: "title")), alias: "title"),
            ]),
            source: .join(join),
            filter: .equal(
                .column(ColumnRef(table: "p", column: "name")),
                .literal(.string("Alice"))
            ),
            orderBy: [
                SortKey(.column(ColumnRef(table: "n", column: "title")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let titles = response.rows.compactMap { $0.fields["title"]?.stringValue }
        let people = Set(response.rows.compactMap { $0.fields["person"]?.stringValue })

        #expect(titles == ["Architecture", "Vector"])
        #expect(people == ["Alice"])
    }

    @Test("canonical query RPC executes graph table sources")
    func graphTableSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "edge-2"
        edge2.from = "alice"
        edge2.target = "carol"
        edge2.label = "KNOWS"
        edge2.since = 2021
        context.insert(edge2)

        try await context.save()

        let graphSource = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a")),
                    .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                    .node(NodePattern(variable: "b")),
                ])
            ])
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("source")), alias: "source"),
                ProjectionItem(.column(ColumnRef("target")), alias: "target"),
                ProjectionItem(.column(ColumnRef("since")), alias: "since"),
            ]),
            source: .graphTable(graphSource),
            orderBy: [
                SortKey(.column(ColumnRef("target")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let targets = response.rows.compactMap { $0.fields["target"]?.stringValue }
        let since = response.rows.compactMap { $0.fields["since"]?.int64Value }

        #expect(targets == ["bob", "carol"])
        #expect(since == [2020, 2021])
    }

    @Test("canonical query RPC executes graph pattern sources")
    func graphPatternSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "pattern-edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "pattern-edge-2"
        edge2.from = "alice"
        edge2.target = "carol"
        edge2.label = "KNOWS"
        edge2.since = 2021
        context.insert(edge2)

        var edge3 = RPCEdge()
        edge3.id = "pattern-edge-3"
        edge3.from = "dave"
        edge3.target = "erin"
        edge3.label = "FOLLOWS"
        edge3.since = 2022
        context.insert(edge3)

        try await context.save()

        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("source"),
                predicate: .iri("KNOWS"),
                object: .variable("target")
            )
        ])

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("target")), alias: "target"),
                ProjectionItem(.variable(Variable("source")), alias: "source"),
            ]),
            source: .graphPattern(pattern),
            orderBy: [
                SortKey(.variable(Variable("target")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let targets = response.rows.compactMap { $0.fields["target"]?.stringValue }
        let sources = Set(response.rows.compactMap { $0.fields["source"]?.stringValue })

        #expect(targets == ["bob", "carol"])
        #expect(sources == ["alice"])
    }

    @Test("canonical query RPC routes partition values through subquery sources")
    func subquerySourceWithPartitionValues() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var tenantAOpen = RPCTenantOrder()
        tenantAOpen.id = "tenant-a-open"
        tenantAOpen.tenantID = "tenant-a"
        tenantAOpen.userID = "alice"
        tenantAOpen.status = "open"
        context.insert(tenantAOpen)

        var tenantAClosed = RPCTenantOrder()
        tenantAClosed.id = "tenant-a-closed"
        tenantAClosed.tenantID = "tenant-a"
        tenantAClosed.userID = "bob"
        tenantAClosed.status = "closed"
        context.insert(tenantAClosed)

        var tenantBOpen = RPCTenantOrder()
        tenantBOpen.id = "tenant-b-open"
        tenantBOpen.tenantID = "tenant-b"
        tenantBOpen.userID = "carol"
        tenantBOpen.status = "open"
        context.insert(tenantBOpen)

        try await context.save()

        let orders = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef("id"))),
                ProjectionItem(.column(ColumnRef("userID"))),
                ProjectionItem(.column(ColumnRef("status"))),
            ]),
            source: .table(TableRef(table: "RPCTenantOrder")),
            filter: .equal(
                .column(ColumnRef("status")),
                .literal(.string("open"))
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "orders", column: "userID")), alias: "userID"),
            ]),
            source: .subquery(orders, alias: "orders"),
            orderBy: [
                SortKey(.column(ColumnRef(table: "orders", column: "userID")))
            ]
        )

        let response = try await send(
            query,
            partitionValues: ["tenantID": "tenant-a"],
            endpoint: endpoint
        )
        let userIDs = response.rows.compactMap { $0.fields["userID"]?.stringValue }

        #expect(userIDs == ["alice"])
    }

    @Test("canonical query RPC routes partition values through join sources")
    func joinSourceWithPartitionValues() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var orderA = RPCTenantOrder()
        orderA.id = "tenant-a-order"
        orderA.tenantID = "tenant-a"
        orderA.userID = "alice"
        orderA.status = "open"
        context.insert(orderA)

        var orderB = RPCTenantOrder()
        orderB.id = "tenant-b-order"
        orderB.tenantID = "tenant-b"
        orderB.userID = "bob"
        orderB.status = "open"
        context.insert(orderB)

        var noteA = RPCNote()
        noteA.id = "tenant-note-a"
        noteA.userID = "alice"
        noteA.title = "Tenant A"
        context.insert(noteA)

        var noteB = RPCNote()
        noteB.id = "tenant-note-b"
        noteB.userID = "bob"
        noteB.title = "Tenant B"
        context.insert(noteB)

        try await context.save()

        let join = JoinClause(
            type: .inner,
            left: .table(TableRef(table: "RPCTenantOrder", alias: "o")),
            right: .table(TableRef(table: "RPCNote", alias: "n")),
            condition: .on(
                .equal(
                    .column(ColumnRef(table: "o", column: "userID")),
                    .column(ColumnRef(table: "n", column: "userID"))
                )
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "n", column: "title")), alias: "title"),
            ]),
            source: .join(join),
            orderBy: [
                SortKey(.column(ColumnRef(table: "n", column: "title")))
            ]
        )

        let response = try await send(
            query,
            partitionValues: ["tenantID": "tenant-a"],
            endpoint: endpoint
        )
        let titles = response.rows.compactMap { $0.fields["title"]?.stringValue }

        #expect(titles == ["Tenant A"])
    }

    private func makeHarness() async throws -> (DBContainer, DatabaseEndpoint) {
        let schema = Schema(
            [RPCPerson.self, RPCNote.self, RPCEdge.self, RPCTenantOrder.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        return (container, DatabaseEndpoint(container: container))
    }

    private func send(
        _ selectQuery: SelectQuery,
        partitionValues: [String: String]? = nil,
        endpoint: DatabaseEndpoint
    ) async throws -> QueryResponse {
        let request = QueryRequest(
            statement: .select(selectQuery),
            partitionValues: partitionValues
        )
        let payload = try JSONEncoder().encode(request)
        let envelope = ServiceEnvelope(operationID: "query", payload: payload)
        let requestData = try JSONEncoder().encode(envelope)

        let responseData = await endpoint.handleRequest(requestData)
        let responseEnvelope = try JSONDecoder().decode(ServiceEnvelope.self, from: responseData)

        if responseEnvelope.isError == true {
            throw ServiceError(
                code: responseEnvelope.errorCode ?? "UNKNOWN",
                message: responseEnvelope.errorMessage ?? "Unknown service error"
            )
        }

        return try JSONDecoder().decode(QueryResponse.self, from: responseEnvelope.payload)
    }
}
#endif

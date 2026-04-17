#if SQLITE
import Testing
import Foundation
import Core
import DatabaseEngine
import Database
import DatabaseServer
import DatabaseClientProtocol
import QueryIR
import StorageKit
import BitmapIndex
import FullTextIndex

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

private protocol RPCDocument: Polymorphable {
    var id: String { get }
    var ownerID: String { get }
    var title: String { get }
}

private extension RPCDocument {
    static var polymorphableType: String { "RPCDocument" }
    static var polymorphicDirectoryPathComponents: [any DirectoryPathElement] {
        [Path("test"), Path("server"), Path("documents")]
    }
    static var polymorphicIndexDescriptors: [IndexDescriptor] {
        [
            IndexDescriptor(
                name: "rpc_document_title",
                keyPaths: [] as [PartialKeyPath<RPCArticle>],
                kind: FullTextIndexKind<RPCArticle>(fieldNames: ["title"])
            )
        ]
    }
}

private struct RPCTestAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

private struct RPCPolymorphicAccessPathExecutor: PolymorphicIndexReadExecutor {
    let kindIdentifier = "test.rpc.polymorphic"

    func execute(
        context: FDBContext,
        selectQuery: SelectQuery,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionOptions,
        partitionValues: [String : String]?
    ) async throws -> QueryResponse {
        QueryResponse(
            rows: [
                QueryRow(
                    fields: [
                        "id": .string("poly-executor-1"),
                        "title": .string("Executor Result")
                    ],
                    annotations: [
                        "_typeName": .string("RPCArticle"),
                        "_typeCode": .int64(1),
                        "group": .string(group.identifier)
                    ]
                )
            ]
        )
    }
}

@Persistable
private struct RPCArticle: RPCDocument, SecurityPolicy {
    #Directory<RPCArticle>("test", "server", "articles")

    var id: String = UUID().uuidString
    var ownerID: String = ""
    var title: String = ""
    var body: String = ""

    static func allowGet(resource: RPCArticle, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<RPCArticle>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: RPCArticle, auth: (any AuthContext)?) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(resource: RPCArticle, newResource: RPCArticle, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID && newResource.ownerID == auth?.userID
    }

    static func allowDelete(resource: RPCArticle, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }
}

@Persistable
private struct RPCReport: RPCDocument, SecurityPolicy {
    #Directory<RPCReport>("test", "server", "reports")

    var id: String = UUID().uuidString
    var ownerID: String = ""
    var title: String = ""
    var summary: String = ""

    static func allowGet(resource: RPCReport, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(query: SecurityQuery<RPCReport>, auth: (any AuthContext)?) -> Bool {
        auth != nil
    }

    static func allowCreate(newResource: RPCReport, auth: (any AuthContext)?) -> Bool {
        newResource.ownerID == auth?.userID
    }

    static func allowUpdate(resource: RPCReport, newResource: RPCReport, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID && newResource.ownerID == auth?.userID
    }

    static func allowDelete(resource: RPCReport, auth: (any AuthContext)?) -> Bool {
        resource.ownerID == auth?.userID
    }
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

    @Test("canonical query RPC executes graph table path constraints")
    func graphTablePathConstraints() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "path-edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "path-edge-2"
        edge2.from = "bob"
        edge2.target = "carol"
        edge2.label = "KNOWS"
        edge2.since = 2021
        context.insert(edge2)

        var edge3 = RPCEdge()
        edge3.id = "path-edge-3"
        edge3.from = "alice"
        edge3.target = "dave"
        edge3.label = "KNOWS"
        edge3.since = 2022
        context.insert(edge3)

        var edge4 = RPCEdge()
        edge4.id = "path-edge-4"
        edge4.from = "dave"
        edge4.target = "erin"
        edge4.label = "KNOWS"
        edge4.since = 2023
        context.insert(edge4)

        try await context.save()

        let graphSource = GraphTableSource(
            graphName: "SocialGraph",
            matchPattern: MatchPattern(paths: [
                PathPattern(elements: [
                    .node(NodePattern(variable: "a", properties: [
                        PropertyBinding(key: "id", value: .literal(.string("alice")))
                    ])),
                    .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                    .node(NodePattern(variable: "b", properties: [
                        PropertyBinding(key: "id", value: .literal(.string("bob")))
                    ])),
                    .edge(EdgePattern(labels: ["KNOWS"], direction: .outgoing)),
                    .node(NodePattern(variable: "c")),
                ])
            ])
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "a", column: "id")), alias: "source"),
                ProjectionItem(.column(ColumnRef(table: "c", column: "id")), alias: "target"),
            ]),
            source: .graphTable(graphSource)
        )

        let response = try await send(query, endpoint: endpoint)
        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["source"]?.stringValue == "alice")
        #expect(response.rows.first?.fields["target"]?.stringValue == "carol")
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

    @Test("canonical query RPC executes nested graph pattern sources")
    func nestedGraphPatternSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "nested-pattern-edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "nested-pattern-edge-2"
        edge2.from = "alice"
        edge2.target = "carol"
        edge2.label = "KNOWS"
        edge2.since = 2021
        context.insert(edge2)

        try await context.save()

        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("source"),
                predicate: .iri("KNOWS"),
                object: .variable("target")
            )
        ])

        let inner = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("target")), alias: "target"),
            ]),
            source: .graphPattern(pattern)
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "g", column: "target")), alias: "target"),
            ]),
            source: .subquery(inner, alias: "g"),
            orderBy: [
                SortKey(.column(ColumnRef(table: "g", column: "target")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let targets = response.rows.compactMap { $0.fields["target"]?.stringValue }

        #expect(targets == ["bob", "carol"])
    }

    @Test("canonical query RPC executes graph pattern sources inside joins")
    func graphPatternJoinSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var alice = RPCPerson()
        alice.id = "graph-join-alice"
        alice.name = "Alice"
        alice.age = 42
        context.insert(alice)

        var bob = RPCPerson()
        bob.id = "graph-join-bob"
        bob.name = "Bob"
        bob.age = 29
        context.insert(bob)

        var edge1 = RPCEdge()
        edge1.id = "graph-join-edge-1"
        edge1.from = alice.id
        edge1.target = bob.id
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        try await context.save()

        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("source"),
                predicate: .iri("KNOWS"),
                object: .variable("target")
            )
        ])

        let join = JoinClause(
            type: .inner,
            left: .subquery(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(.variable(Variable("source")), alias: "source"),
                        ProjectionItem(.variable(Variable("target")), alias: "target"),
                    ]),
                    source: .graphPattern(pattern)
                ),
                alias: "g"
            ),
            right: .table(TableRef(table: "RPCPerson", alias: "p")),
            condition: .on(
                .equal(
                    .column(ColumnRef(table: "g", column: "target")),
                    .column(ColumnRef(table: "p", column: "id"))
                )
            )
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "p", column: "name")), alias: "name"),
            ]),
            source: .join(join)
        )

        let response = try await send(query, endpoint: endpoint)
        let names = response.rows.compactMap { $0.fields["name"]?.stringValue }

        #expect(names == ["Bob"])
    }

    @Test("canonical query RPC executes graph pattern sources inside unions")
    func graphPatternUnionSource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "graph-union-edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "graph-union-edge-2"
        edge2.from = "dave"
        edge2.target = "erin"
        edge2.label = "FOLLOWS"
        edge2.since = 2021
        context.insert(edge2)

        try await context.save()

        let knowsPattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("source"),
                predicate: .iri("KNOWS"),
                object: .variable("target")
            )
        ])
        let followsPattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("source"),
                predicate: .iri("FOLLOWS"),
                object: .variable("target")
            )
        ])

        let knowsQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("target")), alias: "target"),
            ]),
            source: .graphPattern(knowsPattern)
        )
        let followsQuery = SelectQuery(
            projection: .items([
                ProjectionItem(.variable(Variable("target")), alias: "target"),
            ]),
            source: .graphPattern(followsPattern)
        )

        let query = SelectQuery(
            projection: .items([
                ProjectionItem(.column(ColumnRef(table: "g", column: "target")), alias: "target"),
            ]),
            source: .subquery(
                SelectQuery(
                    projection: .items([
                        ProjectionItem(.column(ColumnRef("target")), alias: "target"),
                    ]),
                    source: .union([
                        .subquery(knowsQuery, alias: "knows"),
                        .subquery(followsQuery, alias: "follows"),
                    ])
                ),
                alias: "g"
            ),
            orderBy: [
                SortKey(.column(ColumnRef(table: "g", column: "target")))
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let targets = response.rows.compactMap { $0.fields["target"]?.stringValue }

        #expect(targets == ["bob", "erin"])
    }

    @Test("canonical query RPC executes graph pattern named subqueries")
    func graphPatternNamedSubquerySource() async throws {
        let (container, endpoint) = try await makeHarness()
        let context = container.newContext()

        var edge1 = RPCEdge()
        edge1.id = "graph-named-edge-1"
        edge1.from = "alice"
        edge1.target = "bob"
        edge1.label = "KNOWS"
        edge1.since = 2020
        context.insert(edge1)

        var edge2 = RPCEdge()
        edge2.id = "graph-named-edge-2"
        edge2.from = "alice"
        edge2.target = "carol"
        edge2.label = "KNOWS"
        edge2.since = 2021
        context.insert(edge2)

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
                ProjectionItem(.column(ColumnRef(table: "g", column: "target")), alias: "target"),
            ]),
            source: .table(TableRef(table: "graph_hits", alias: "g")),
            orderBy: [
                SortKey(.column(ColumnRef(table: "g", column: "target")))
            ],
            subqueries: [
                NamedSubquery(
                    name: "graph_hits",
                    query: SelectQuery(
                        projection: .items([
                            ProjectionItem(.variable(Variable("target")), alias: "target"),
                        ]),
                        source: .graphPattern(pattern)
                    )
                )
            ]
        )

        let response = try await send(query, endpoint: endpoint)
        let targets = response.rows.compactMap { $0.fields["target"]?.stringValue }

        #expect(targets == ["bob", "carol"])
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

    @Test("canonical query RPC preserves polymorphic annotations through subquery sources")
    func polymorphicSubqueryPreservesAnnotations() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-article"
        article.ownerID = "alice"
        article.title = "Article"
        article.body = "Architecture"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-report"
        report.ownerID = "alice"
        report.title = "Report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        let base = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            )
        )
        let query = SelectQuery(
            projection: .all,
            source: .subquery(base, alias: "docs"),
            orderBy: [SortKey(.column(ColumnRef(table: "docs", column: "title")))]
        )

        let response = try await send(query, endpoint: endpoint)

        #expect(response.rows.count == 2)
        #expect(response.rows.allSatisfy { $0.annotations["_typeName"]?.stringValue != nil })
        #expect(response.rows.allSatisfy { $0.annotations["_typeCode"]?.int64Value != nil })
    }

    @Test("canonical polymorphic query applies list and get security filtering")
    func polymorphicLogicalSourceRespectsSecurity() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(
            security: .enabled(strict: true)
        )

        try await AuthContextKey.$current.withValue(RPCTestAuth(userID: "alice")) {
            let context = container.newContext()
            var article = RPCArticle()
            article.id = "alice-article"
            article.ownerID = "alice"
            article.title = "Alice Article"
            article.body = "Body"
            context.insert(article)
            try await context.save()
        }

        try await AuthContextKey.$current.withValue(RPCTestAuth(userID: "bob")) {
            let context = container.newContext()
            var report = RPCReport()
            report.id = "bob-report"
            report.ownerID = "bob"
            report.title = "Bob Report"
            report.summary = "Summary"
            context.insert(report)
            try await context.save()
        }

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            orderBy: [SortKey(.column(ColumnRef("title")))]
        )

        let response = try await AuthContextKey.$current.withValue(RPCTestAuth(userID: "alice")) {
            try await send(query, endpoint: endpoint)
        }

        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["title"]?.stringValue == "Alice Article")
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == "RPCArticle")
    }

    @Test("developer polymorphic query decodes mixed concrete results")
    func developerPolymorphicQueryDecodesMixedResults() async throws {
        let (container, _) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "developer-poly-article"
        article.ownerID = "alice"
        article.title = "Article"
        article.body = "Body"
        context.insert(article)

        var report = RPCReport()
        report.id = "developer-poly-report"
        report.ownerID = "alice"
        report.title = "Report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        let results = try await context.findPolymorphic(RPCArticle.self)
            .orderBy(\.title)
            .execute()

        #expect(results.count == 2)
        #expect(results.map(\.typeName) == ["RPCArticle", "RPCReport"])
        #expect(results[0].item(as: RPCArticle.self)?.body == "Body")
        #expect(results[1].item(as: RPCReport.self)?.summary == "Summary")
    }

    @Test("developer polymorphic full-text query hides logical source wiring")
    func developerPolymorphicFullTextQuery() async throws {
        let (container, _) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "developer-ft-article"
        article.ownerID = "user-1"
        article.title = "Vector architecture"
        article.body = "Document body"
        context.insert(article)

        var report = RPCReport()
        report.id = "developer-ft-report"
        report.ownerID = "user-2"
        report.title = "Quarterly report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicTerm(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_title",
            type: RPCArticle.self,
            id: article.id,
            term: "vector"
        )
        try await writePolymorphicTerm(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_title",
            type: RPCReport.self,
            id: report.id,
            term: "quarterly"
        )

        let results = try await context.findPolymorphic(RPCArticle.self)
            .fullText(\.title)
            .terms("vector")
            .execute()

        #expect(results.count == 1)
        #expect(results[0].typeName == RPCArticle.persistableType)
        #expect(results[0].item(as: RPCArticle.self)?.id == article.id)
    }

    @Test("canonical query RPC dispatches polymorphic access-path queries through the registry")
    func polymorphicAccessPathSource() async throws {
        ReadExecutorRegistry.shared.registerPolymorphic(RPCPolymorphicAccessPathExecutor())

        let (_, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_test",
                    kindIdentifier: "test.rpc.polymorphic",
                    parameters: [:]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)

        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["title"]?.stringValue == "Executor Result")
        #expect(response.rows.first?.annotations["group"]?.stringValue == "RPCDocument")
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == "RPCArticle")
    }

    @Test("canonical query RPC executes built-in polymorphic full-text access paths")
    func polymorphicBuiltInFullTextAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-ft-article"
        article.ownerID = "user-1"
        article.title = "Vector architecture"
        article.body = "Document body"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-ft-report"
        report.ownerID = "user-2"
        report.title = "Quarterly report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicTerm(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_title",
            type: RPCArticle.self,
            id: article.id,
            term: "vector"
        )
        try await writePolymorphicTerm(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_title",
            type: RPCReport.self,
            id: report.id,
            term: "quarterly"
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_title",
                    kindIdentifier: "fulltext",
                    parameters: [
                        "fieldName": .string("title"),
                        "terms": .array([.string("vector")]),
                        "matchMode": .string("all"),
                        "returnScores": .bool(false),
                        "includeFacets": .bool(false)
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)

        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["id"]?.stringValue == article.id)
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == RPCArticle.persistableType)
    }

    @Test("canonical query RPC executes built-in polymorphic vector access paths")
    func polymorphicBuiltInVectorAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-vector-article"
        article.ownerID = "user-1"
        article.title = "Vector article"
        article.body = "Document body"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-vector-report"
        report.ownerID = "user-2"
        report.title = "Vector report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicVector(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_embedding",
            type: RPCArticle.self,
            id: article.id,
            vector: [1, 0, 0]
        )
        try await writePolymorphicVector(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_embedding",
            type: RPCReport.self,
            id: report.id,
            vector: [0, 1, 0]
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_embedding",
                    kindIdentifier: "vector",
                    parameters: [
                        "fieldName": .string("embedding"),
                        "dimensions": .int(3),
                        "queryVector": .array([.double(1), .double(0), .double(0)]),
                        "k": .int(2),
                        "metric": .string("cosine")
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)
        let ids = response.rows.compactMap { $0.fields["id"]?.stringValue }

        #expect(ids == [article.id, report.id])
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == RPCArticle.persistableType)
        #expect((response.rows.first?.annotations["distance"]?.doubleValue ?? 1) < 0.0001)
    }

    @Test("canonical query RPC executes built-in polymorphic rank access paths")
    func polymorphicBuiltInRankAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-rank-article"
        article.ownerID = "user-1"
        article.title = "Article"
        article.body = "Body"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-rank-report"
        report.ownerID = "user-2"
        report.title = "Report"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicRankEntry(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_score",
            score: 100,
            type: RPCArticle.self,
            id: article.id
        )
        try await writePolymorphicRankEntry(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_score",
            score: 50,
            type: RPCReport.self,
            id: report.id
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_score",
                    kindIdentifier: "rank",
                    parameters: [
                        "fieldName": .string("score"),
                        "mode": .string("top"),
                        "count": .int(2)
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)
        let ids = response.rows.compactMap { $0.fields["id"]?.stringValue }
        let ranks = response.rows.compactMap { $0.annotations["rank"]?.int64Value }

        #expect(ids == [article.id, report.id])
        #expect(ranks == [0, 1])
    }

    @Test("canonical query RPC executes built-in polymorphic bitmap access paths")
    func polymorphicBuiltInBitmapAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-bitmap-article"
        article.ownerID = "user-1"
        article.title = "Architecture"
        article.body = "Body"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-bitmap-report"
        report.ownerID = "user-2"
        report.title = "Operations"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicBitmap(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_category",
            type: RPCArticle.self,
            id: article.id,
            seqID: 0,
            fieldValue: "tech"
        )
        try await writePolymorphicBitmap(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_category",
            type: RPCReport.self,
            id: report.id,
            seqID: 1,
            fieldValue: "ops"
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_category",
                    kindIdentifier: "bitmap",
                    parameters: [
                        "fieldName": .string("category"),
                        "operation": .string("equals"),
                        "values": .array([try DatabaseEngine.CanonicalTupleElementCodec.encode("tech")])
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)
        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["id"]?.stringValue == article.id)
    }

    @Test("canonical query RPC executes built-in polymorphic permuted access paths")
    func polymorphicBuiltInPermutedAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)
        let context = container.newContext()

        var article = RPCArticle()
        article.id = "poly-permuted-article"
        article.ownerID = "user-1"
        article.title = "Tokyo architecture"
        article.body = "Body"
        context.insert(article)

        var report = RPCReport()
        report.id = "poly-permuted-report"
        report.ownerID = "user-2"
        report.title = "Osaka operations"
        report.summary = "Summary"
        context.insert(report)

        try await context.save()

        try await writePolymorphicPermutedEntry(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_location",
            type: RPCArticle.self,
            id: article.id,
            permutedValues: ["tokyo", "jp"]
        )
        try await writePolymorphicPermutedEntry(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_location",
            type: RPCReport.self,
            id: report.id,
            permutedValues: ["osaka", "jp"]
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_location",
                    kindIdentifier: "permuted",
                    parameters: [
                        "queryType": .string("prefix"),
                        "values": .array([try DatabaseEngine.CanonicalTupleElementCodec.encode("tokyo")]),
                        "permutation": .array([.int(1), .int(0)])
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)

        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["id"]?.stringValue == article.id)
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == RPCArticle.persistableType)
    }

    @Test("canonical query RPC executes built-in polymorphic version access paths")
    func polymorphicBuiltInVersionAccessPath() async throws {
        let (container, endpoint) = try await makePolymorphicHarness(security: .disabled)

        var article = RPCArticle()
        article.id = "poly-version-article"
        article.ownerID = "user-1"
        article.title = "Versioned article"
        article.body = "Body"

        try await writePolymorphicVersionEntry(
            container: container,
            groupIdentifier: "RPCDocument",
            indexName: "rpc_document_version_id",
            type: RPCArticle.self,
            id: article.id,
            versionBytes: [0, 0, 0, 0, 0, 0, 0, 1, 0, 1],
            item: article
        )

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                LogicalSourceRef(
                    kindIdentifier: BuiltinLogicalSourceKind.polymorphic,
                    identifier: "RPCDocument"
                )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "rpc_document_version_id",
                    kindIdentifier: "version",
                    parameters: [
                        "primaryKey": .array([
                            try DatabaseEngine.CanonicalTupleElementCodec.encode(
                                RPCArticle.typeCode(for: RPCArticle.persistableType)
                            ),
                            try DatabaseEngine.CanonicalTupleElementCodec.encode(article.id)
                        ]),
                        "limit": .int(5)
                    ]
                )
            )
        )

        let response = try await send(query, endpoint: endpoint)

        #expect(response.rows.count == 1)
        #expect(response.rows.first?.fields["id"]?.stringValue == article.id)
        #expect(response.rows.first?.annotations["_typeName"]?.stringValue == RPCArticle.persistableType)
        #expect(response.rows.first?.annotations["version"]?.dataValue == Data([0, 0, 0, 0, 0, 0, 0, 1, 0, 1]))
    }

    private func makeHarness() async throws -> (DBContainer, DatabaseEndpoint) {
        let schema = Schema(
            [RPCPerson.self, RPCNote.self, RPCEdge.self, RPCTenantOrder.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(for: schema, security: .disabled)
        return (container, DatabaseEndpoint(container: container))
    }

    private func makePolymorphicHarness(
        security: SecurityConfiguration
    ) async throws -> (DBContainer, DatabaseEndpoint) {
        let schema = Schema(
            [RPCArticle.self, RPCReport.self],
            version: Schema.Version(1, 0, 0)
        )
        let container = try await DBContainer.inMemory(for: schema, security: security)
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

    private func writePolymorphicVector<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        type: T.Type,
        id: String,
        vector: [Float]
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let indexSubspace = directory.subspace(SubspaceKey.indexes).subspace(indexName)
        let key = indexSubspace.pack(polymorphicID(type: type, id: id))
        let value = Tuple(vector.map { $0 as any TupleElement }).pack()

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue(value, for: key)
        }
    }

    private func writePolymorphicTerm<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        type: T.Type,
        id: String,
        term: String
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let indexSubspace = directory.subspace(SubspaceKey.indexes).subspace(indexName)
        let key = indexSubspace.subspace("terms").subspace(term).pack(polymorphicID(type: type, id: id))

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue([], for: key)
        }
    }

    private func writePolymorphicRankEntry<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        score: Int,
        type: T.Type,
        id: String
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let key = directory
            .subspace(SubspaceKey.indexes)
            .subspace(indexName)
            .subspace("scores")
            .pack(Tuple([score, type.typeCode(for: type.persistableType), id]))

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue([], for: key)
        }
    }

    private func writePolymorphicBitmap<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        type: T.Type,
        id: String,
        seqID: UInt32,
        fieldValue: String
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let indexSubspace = directory.subspace(SubspaceKey.indexes).subspace(indexName)
        let tupleID = polymorphicID(type: type, id: id)

        var bitmap = RoaringBitmap()
        bitmap.add(seqID)
        let bitmapData = try bitmap.serialize()

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue(Array(bitmapData), for: indexSubspace.subspace("data").pack(Tuple(fieldValue)))
            transaction.setValue(tupleID.pack(), for: indexSubspace.subspace("ids").pack(Tuple(Int(seqID))))
            transaction.setValue(ByteConversion.int64ToBytes(Int64(seqID)), for: indexSubspace.subspace("pks").pack(tupleID))
        }
    }

    private func writePolymorphicPermutedEntry<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        type: T.Type,
        id: String,
        permutedValues: [String]
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let indexSubspace = directory.subspace(SubspaceKey.indexes).subspace(indexName)
        let tupleID = polymorphicID(type: type, id: id)
        let key = indexSubspace.pack(
            Tuple(
                permutedValues.map { $0 as any TupleElement } +
                (0..<tupleID.count).compactMap { tupleID[$0] }
            )
        )

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue([], for: key)
        }
    }

    private func writePolymorphicVersionEntry<T: Persistable & Polymorphable>(
        container: DBContainer,
        groupIdentifier: String,
        indexName: String,
        type: T.Type,
        id: String,
        versionBytes: [UInt8],
        item: T
    ) async throws {
        let directory = try await container.resolvePolymorphicDirectory(for: groupIdentifier)
        let indexSubspace = directory.subspace(SubspaceKey.indexes).subspace(indexName)
        let primaryKey = polymorphicID(type: type, id: id)
        let key = indexSubspace.pack(primaryKey) + versionBytes

        let itemData = try DataAccess.serialize(item)
        let timestamp = Date().timeIntervalSince1970
        let value: [UInt8] = {
            var bytes = withUnsafeBytes(of: timestamp.bitPattern) { Array($0) }
            bytes.append(contentsOf: itemData)
            return bytes
        }()

        try await container.engine.withTransaction(configuration: .default) { transaction in
            transaction.setValue(value, for: key)
        }
    }

    private func polymorphicID<T: Persistable & Polymorphable>(
        type: T.Type,
        id: String
    ) -> Tuple {
        Tuple([
            type.typeCode(for: type.persistableType) as any TupleElement,
            id as any TupleElement
        ])
    }
}
#endif

#if SQLITE
import Foundation
import Testing
import Database
import DatabaseRuntime
import TestSupport
import TestHeartbeat

@Persistable
private struct DeepE2EIndexedTicket {
    #Directory<DeepE2EIndexedTicket>("database-framework-deep-e2e", "indexed-tickets")
    #Index(
        .ordered(
            name: "deep_e2e_ticket_status", keys: [.ascending(\DeepE2EIndexedTicket.status)],
            unique: false))
    #Index(
        .ordered(
            name: "deep_e2e_ticket_tenant_status",
            keys: [
                .ascending(\DeepE2EIndexedTicket.tenantID),
                .ascending(\DeepE2EIndexedTicket.status),
            ], unique: false))
    #Index(
        .text(
            name: "deep_e2e_ticket_description", fields: [\DeepE2EIndexedTicket.description],
            mode: .fullText(
                tokenizer: .simple, storePositions: true, ngramSize: 3, minimumTermLength: 2)))
    #Index(
        .bitmap(
            name: "deep_e2e_ticket_status_bitmap",
            field: \DeepE2EIndexedTicket.status
        ))
    #Index(
        .aggregate(
            name: "deep_e2e_ticket_count_by_tenant", function: .count,
            groupBy: [.ascending(\DeepE2EIndexedTicket.tenantID)]))
    #Index(
        .aggregate(
            name: "deep_e2e_ticket_sum_by_tenant", function: .sum,
            groupBy: [.ascending(\DeepE2EIndexedTicket.tenantID)],
            value: \DeepE2EIndexedTicket.amountCents))

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var status: String = ""
    var priority: Int64 = 0
    var amountCents: Int64 = 0
    var description: String = ""
    var payload: String = ""
}

@Persistable
private struct DeepE2ECustomer {
    #Directory<DeepE2ECustomer>("database-framework-deep-e2e", "customers")

    var id: String = UUID().uuidString
    var name: String = ""
}

@Persistable
private struct DeepE2ERelationshipOrder {
    #Directory<DeepE2ERelationshipOrder>("database-framework-deep-e2e", "relationship-orders")

    var id: String = UUID().uuidString
    var total: Double = 0

    @Relationship(deleteRule: .nullify)
    var customer: PersistableReference<DeepE2ECustomer>? = nil
}

@Persistable
private struct DeepE2ESecureTenantDocument: SecurityPolicy {
    #Directory<DeepE2ESecureTenantDocument>(
        "database-framework-deep-e2e",
        \DeepE2ESecureTenantDocument.tenantID,
        "secure-tenant-documents",
        layer: .partition
    )
    #Index(
        .ordered(
            name: "deep_e2e_secure_document_title",
            keys: [.ascending(\DeepE2ESecureTenantDocument.title)], unique: false))

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var ownerID: String = ""
    var title: String = ""
    var body: String = ""

    static func permitsRead(
        of resource: borrowing DeepE2ESecureTenantDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.isAuthenticated
    }

    static func permitsCreate(
        _ newResource: borrowing DeepE2ESecureTenantDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.isAuthenticated
    }

    static func permitsUpdate(
        from resource: borrowing DeepE2ESecureTenantDocument,
        to newResource: borrowing DeepE2ESecureTenantDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsDelete(
        _ resource: borrowing DeepE2ESecureTenantDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
}

private enum DeepE2EError: Error {
    case rollback
}

private func deepE2ETemporarySQLiteContainer(
    for schema: Schema,
    entityRuntimes: [EntityRuntimeRegistration],
    authorizationPolicies: [AuthorizationPolicyHandler] = [],
    security: SecurityConfiguration = .testingDisabled
) async throws -> (DBContainer, URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("database-framework-deep-e2e-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let databasePath = directory.appendingPathComponent("database.sqlite").path
    let container = try await DBContainer.sqlite(
        for: schema,
        path: databasePath,
        runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "database-tests",
                revision: 1
            ),
            entityRuntimes: entityRuntimes,
            authorizationPolicies: authorizationPolicies
        ),
        security: security
    )
    return (container, directory)
}

private func deepE2ERemoveTemporaryDirectory(_ directory: URL) {
    do {
        try FileManager.default.removeItem(at: directory)
    } catch {
        Issue.record("Failed to clean up temporary E2E directory: \(error)")
    }
}

private func deepE2ETicket(
    id: String,
    tenantID: String,
    status: String,
    priority: Int64,
    amountCents: Int64,
    description: String,
    payload: String = ""
) -> DeepE2EIndexedTicket {
    var ticket = DeepE2EIndexedTicket(
        tenantID: tenantID,
        status: status,
        priority: priority,
        amountCents: amountCents,
        description: description,
        payload: payload
    )
    ticket.id = id
    return ticket
}

private func deepE2EAggregates(
    in context: DatabaseContext,
    tenantID: String
) async throws -> (count: Int64, sum: Int64) {
    let results = try await context.aggregate(DeepE2EIndexedTicket.self)
        .groupBy(DeepE2EIndexedTicket.fields.tenantID)
        .count(as: "ticketCount")
        .sum(DeepE2EIndexedTicket.fields.amountCents, as: "amountSum")
        .execute()

    guard let result = results.first(where: { aggregate in
        if case .string(let value) = aggregate.groupKey["tenantID"] {
            return value == tenantID
        }
        return false
    }) else {
        return (0, 0)
    }

    let count: Int64
    if let storedValue = result.aggregates["ticketCount"],
       let fieldValue = storedValue,
       case .int64(let value) = fieldValue {
        count = value
    } else {
        count = 0
    }

    let sum: Int64
    if let storedValue = result.aggregates["amountSum"],
       let fieldValue = storedValue,
       case .int64(let value) = fieldValue {
        sum = value
    } else {
        sum = 0
    }

    return (count, sum)
}

@Suite("DatabaseFramework Deep E2E Tests", .serialized, .heartbeat)
struct DatabaseFrameworkDeepE2ETests {
    @Test("Fusion restricts FullText before limit and ranks the admitted candidates")
    func fusionRestrictsFullTextBeforeLimitAndRanksCandidates() async throws {
        let schema = try Schema(
            entities: [try DeepE2EIndexedTicket.schemaEntity],
            version: .init(1, 0, 0)
        )
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    DeepE2EIndexedTicket.self
                )
            ]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        let context = container.testBaseContext()
        for ticket in [
            deepE2ETicket(
                id: "fusion-open-low",
                tenantID: "fusion",
                status: "open",
                priority: 1,
                amountCents: 10,
                description: "fusion filler filler filler filler"
            ),
            deepE2ETicket(
                id: "fusion-open-high",
                tenantID: "fusion",
                status: "open",
                priority: 9,
                amountCents: 20,
                description: "fusion filler filler filler"
            ),
            deepE2ETicket(
                id: "fusion-closed-global-best",
                tenantID: "fusion",
                status: "closed",
                priority: 100,
                amountCents: 30,
                description: "fusion"
            ),
            deepE2ETicket(
                id: "fusion-repeated-phrase",
                tenantID: "fusion",
                status: "closed",
                priority: 0,
                amountCents: 0,
                description: "to be or not to be"
            ),
            deepE2ETicket(
                id: "fusion-shorter-phrase",
                tenantID: "fusion",
                status: "closed",
                priority: 0,
                amountCents: 0,
                description: "to be or not"
            ),
        ] {
            try context.insert(ticket)
        }
        try await context.save()

        let eligible = try Filter(
            DeepE2EIndexedTicket.fields.status,
            equals: "open"
        )
        let query = FusionQuery<DeepE2EIndexedTicket> {
            eligible
            Search(DeepE2EIndexedTicket.fields.description)
                .terms(["fusion"])
                .limit(2)
            Rank(DeepE2EIndexedTicket.fields.priority)
                .order(.descending)
        }
        .strategy(.weighted([0, 1]))

        let response = try await context.execute(query)

        #expect(response.results.map(\.item.id) == [
            "fusion-open-high",
            "fusion-open-low",
        ])
        #expect(response.results.map(\.score) == [1, 0.5])
        #expect(response.continuation == nil)

        let firstPage = try await context.execute(
            query,
            options: ReadExecutionOptions(pageSize: 1)
        )
        #expect(firstPage.results.map(\.item.id) == [
            response.results[0].item.id,
        ])
        let continuation = try #require(firstPage.continuation)
        let secondPage = try await context.execute(
            query,
            options: ReadExecutionOptions(
                pageSize: 1,
                continuation: continuation
            )
        )
        #expect(secondPage.results.map(\.item.id) == [
            response.results[1].item.id,
        ])
        #expect(secondPage.continuation == nil)

        let unrestrictedAny = FusionQuery<DeepE2EIndexedTicket> {
            Search(DeepE2EIndexedTicket.fields.description)
                .terms(["fusion", "filler"], mode: .any)
                .limit(1_000_000)
        }
        let unrestrictedResponse = try await context.execute(unrestrictedAny)
        #expect(unrestrictedResponse.results.count == 3)
        #expect(Set(unrestrictedResponse.results.map(\.item.id)) == [
            "fusion-open-low",
            "fusion-open-high",
            "fusion-closed-global-best",
        ])

        let repeatedPhrase = FusionQuery<DeepE2EIndexedTicket> {
            Search(DeepE2EIndexedTicket.fields.description)
                .terms(["to be or not to be"], mode: .phrase)
        }
        let phraseResponse = try await context.execute(repeatedPhrase)
        #expect(phraseResponse.results.map(\.item.id) == [
            "fusion-repeated-phrase"
        ])

        let unavailableBitmap = try Bitmap(
            DeepE2EIndexedTicket.fields.status,
            equals: "open"
        )
        let unsupportedQuery = FusionQuery<DeepE2EIndexedTicket> {
            unavailableBitmap
            Search(DeepE2EIndexedTicket.fields.description)
                .terms(["fusion"])
        }
        await #expect {
            _ = try await context.execute(unsupportedQuery)
        } throws: { error in
            error as? FusionExecutionError
                == .indexExecutorNotRegistered(.bitmap)
        }
    }

    @Test("SQLite maintains scalar full-text and aggregation indexes through update delete and rollback")
    func sqliteMaintainsMultipleIndexesThroughUpdateDeleteAndRollback() async throws {
        let schema = try Schema(entities: [try DeepE2EIndexedTicket.schemaEntity], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2EIndexedTicket.self)]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        let context = container.testBaseContext()
        let first = deepE2ETicket(
            id: "deep-ticket-1",
            tenantID: "tenant-a",
            status: "open",
            priority: 1,
            amountCents: 100,
            description: "alpha searchable"
        )
        let second = deepE2ETicket(
            id: "deep-ticket-2",
            tenantID: "tenant-a",
            status: "open",
            priority: 2,
            amountCents: 200,
            description: "beta"
        )
        try context.insert(first)
        try context.insert(second)
        try await context.save()

        let initialOpen = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .orderBy(DeepE2EIndexedTicket.fields.priority)
            .execute()
        let initialSearch = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(DeepE2EIndexedTicket.fields.description)
            .terms(["searchable"])
            .execute()
        let initialAggregates = try await deepE2EAggregates(in: context, tenantID: "tenant-a")

        #expect(initialOpen.map { $0.id } == ["deep-ticket-1", "deep-ticket-2"])
        #expect(initialSearch.map { $0.id } == ["deep-ticket-1"])
        #expect(initialAggregates.count == 2)
        #expect(initialAggregates.sum == 300)

        var replaced = first
        replaced.status = "closed"
        replaced.amountCents = 150
        replaced.description = "alpha archived"
        try context.update(replaced)
        try await context.save()

        let openAfterReplace = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .orderBy(DeepE2EIndexedTicket.fields.priority)
            .execute()
        let closedAfterReplace = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "closed")
            .execute()
        let oldTokenAfterReplace = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(DeepE2EIndexedTicket.fields.description)
            .terms(["searchable"])
            .execute()
        let newTokenAfterReplace = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(DeepE2EIndexedTicket.fields.description)
            .terms(["archived"])
            .execute()
        let aggregatesAfterReplace = try await deepE2EAggregates(in: context, tenantID: "tenant-a")

        #expect(openAfterReplace.map { $0.id } == ["deep-ticket-2"])
        #expect(closedAfterReplace.map { $0.id } == ["deep-ticket-1"])
        #expect(oldTokenAfterReplace.isEmpty)
        #expect(newTokenAfterReplace.map { $0.id } == ["deep-ticket-1"])
        #expect(aggregatesAfterReplace.count == 2)
        #expect(aggregatesAfterReplace.sum == 350)

        do {
            try await context.withTransaction { transaction in
                var rolledBack = second
                rolledBack.status = "closed"
                rolledBack.amountCents = 900
                rolledBack.description = "rollback"
                try await transaction.save(
                    rolledBack,
                    precondition: .exists
                )
                throw DeepE2EError.rollback
            }
            Issue.record("Expected transaction rollback")
        } catch let error as DeepE2EError {
            #expect(error == .rollback)
        }

        let rollbackTokenHits = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(DeepE2EIndexedTicket.fields.description)
            .terms(["rollback"])
            .execute()
        let aggregatesAfterRollback = try await deepE2EAggregates(in: context, tenantID: "tenant-a")
        #expect(rollbackTokenHits.isEmpty)
        #expect(aggregatesAfterRollback.count == 2)
        #expect(aggregatesAfterRollback.sum == 350)

        try context.delete(replaced)
        try await context.save()

        let closedAfterDelete = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "closed")
            .execute()
        let archivedAfterDelete = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(DeepE2EIndexedTicket.fields.description)
            .terms(["archived"])
            .execute()
        let aggregatesAfterDelete = try await deepE2EAggregates(in: context, tenantID: "tenant-a")

        #expect(closedAfterDelete.isEmpty)
        #expect(archivedAfterDelete.isEmpty)
        #expect(aggregatesAfterDelete.count == 1)
        #expect(aggregatesAfterDelete.sum == 200)
    }

    @Test("SQLite cursor pages remain consistent when later rows change between pages")
    func sqliteCursorPagesRemainConsistentWhenLaterRowsChangeBetweenPages() async throws {
        let schema = try Schema(entities: [try DeepE2EIndexedTicket.schemaEntity], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2EIndexedTicket.self)]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        let context = container.testBaseContext()
        for priority in Int64(1)...6 {
            try context.insert(
                deepE2ETicket(
                    id: "cursor-ticket-\(priority)",
                    tenantID: "tenant-cursor",
                    status: "open",
                    priority: priority,
                    amountCents: Int64(priority * 10),
                    description: "cursor page \(priority)"
                )
            )
        }
        try await context.save()

        let cursor = try context.cursor(DeepE2EIndexedTicket.self)
            .orderBy(DeepE2EIndexedTicket.fields.priority)
            .limit(5)
            .batchSize(2)
            .build()

        let firstPage = try await cursor.next()
        #expect(firstPage.items.map { $0.id } == ["cursor-ticket-1", "cursor-ticket-2"])
        #expect(firstPage.hasMore)

        var futureRow = deepE2ETicket(
            id: "cursor-ticket-6",
            tenantID: "tenant-cursor",
            status: "open",
            priority: 6,
            amountCents: 60,
            description: "cursor page 6"
        )
        futureRow.description = "cursor page 6 changed"
        let mutationContext = container.testBaseContext()
        try mutationContext.upsert(futureRow)
        try mutationContext.insert(
            deepE2ETicket(
                id: "cursor-ticket-closed-new",
                tenantID: "tenant-cursor",
                status: "open",
                priority: 99,
                amountCents: 1,
                description: "outside cursor limit"
            )
        )
        try await mutationContext.save()

        let secondPage = try await cursor.next()
        let thirdPage = try await cursor.next()

        #expect(secondPage.items.map { $0.id } == ["cursor-ticket-3", "cursor-ticket-4"])
        #expect(secondPage.hasMore)
        #expect(thirdPage.items.map { $0.id } == ["cursor-ticket-5"])
        #expect(!thirdPage.hasMore)
    }

    @Test("SQLite relationship updates maintain canonical references and nullify on delete")
    func sqliteRelationshipUpdatesMaintainCanonicalReferences() async throws {
        let schema = try Schema(
            entities: [
                try DeepE2ECustomer.schemaEntity,
                try DeepE2ERelationshipOrder.schemaEntity,
            ],
            version: .init(1, 0, 0)
        )
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2ECustomer.self), try DatabaseFrameworkRuntime.entity(DeepE2ERelationshipOrder.self),
            ]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        var alice = DeepE2ECustomer(name: "Alice")
        alice.id = "deep-customer-alice"
        var bob = DeepE2ECustomer(name: "Bob")
        bob.id = "deep-customer-bob"
        let context = container.testBaseContext()
        var order = DeepE2ERelationshipOrder(
            total: 125,
            customer: try context.reference(to: alice)
        )
        order.id = "deep-order-1"
        try context.insert(alice)
        try context.insert(bob)
        try context.insert(order)
        try await context.save()

        let initialRelated = try await context.related(order, DeepE2ERelationshipOrder.fields.customer)
        #expect(initialRelated?.id == alice.id)

        var movedOrder = order
        movedOrder.customer = try context.reference(to: bob)
        try context.update(movedOrder)
        try await context.save()

        let inverse = context.inverseRelationshipResolver()
        let oldCustomerOrders = try await inverse.referencedBy(
            try context.reference(to: alice),
            from: DeepE2ERelationshipOrder.self,
            via: DeepE2ERelationshipOrder.fields.customer,
            limit: 10
        )
        let newCustomerOrders = try await inverse.referencedBy(
            try context.reference(to: bob),
            from: DeepE2ERelationshipOrder.self,
            via: DeepE2ERelationshipOrder.fields.customer,
            limit: 10
        )
        let movedRelated = try await context.related(movedOrder, DeepE2ERelationshipOrder.fields.customer)

        #expect(oldCustomerOrders.entities.isEmpty)
        #expect(newCustomerOrders.entities.map { $0.id } == [order.id])
        #expect(movedRelated?.id == bob.id)

        try context.delete(bob)
        try await context.save()

        let nullifiedOrder = try await context.model(
            for: order.id,
            as: DeepE2ERelationshipOrder.self
        )
        #expect(nullifiedOrder?.customer == nil)

        try context.delete(movedOrder)
        try await context.save()

        #expect(
            try await context.model(
                for: order.id,
                as: DeepE2ERelationshipOrder.self
            ) == nil
        )
    }

    @Test("SQLite secure dynamic directory enforces stored-owner security during tenant moves")
    func sqliteSecureDynamicDirectoryEnforcesStoredOwnerSecurityDuringTenantMoves() async throws {
        let schema = try Schema(entities: [try DeepE2ESecureTenantDocument.schemaEntity], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2ESecureTenantDocument.self)],
            authorizationPolicies: [
                AuthorizationPolicyHandler(DeepE2ESecureTenantDocument.self)
            ],
            security: .enabled()
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }
        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal("alice"),
            access: [.read, .write]
        )
        try await container.grantTestBaseAccess(
            to: .principal("bob"),
            access: [.read, .write]
        )
        #endif
        let aliceAuthorization = AuthorizationContext.authenticated(
            Principal(identifier: "alice")
        )
        let bobAuthorization = AuthorizationContext.authenticated(
            Principal(identifier: "bob")
        )

        var original = DeepE2ESecureTenantDocument()
        original.id = "deep-secure-document"
        original.tenantID = "tenant-secure-a"
        original.ownerID = "alice"
        original.title = "Original"
        original.body = "body"

        let createContext = container.testBaseContext(
            authorization: aliceAuthorization
        )
        try createContext.insert(original)
        try await createContext.save()

        var moved = original
        moved.tenantID = "tenant-secure-b"
        moved.ownerID = "bob"
        moved.title = "Moved"
        let updateContext = container.testBaseContext(
            authorization: aliceAuthorization
        )
        try updateContext.delete(original, precondition: .exists)
        try updateContext.insert(moved, precondition: .notExists)
        try await updateContext.save()

        do {
            var denied = moved
            denied.title = "Denied"
            let deniedContext = container.testBaseContext(
                authorization: aliceAuthorization
            )
            try deniedContext.update(denied)
            try await deniedContext.save()
            Issue.record("Expected stale-owner update to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .update)
            #expect(error.userID == "alice")
        }

        do {
            let deniedContext = container.testBaseContext(
                authorization: aliceAuthorization
            )
            try deniedContext.delete(moved)
            try await deniedContext.save()
            Issue.record("Expected stale-owner delete to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .delete)
            #expect(error.resource?.id == .string(original.id))
            #expect(error.userID == "alice")
        }

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: directory.appendingPathComponent("database.sqlite").path,
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2ESecureTenantDocument.self)]
            ),
            security: .testingDisabled
        )
        defer {
            await verificationContainer.shutdown()
        }
        let oldPartition = try await verificationContainer.testBaseContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(DeepE2ESecureTenantDocument.fields.tenantID, equals: "tenant-secure-a")
            .execute()
        let newPartition = try await verificationContainer.testBaseContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(DeepE2ESecureTenantDocument.fields.tenantID, equals: "tenant-secure-b")
            .where(DeepE2ESecureTenantDocument.fields.title == "Moved")
            .execute()
        let deniedTitle = try await verificationContainer.testBaseContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(DeepE2ESecureTenantDocument.fields.tenantID, equals: "tenant-secure-b")
            .where(DeepE2ESecureTenantDocument.fields.title == "Denied")
            .execute()

        #expect(oldPartition.isEmpty)
        #expect(newPartition.map { $0.id } == [original.id])
        #expect(newPartition.first?.ownerID == "bob")
        #expect(deniedTitle.isEmpty)

        let deleteContext = container.testBaseContext(
            authorization: bobAuthorization
        )
        try deleteContext.delete(moved)
        try await deleteContext.save()

        let afterBobDelete = try await verificationContainer.testBaseContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(DeepE2ESecureTenantDocument.fields.tenantID, equals: "tenant-secure-b")
            .where(DeepE2ESecureTenantDocument.fields.title == "Moved")
            .execute()
        #expect(afterBobDelete.isEmpty)
    }

    @Test("SQLite public write APIs produce equivalent persisted rows and index cleanup")
    func sqlitePublicWriteAPIsProduceEquivalentPersistedRowsAndIndexCleanup() async throws {
        let schema = try Schema(entities: [try DeepE2EIndexedTicket.schemaEntity], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2EIndexedTicket.self)]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        let contextInsert = container.testBaseContext()
        try contextInsert.insert(
            deepE2ETicket(
                id: "api-ticket-insert",
                tenantID: "tenant-api",
                status: "open",
                priority: 1,
                amountCents: 10,
                description: "api insert"
            )
        )
        try await contextInsert.save()

        try await container.testBaseContext().withTransaction { transaction in
            try await transaction.save(
                deepE2ETicket(
                    id: "api-ticket-transaction",
                    tenantID: "tenant-api",
                    status: "open",
                    priority: 2,
                    amountCents: 20,
                    description: "api transaction"
                ),
                precondition: .notExists
            )
        }

        let contextCreate = container.testBaseContext()
        try contextCreate.insert(
            deepE2ETicket(
                id: "api-ticket-create",
                tenantID: "tenant-api",
                status: "open",
                priority: 3,
                amountCents: 30,
                description: "api create"
            )
        )
        try await contextCreate.save()

        let openBeforeDelete = try await container.testBaseContext()
            .fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .orderBy(DeepE2EIndexedTicket.fields.priority)
            .execute()
        let aggregatesBeforeDelete = try await deepE2EAggregates(
            in: container.testBaseContext(),
            tenantID: "tenant-api"
        )

        #expect(openBeforeDelete.map { $0.id } == [
            "api-ticket-insert",
            "api-ticket-transaction",
            "api-ticket-create",
        ])
        #expect(aggregatesBeforeDelete.count == 3)
        #expect(aggregatesBeforeDelete.sum == 60)

        let deleteByContext = container.testBaseContext()
        try deleteByContext.delete(openBeforeDelete[0])
        try await deleteByContext.save()

        let transactionDeleteModel = openBeforeDelete[1]
        try await container.testBaseContext().withTransaction { transaction in
            try await transaction.delete(transactionDeleteModel)
        }

        let deleteByID = container.testBaseContext()
        try await deleteByID.delete(DeepE2EIndexedTicket.self, where: DeepE2EIndexedTicket.fields.id == openBeforeDelete[2].id)
        try await deleteByID.save()

        let openAfterDelete = try await container.testBaseContext()
            .fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .execute()
        let aggregatesAfterDelete = try await deepE2EAggregates(
            in: container.testBaseContext(),
            tenantID: "tenant-api"
        )

        #expect(openAfterDelete.isEmpty)
        #expect(aggregatesAfterDelete.count == 0)
        #expect(aggregatesAfterDelete.sum == 0)
    }

    @Test("SQLite complex query applies indexed predicates residual filters sorting and limits")
    func sqliteComplexQueryAppliesIndexedPredicatesResidualFiltersSortingAndLimits() async throws {
        let schema = try Schema(entities: [try DeepE2EIndexedTicket.schemaEntity], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(DeepE2EIndexedTicket.self)]
        )
        defer {
            await container.shutdown()
            deepE2ERemoveTemporaryDirectory(directory)
        }

        let context = container.testBaseContext()
        let tickets = [
            deepE2ETicket(id: "query-1", tenantID: "tenant-query", status: "open", priority: 1, amountCents: 25, description: "query low"),
            deepE2ETicket(id: "query-2", tenantID: "tenant-query", status: "open", priority: 2, amountCents: 200, description: "query high"),
            deepE2ETicket(id: "query-3", tenantID: "tenant-query", status: "open", priority: 3, amountCents: 150, description: "query middle"),
            deepE2ETicket(id: "query-4", tenantID: "tenant-query", status: "closed", priority: 4, amountCents: 300, description: "query closed"),
            deepE2ETicket(id: "query-5", tenantID: "other-tenant", status: "open", priority: 5, amountCents: 500, description: "query other"),
        ]
        for ticket in tickets {
            try context.insert(ticket)
        }
        try await context.save()

        let results = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.tenantID == "tenant-query")
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .orderBy(DeepE2EIndexedTicket.fields.priority, .descending)
            .execute()

        #expect(results.map { $0.id } == ["query-3", "query-2", "query-1"])
        #expect(results.allSatisfy { $0.status == "open" && $0.tenantID == "tenant-query" })

        var updated = tickets[1]
        updated.status = "closed"
        updated.amountCents = 50
        try context.update(updated)
        try await context.save()

        let afterUpdate = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(DeepE2EIndexedTicket.fields.tenantID == "tenant-query")
            .where(DeepE2EIndexedTicket.fields.status == "open")
            .orderBy(DeepE2EIndexedTicket.fields.priority, .descending)
            .execute()

        #expect(afterUpdate.map { $0.id } == ["query-3", "query-1"])
    }
}
#endif

#if SQLITE
import Foundation
import Testing
import Database
import TestHeartbeat

@Persistable
private struct DeepE2EIndexedTicket {
    #Directory<DeepE2EIndexedTicket>("database-framework-deep-e2e", "indexed-tickets")
    #Index(
        ScalarIndexKind<DeepE2EIndexedTicket>(fields: [\.status]),
        name: "deep_e2e_ticket_status"
    )
    #Index(
        ScalarIndexKind<DeepE2EIndexedTicket>(fields: [\.tenantID, \.status]),
        name: "deep_e2e_ticket_tenant_status"
    )
    #Index(
        FullTextIndexKind<DeepE2EIndexedTicket>(fields: [\.description], tokenizer: .simple),
        name: "deep_e2e_ticket_description"
    )
    #Index(
        CountIndexKind<DeepE2EIndexedTicket>(groupBy: [\.tenantID]),
        name: "deep_e2e_ticket_count_by_tenant"
    )
    #Index(
        SumIndexKind<DeepE2EIndexedTicket, Int64>(groupBy: [\.tenantID], value: \.amountCents),
        name: "deep_e2e_ticket_sum_by_tenant"
    )

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var status: String = ""
    var priority: Int = 0
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

    @Relationship(DeepE2ECustomer.self)
    var customerID: String = ""
}

@Persistable
private struct DeepE2ESecureTenantDocument: SecurityPolicy {
    #Directory<DeepE2ESecureTenantDocument>(
        "database-framework-deep-e2e",
        Field<DeepE2ESecureTenantDocument>(\.tenantID),
        "secure-tenant-documents",
        layer: .partition
    )
    #Index(
        ScalarIndexKind<DeepE2ESecureTenantDocument>(fields: [\.title]),
        name: "deep_e2e_secure_document_title"
    )

    var id: String = UUID().uuidString
    var tenantID: String = ""
    var ownerID: String = ""
    var title: String = ""
    var body: String = ""

    static func allowGet(
        resource: DeepE2ESecureTenantDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowList(
        query: SecurityQuery<DeepE2ESecureTenantDocument>,
        auth: (any AuthContext)?
    ) -> Bool {
        auth != nil
    }

    static func allowCreate(
        newResource: DeepE2ESecureTenantDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        auth != nil
    }

    static func allowUpdate(
        resource: DeepE2ESecureTenantDocument,
        newResource: DeepE2ESecureTenantDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }

    static func allowDelete(
        resource: DeepE2ESecureTenantDocument,
        auth: (any AuthContext)?
    ) -> Bool {
        resource.ownerID == auth?.userID
    }
}

private struct DeepE2EAuth: AuthContext {
    let userID: String
    var roles: Set<String> = []
}

private enum DeepE2EError: Error {
    case rollback
}

private func deepE2ETemporarySQLiteContainer(
    for schema: Schema,
    security: SecurityConfiguration = .disabled
) async throws -> (DBContainer, URL) {
    let directory = FileManager.default.temporaryDirectory
        .appendingPathComponent("database-framework-deep-e2e-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    let databasePath = directory.appendingPathComponent("database.sqlite").path
    let container = try await DBContainer.sqlite(
        for: schema,
        path: databasePath,
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
    priority: Int,
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
    in context: FDBContext,
    tenantID: String
) async throws -> (count: Int64, sum: Double) {
    let results = try await context.aggregate(DeepE2EIndexedTicket.self)
        .groupBy(\.tenantID)
        .count(as: "ticketCount")
        .sum(\.amountCents, as: "amountSum")
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
    if let fieldValue = result.aggregates["ticketCount"], case .int64(let value) = fieldValue {
        count = value
    } else {
        count = 0
    }

    let sum: Double
    if let fieldValue = result.aggregates["amountSum"], case .double(let value) = fieldValue {
        sum = value
    } else {
        sum = 0
    }

    return (count, sum)
}

@Suite("DatabaseFramework Deep E2E Tests", .serialized, .heartbeat)
struct DatabaseFrameworkDeepE2ETests {
    @Test("SQLite maintains scalar full-text and aggregation indexes through replace delete and rollback")
    func sqliteMaintainsMultipleIndexesThroughReplaceDeleteAndRollback() async throws {
        let schema = Schema([DeepE2EIndexedTicket.self], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(for: schema)
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        let context = container.newContext()
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
        context.insert(first)
        context.insert(second)
        try await context.save()

        let initialOpen = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "open")
            .orderBy(\.priority)
            .execute()
        let initialSearch = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(\.description)
            .terms(["searchable"])
            .execute()
        let initialAggregates = try await deepE2EAggregates(in: context, tenantID: "tenant-a")

        #expect(initialOpen.map(\.id) == ["deep-ticket-1", "deep-ticket-2"])
        #expect(initialSearch.map(\.id) == ["deep-ticket-1"])
        #expect(initialAggregates.count == 2)
        #expect(initialAggregates.sum == 300)

        var replaced = first
        replaced.status = "closed"
        replaced.amountCents = 150
        replaced.description = "alpha archived"
        context.replace(old: first, with: replaced)
        try await context.save()

        let openAfterReplace = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "open")
            .orderBy(\.priority)
            .execute()
        let closedAfterReplace = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "closed")
            .execute()
        let oldTokenAfterReplace = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(\.description)
            .terms(["searchable"])
            .execute()
        let newTokenAfterReplace = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(\.description)
            .terms(["archived"])
            .execute()
        let aggregatesAfterReplace = try await deepE2EAggregates(in: context, tenantID: "tenant-a")

        #expect(openAfterReplace.map(\.id) == ["deep-ticket-2"])
        #expect(closedAfterReplace.map(\.id) == ["deep-ticket-1"])
        #expect(oldTokenAfterReplace.isEmpty)
        #expect(newTokenAfterReplace.map(\.id) == ["deep-ticket-1"])
        #expect(aggregatesAfterReplace.count == 2)
        #expect(aggregatesAfterReplace.sum == 350)

        do {
            try await context.withTransaction { transaction in
                var rolledBack = second
                rolledBack.status = "closed"
                rolledBack.amountCents = 900
                rolledBack.description = "rollback"
                try await transaction.set(rolledBack)
                throw DeepE2EError.rollback
            }
            Issue.record("Expected transaction rollback")
        } catch let error as DeepE2EError {
            #expect(error == .rollback)
        }

        let rollbackTokenHits = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(\.description)
            .terms(["rollback"])
            .execute()
        let aggregatesAfterRollback = try await deepE2EAggregates(in: context, tenantID: "tenant-a")
        #expect(rollbackTokenHits.isEmpty)
        #expect(aggregatesAfterRollback.count == 2)
        #expect(aggregatesAfterRollback.sum == 350)

        context.delete(replaced)
        try await context.save()

        let closedAfterDelete = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "closed")
            .execute()
        let archivedAfterDelete = try await context.search(DeepE2EIndexedTicket.self)
            .fullText(\.description)
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
        let schema = Schema([DeepE2EIndexedTicket.self], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(for: schema)
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        let context = container.newContext()
        for priority in 1...6 {
            context.insert(
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
            .orderBy(\.priority)
            .limit(5)
            .batchSize(2)
            .build()

        let firstPage = try await cursor.next()
        #expect(firstPage.items.map(\.id) == ["cursor-ticket-1", "cursor-ticket-2"])
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
        let mutationContext = container.newContext()
        mutationContext.upsert(futureRow)
        mutationContext.insert(
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

        #expect(secondPage.items.map(\.id) == ["cursor-ticket-3", "cursor-ticket-4"])
        #expect(secondPage.hasMore)
        #expect(thirdPage.items.map(\.id) == ["cursor-ticket-5"])
        #expect(!thirdPage.hasMore)
    }

    @Test("SQLite relationship updates tolerate orphan references and clear FK indexes on delete")
    func sqliteRelationshipUpdatesTolerateOrphansAndClearFKIndexesOnDelete() async throws {
        let schema = Schema(
            [DeepE2ECustomer.self, DeepE2ERelationshipOrder.self],
            version: .init(1, 0, 0)
        )
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(for: schema)
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        var alice = DeepE2ECustomer(name: "Alice")
        alice.id = "deep-customer-alice"
        var bob = DeepE2ECustomer(name: "Bob")
        bob.id = "deep-customer-bob"
        var order = DeepE2ERelationshipOrder(total: 125, customerID: alice.id)
        order.id = "deep-order-1"

        let context = container.newContext()
        context.insert(alice)
        context.insert(bob)
        context.insert(order)
        try await context.save()

        let initialRelated = try await context.related(order, \.customerID, as: DeepE2ECustomer.self)
        #expect(initialRelated?.id == alice.id)

        var movedOrder = order
        movedOrder.customerID = bob.id
        context.replace(old: order, with: movedOrder)
        try await context.save()

        let oldCustomerOrders = try await context.fetch(DeepE2ERelationshipOrder.self)
            .where(\.customerID == alice.id)
            .execute()
        let newCustomerOrders = try await context.fetch(DeepE2ERelationshipOrder.self)
            .where(\.customerID == bob.id)
            .execute()
        let movedRelated = try await context.related(movedOrder, \.customerID, as: DeepE2ECustomer.self)

        #expect(oldCustomerOrders.isEmpty)
        #expect(newCustomerOrders.map(\.id) == [order.id])
        #expect(movedRelated?.id == bob.id)

        context.delete(bob)
        try await context.save()

        let orphanRelated = try await context.related(movedOrder, \.customerID, as: DeepE2ECustomer.self)
        let orphanOrderHits = try await context.fetch(DeepE2ERelationshipOrder.self)
            .where(\.customerID == bob.id)
            .execute()
        #expect(orphanRelated == nil)
        #expect(orphanOrderHits.map(\.id) == [order.id])

        context.delete(movedOrder)
        try await context.save()

        let afterOrderDelete = try await context.fetch(DeepE2ERelationshipOrder.self)
            .where(\.customerID == bob.id)
            .execute()
        #expect(afterOrderDelete.isEmpty)
    }

    @Test("SQLite secure dynamic directory enforces stored-owner security during tenant moves")
    func sqliteSecureDynamicDirectoryEnforcesStoredOwnerSecurityDuringTenantMoves() async throws {
        let schema = Schema([DeepE2ESecureTenantDocument.self], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(
            for: schema,
            security: .enabled()
        )
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        var original = DeepE2ESecureTenantDocument()
        original.id = "deep-secure-document"
        original.tenantID = "tenant-secure-a"
        original.ownerID = "alice"
        original.title = "Original"
        original.body = "body"

        try await AuthContextKey.$current.withValue(DeepE2EAuth(userID: "alice")) {
            let createContext = container.newContext()
            createContext.insert(original)
            try await createContext.save()
        }

        var moved = original
        moved.tenantID = "tenant-secure-b"
        moved.ownerID = "bob"
        moved.title = "Moved"
        try await AuthContextKey.$current.withValue(DeepE2EAuth(userID: "alice")) {
            let updateContext = container.newContext()
            updateContext.replace(old: original, with: moved)
            try await updateContext.save()
        }

        do {
            try await AuthContextKey.$current.withValue(DeepE2EAuth(userID: "alice")) {
                var denied = moved
                denied.title = "Denied"
                let deniedContext = container.newContext()
                deniedContext.replace(old: moved, with: denied)
                try await deniedContext.save()
            }
            Issue.record("Expected stale-owner update to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .update)
            #expect(error.userID == "alice")
        }

        do {
            try await AuthContextKey.$current.withValue(DeepE2EAuth(userID: "alice")) {
                let deniedContext = container.newContext()
                deniedContext.delete(moved)
                try await deniedContext.save()
            }
            Issue.record("Expected stale-owner delete to be denied")
        } catch let error as SecurityError {
            #expect(error.operation == .delete)
            #expect(error.resourceID == original.id)
            #expect(error.userID == "alice")
        }

        let verificationContainer = try await DBContainer.sqlite(
            for: schema,
            path: directory.appendingPathComponent("database.sqlite").path,
            security: .disabled
        )
        let oldPartition = try await verificationContainer.newContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(\.tenantID, equals: "tenant-secure-a")
            .execute()
        let newPartition = try await verificationContainer.newContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(\.tenantID, equals: "tenant-secure-b")
            .where(\.title == "Moved")
            .execute()
        let deniedTitle = try await verificationContainer.newContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(\.tenantID, equals: "tenant-secure-b")
            .where(\.title == "Denied")
            .execute()

        #expect(oldPartition.isEmpty)
        #expect(newPartition.map(\.id) == [original.id])
        #expect(newPartition.first?.ownerID == "bob")
        #expect(deniedTitle.isEmpty)

        try await AuthContextKey.$current.withValue(DeepE2EAuth(userID: "bob")) {
            let deleteContext = container.newContext()
            deleteContext.delete(moved)
            try await deleteContext.save()
        }

        let afterBobDelete = try await verificationContainer.newContext()
            .fetch(DeepE2ESecureTenantDocument.self)
            .partition(\.tenantID, equals: "tenant-secure-b")
            .where(\.title == "Moved")
            .execute()
        #expect(afterBobDelete.isEmpty)
    }

    @Test("SQLite public write APIs produce equivalent persisted rows and index cleanup")
    func sqlitePublicWriteAPIsProduceEquivalentPersistedRowsAndIndexCleanup() async throws {
        let schema = Schema([DeepE2EIndexedTicket.self], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(for: schema)
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        let contextInsert = container.newContext()
        contextInsert.insert(
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

        try await container.newContext().withTransaction { transaction in
            try await transaction.set(
                deepE2ETicket(
                    id: "api-ticket-transaction",
                    tenantID: "tenant-api",
                    status: "open",
                    priority: 2,
                    amountCents: 20,
                    description: "api transaction"
                )
            )
        }

        let contextCreate = container.newContext()
        contextCreate.create(
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

        let openBeforeDelete = try await container.newContext()
            .fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "open")
            .orderBy(\.priority)
            .execute()
        let aggregatesBeforeDelete = try await deepE2EAggregates(
            in: container.newContext(),
            tenantID: "tenant-api"
        )

        #expect(openBeforeDelete.map(\.id) == [
            "api-ticket-insert",
            "api-ticket-transaction",
            "api-ticket-create",
        ])
        #expect(aggregatesBeforeDelete.count == 3)
        #expect(aggregatesBeforeDelete.sum == 60)

        let deleteByContext = container.newContext()
        deleteByContext.delete(openBeforeDelete[0])
        try await deleteByContext.save()

        let transactionDeleteModel = openBeforeDelete[1]
        try await container.newContext().withTransaction { transaction in
            try await transaction.delete(transactionDeleteModel)
        }

        let deleteByID = container.newContext()
        try await deleteByID.delete(DeepE2EIndexedTicket.self, where: \.id == openBeforeDelete[2].id)
        try await deleteByID.save()

        let openAfterDelete = try await container.newContext()
            .fetch(DeepE2EIndexedTicket.self)
            .where(\.status == "open")
            .execute()
        let aggregatesAfterDelete = try await deepE2EAggregates(
            in: container.newContext(),
            tenantID: "tenant-api"
        )

        #expect(openAfterDelete.isEmpty)
        #expect(aggregatesAfterDelete.count == 0)
        #expect(aggregatesAfterDelete.sum == 0)
    }

    @Test("SQLite complex query applies indexed predicates residual filters sorting and limits")
    func sqliteComplexQueryAppliesIndexedPredicatesResidualFiltersSortingAndLimits() async throws {
        let schema = Schema([DeepE2EIndexedTicket.self], version: .init(1, 0, 0))
        let (container, directory) = try await deepE2ETemporarySQLiteContainer(for: schema)
        defer { deepE2ERemoveTemporaryDirectory(directory) }

        let context = container.newContext()
        let tickets = [
            deepE2ETicket(id: "query-1", tenantID: "tenant-query", status: "open", priority: 1, amountCents: 25, description: "query low"),
            deepE2ETicket(id: "query-2", tenantID: "tenant-query", status: "open", priority: 2, amountCents: 200, description: "query high"),
            deepE2ETicket(id: "query-3", tenantID: "tenant-query", status: "open", priority: 3, amountCents: 150, description: "query middle"),
            deepE2ETicket(id: "query-4", tenantID: "tenant-query", status: "closed", priority: 4, amountCents: 300, description: "query closed"),
            deepE2ETicket(id: "query-5", tenantID: "other-tenant", status: "open", priority: 5, amountCents: 500, description: "query other"),
        ]
        for ticket in tickets {
            context.insert(ticket)
        }
        try await context.save()

        let results = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.tenantID == "tenant-query")
            .where(\.status == "open")
            .orderBy(\.priority, .descending)
            .execute()

        #expect(results.map(\.id) == ["query-3", "query-2", "query-1"])
        #expect(results.allSatisfy { $0.status == "open" && $0.tenantID == "tenant-query" })

        var updated = tickets[1]
        updated.status = "closed"
        updated.amountCents = 50
        context.replace(old: tickets[1], with: updated)
        try await context.save()

        let afterUpdate = try await context.fetch(DeepE2EIndexedTicket.self)
            .where(\.tenantID == "tenant-query")
            .where(\.status == "open")
            .orderBy(\.priority, .descending)
            .execute()

        #expect(afterUpdate.map(\.id) == ["query-3", "query-1"])
    }
}
#endif

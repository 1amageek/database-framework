#if POSTGRESQL
// PostgreSQLPointReadTests.swift
// Point read behavior tests against PostgreSQL backend

import Testing
import Foundation
import StorageKit
import PostgreSQLStorage
@testable import DatabaseEngine
@testable import DatabaseKit
import TestSupport
import DatabaseRuntime

@Persistable
private struct PGPointReadItem: Equatable {
    #Directory<PGPointReadItem>("test", "pg", "point-read")

    var id: String = UUID().uuidString
    var name: String = ""
    var value: Int64 = 0
}

@Persistable
private struct PGSecuredPointReadItem: Equatable, SecurityPolicy {
    #Directory<PGSecuredPointReadItem>("test", "pg", "secured-point-read")

    var id: String = UUID().uuidString
    var ownerID: String = ""
    var name: String = ""

    static func permitsRead(
        of resource: borrowing PGSecuredPointReadItem,
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
        _ newResource: borrowing PGSecuredPointReadItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        newResource.ownerID == context.principal?.identifier
    }

    static func permitsUpdate(
        from resource: borrowing PGSecuredPointReadItem,
        to newResource: borrowing PGSecuredPointReadItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
            && newResource.ownerID == context.principal?.identifier
    }

    static func permitsDelete(
        _ resource: borrowing PGSecuredPointReadItem,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
}

@Suite("PostgreSQL Point Read Tests", .serialized, .heartbeat, .enabled(if: PostgreSQLScenarioCoordinator.isConfigured))
struct PostgreSQLPointReadTests {

    private func uniqueID(_ prefix: String) -> String {
        "\(prefix)-\(UUID().uuidString.prefix(8))"
    }

    private func setupStaticContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try PGPointReadItem.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGPointReadItem.self)])
    }

    private func setupPartitionedContainer() async throws -> DBContainer {
        let schema = try Schema(entities: [try TenantOrder.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await PostgreSQLScenarioCoordinator.shared.makeContainer(schema: schema, entityRuntimes: [try DatabaseFrameworkRuntime.entity(TenantOrder.self)])
    }

    private func setupSecuredContainer() async throws -> DBContainer {
        let engine = try await PostgreSQLScenarioCoordinator.shared.engine
        let schema = try Schema(entities: [try PGSecuredPointReadItem.schemaEntity], version: Schema.Version(1, 0, 0))
        return try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [try DatabaseFrameworkRuntime.entity(PGPointReadItem.self), try DatabaseFrameworkRuntime.entity(PGSecuredPointReadItem.self), try DatabaseFrameworkRuntime.entity(TenantOrder.self)],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(PGSecuredPointReadItem.self)
                ]
            ),
            security: .enabled()
        )
    }

    @Test("DataStore.fetch(id:) returns an item and nil for a missing key")
    func staticPointRead() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupStaticContainer()
            let context = container.newContext()

            let itemID = uniqueID("point")
            var item = PGPointReadItem()
            item.id = itemID
            item.name = "stored"
            item.value = 42
            try context.insert(item)
            try await context.save()

            let store = try await container.store(for: PGPointReadItem.self)
            let fetched = try await store.fetch(PGPointReadItem.self, id: itemID)
            let missing = try await store.fetch(PGPointReadItem.self, id: uniqueID("missing"))

            #expect(fetched?.id == itemID)
            #expect(fetched?.name == "stored")
            #expect(fetched?.value == 42)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.executeBatch single-item path preserves upsert and delete semantics")
    func dataStoreSingleItemExecuteBatch() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupStaticContainer()
            let store = try await container.store(for: PGPointReadItem.self)

            let itemID = uniqueID("batch")
            var item = PGPointReadItem()
            item.id = itemID
            item.name = "created"
            item.value = 1
            try await store.executeBatch(
                inserts: [try PersistedModel(item)],
                deletes: []
            )

            var updated = item
            updated.name = "updated"
            updated.value = 2
            try await store.executeBatch(
                inserts: [try PersistedModel(updated)],
                deletes: []
            )

            let fetched = try await store.fetch(PGPointReadItem.self, id: itemID)
            #expect(fetched?.name == "updated")
            #expect(fetched?.value == 2)

            try await store.executeBatch(
                inserts: [],
                deletes: [try PersistedModel(updated)]
            )
            let missing = try await store.fetch(PGPointReadItem.self, id: itemID)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.fetch(id:) respects resolved partition path")
    func partitionedPointRead() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupPartitionedContainer()
            let context = container.newContext()

            let tenantID = uniqueID("tenant")
            let orderID = uniqueID("order")
            var order = TenantOrder(tenantID: tenantID, status: "pending", total: 55.0)
            order.id = orderID
            try context.insert(order)
            try await context.save()

            var path = DirectoryPath<TenantOrder>()
            path.set(TenantOrder.fields.tenantID, to: tenantID)
            let store = try await container.store(for: TenantOrder.self, path: path)
            let fetched = try await store.fetch(TenantOrder.self, id: orderID)

            var wrongPath = DirectoryPath<TenantOrder>()
            wrongPath.set(
                TenantOrder.fields.tenantID,
                to: uniqueID("other")
            )
            let wrongStore = try await container.store(for: TenantOrder.self, path: wrongPath)
            let missing = try await wrongStore.fetch(TenantOrder.self, id: orderID)

            #expect(fetched?.id == orderID)
            #expect(fetched?.tenantID == tenantID)
            #expect(missing == nil)
        }
    }

    @Test("DataStore.fetch(id:) preserves GET security checks on point-read fast path")
    func securedPointRead() async throws {
        try await PostgreSQLScenarioCoordinator.shared.withIsolatedScenario {
            let container = try await setupSecuredContainer()
            let itemID = uniqueID("secure")

            let owner = AuthorizationContext.authenticated(
                Principal(identifier: "owner")
            )
            try await RequestAuthorization.$context.withValue(owner) {
                let context = container.newContext()
                var item = PGSecuredPointReadItem()
                item.id = itemID
                item.ownerID = "owner"
                item.name = "secret"
                try context.insert(item)
                try await context.save()
            }

            let store = try await container.store(for: PGSecuredPointReadItem.self)

            let authorized = try await RequestAuthorization.$context.withValue(owner) {
                try await store.fetch(PGSecuredPointReadItem.self, id: itemID)
            }
            #expect(authorized?.id == itemID)

            await #expect(throws: SecurityError.self) {
                let intruder = AuthorizationContext.authenticated(
                    Principal(identifier: "intruder")
                )
                try await RequestAuthorization.$context.withValue(intruder) {
                    _ = try await store.fetch(PGSecuredPointReadItem.self, id: itemID)
                }
            }
        }
    }
}
#endif

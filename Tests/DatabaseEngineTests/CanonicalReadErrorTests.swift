#if !os(WASI)
import Testing
import Foundation
import Core
import DatabaseValue
import QueryIR
import TestSupport
@testable import DatabaseEngine

/// Contract tests for `CanonicalReadError`.
///
/// Focus: canonical-read surfaces are required to fail explicitly rather than
/// silently degrade. These tests lock in the throwing behavior at the pure-unit
/// boundary (no FDB required) so that a future refactor cannot regress the
/// contract.
@Suite("CanonicalReadError Contract Tests")
struct CanonicalReadErrorTests {

    // MARK: - Helper Types

    @Persistable
    struct StaticModel {
        #Directory<StaticModel>("test", "canonical", "static")
        var id: String = ""
        var name: String = ""
    }

    // MARK: - CanonicalPartitionBinding

    @Test("Unknown partition field is rejected")
    func invalidPartitionFieldRejected() {
        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalPartitionBinding.makeBinding(
                for: TenantOrder.self,
                partitions: [
                    DatabaseObjectField(
                        number: 99,
                        name: "nonexistentField",
                        value: .string("value")
                    ),
                ]
            )
        }
    }

    @Test("Valid partition field succeeds")
    func validPartitionFieldAccepted() throws {
        let field = try #require(
            TenantOrder.fieldSchemas.first { $0.name == "tenantID" }
        )
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: TenantOrder.self,
            partitions: [
                DatabaseObjectField(
                    number: UInt32(field.fieldNumber),
                    name: field.name,
                    value: .string("tenant-1")
                ),
            ]
        )
        #expect(binding != nil)
    }

    @Test("Partition value must match the compiled field type")
    func invalidPartitionTypeRejected() throws {
        let field = try #require(
            TenantOrder.fieldSchemas.first { $0.name == "tenantID" }
        )
        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalPartitionBinding.makeBinding(
                for: TenantOrder.self,
                partitions: [
                    DatabaseObjectField(
                        number: UInt32(field.fieldNumber),
                        name: field.name,
                        value: .int64(1)
                    ),
                ]
            )
        }
    }

    @Test("Static-directory model with an empty partition returns nil binding")
    func staticDirectoryWithEmptyBinding() throws {
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: StaticModel.self,
            partitions: []
        )
        #expect(binding == nil)
    }

    // MARK: - ReadExecutorRegistry

    @Test("Unknown index kind returns nil executor")
    func unknownIndexKindReturnsNilExecutor() throws {
        let registry = try ReadExecutorRegistry()
        let executor = registry.indexExecutor(for: "__does_not_exist__")
        #expect(executor == nil)
    }

    @Test("Unknown fusion strategy returns nil executor")
    func unknownFusionStrategyReturnsNilExecutor() throws {
        let registry = try ReadExecutorRegistry()
        let executor = registry.fusionExecutor(for: "__does_not_exist__")
        #expect(executor == nil)
    }

    // MARK: - Enum surface

    @Test("Error cases carry diagnostic context")
    func errorCasesPreserveContext() {
        let missing = CanonicalReadError.missingAnnotation("distance")
        if case .missingAnnotation(let name) = missing {
            #expect(name == "distance")
        } else {
            Issue.record("Expected missingAnnotation, got \(missing)")
        }

        let unencodable = CanonicalReadError.unencodablePredicateValue(
            field: "age",
            valueDescription: "NaN"
        )
        if case .unencodablePredicateValue(let field, let desc) = unencodable {
            #expect(field == "age")
            #expect(desc == "NaN")
        } else {
            Issue.record("Expected unencodablePredicateValue, got \(unencodable)")
        }
    }
}

#endif

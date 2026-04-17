import Testing
import Foundation
import Core
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

    @Test("Unknown partition field throws invalidPartitionField")
    func invalidPartitionFieldRejected() {
        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalPartitionBinding.makeBinding(
                for: TenantOrder.self,
                partitionValues: ["nonexistentField": "value"]
            )
        }
    }

    @Test("Valid partition field succeeds")
    func validPartitionFieldAccepted() throws {
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: TenantOrder.self,
            partitionValues: ["tenantID": "tenant-1"]
        )
        #expect(binding != nil)
    }

    @Test("Static-directory model with nil partition returns nil binding")
    func staticDirectoryWithNilBinding() throws {
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: StaticModel.self,
            partitionValues: nil
        )
        #expect(binding == nil)
    }

    // MARK: - ReadExecutorRegistry

    @Test("Unknown index kind returns nil executor")
    func unknownIndexKindReturnsNilExecutor() {
        let registry = ReadExecutorRegistry()
        let executor = registry.indexExecutor(for: "__does_not_exist__")
        #expect(executor == nil)
    }

    @Test("Unknown fusion strategy returns nil executor")
    func unknownFusionStrategyReturnsNilExecutor() {
        let registry = ReadExecutorRegistry()
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

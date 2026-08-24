#if !os(WASI)
import Testing
import Foundation
import DatabaseKit
import DatabaseTypes
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
                partitions: try FieldObject([
                    (
                        key: "nonexistentField",
                        value: .string("value")
                    ),
                ])
            )
        }
    }

    @Test("Valid partition field succeeds")
    func validPartitionFieldAccepted() throws {
        let field = try #require(
            try TenantOrder.fieldSchemas.first { $0.name == "tenantID" }
        )
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: TenantOrder.self,
            partitions: try FieldObject([
                (
                    key: field.name,
                    value: .string("tenant-1")
                ),
            ])
        )
        #expect(binding != nil)
    }

    @Test("Partition value must match the compiled field type")
    func invalidPartitionTypeRejected() throws {
        let field = try #require(
            try TenantOrder.fieldSchemas.first { $0.name == "tenantID" }
        )
        #expect(throws: CanonicalReadError.self) {
            _ = try CanonicalPartitionBinding.makeBinding(
                for: TenantOrder.self,
                partitions: try FieldObject([
                    (
                        key: field.name,
                        value: .int64(1)
                    ),
                ])
            )
        }
    }

    @Test("Static-directory model with an empty partition returns nil binding")
    func staticDirectoryWithEmptyBinding() throws {
        let binding = try CanonicalPartitionBinding.makeBinding(
            for: StaticModel.self,
            partitions: FieldObject()
        )
        #expect(binding == nil)
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

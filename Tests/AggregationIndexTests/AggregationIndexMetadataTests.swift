import Testing
import TestHeartbeat
import DatabaseKit
import DatabaseTypes
@testable import AggregationIndex

@Suite("Aggregation index metadata", .heartbeat)
struct AggregationIndexMetadataTests {
    @Test("Definitions declare canonical identities")
    func definitionsDeclareCanonicalIdentities() {
        #expect(IndexDefinition.count.identifier == "count")
        #expect(IndexDefinition.sum.identifier == "sum")
        #expect(IndexDefinition.minimum.identifier == "min")
        #expect(IndexDefinition.maximum.identifier == "max")
        #expect(IndexDefinition.count.subspaceStructure == .aggregation)
        #expect(IndexDefinition.sum.subspaceStructure == .aggregation)
        #expect(IndexDefinition.minimum.subspaceStructure == .flat)
        #expect(IndexDefinition.maximum.subspaceStructure == .flat)
    }

    @Test("Metadata restores aggregation semantics")
    func metadataRestoresAggregationSemantics() throws {
        #expect(
            try IndexDefinition(
                metadata: countMetadata(groupingFields: [])
            ) == .count
        )
        #expect(
            try IndexDefinition(
                metadata: numericAggregationMetadata(
                    identifier: "sum",
                    subspaceStructure: .aggregation
                )
            ) == .sum
        )
        #expect(
            try IndexDefinition(
                metadata: numericAggregationMetadata(
                    identifier: "min",
                    subspaceStructure: .flat
                )
            ) == .minimum
        )
        #expect(
            try IndexDefinition(
                metadata: numericAggregationMetadata(
                    identifier: "max",
                    subspaceStructure: .flat
                )
            ) == .maximum
        )
    }

    @Test("Numeric aggregation metadata requires a value field")
    func numericMetadataRequiresValueField() {
        let metadata = IndexKindMetadata(
            identifier: "sum",
            subspaceStructure: .aggregation,
            fields: [],
            metadata: ["valueType": .string(IndexScalarType.int64.rawValue)]
        )

        #expect(throws: IndexKindMetadataError.self) {
            try IndexDefinition(metadata: metadata)
        }
    }

    private func countMetadata(
        groupingFields: [FieldIdentity]
    ) -> IndexKindMetadata {
        countIndexMetadata(groupingFields: groupingFields)
    }

    private func numericAggregationMetadata(
        identifier: String,
        subspaceStructure: SubspaceStructure
    ) -> IndexKindMetadata {
        IndexKindMetadata(
            identifier: identifier,
            subspaceStructure: subspaceStructure,
            fields: [
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "region", number: 2)
                ),
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "amount", number: 3)
                ),
            ],
            metadata: [
                "valueType": .string(IndexScalarType.int64.rawValue)
            ]
        )
    }
}

import Testing
import DatabaseKit
import TestSupport
@testable import ScalarIndex

func scalarIndexMetadata(
    fields: [FieldIdentity]
) -> IndexKindMetadata {
    let definition = IndexDefinition.scalar
    return IndexKindMetadata(
        identifier: definition.identifier,
        subspaceStructure: definition.subspaceStructure,
        fields: fields.map { IndexFieldMetadata(identity: $0) },
        metadata: [:]
    )
}

@Suite("Scalar index metadata", .heartbeat)
struct ScalarIndexMetadataTests {
    @Test("Scalar definition declares its canonical identity")
    func definitionDeclaresCanonicalIdentity() {
        #expect(IndexDefinition.scalar.identifier == "scalar")
        #expect(IndexDefinition.scalar.subspaceStructure == .flat)
    }

    @Test("Canonical metadata restores scalar semantics")
    func metadataRestoresScalarSemantics() throws {
        let metadata = scalarMetadata(fields: [
            FieldIdentity(name: "name", number: 2),
            FieldIdentity(name: "price", number: 3),
        ])

        #expect(try IndexDefinition(metadata: metadata) == .scalar)
    }

    @Test("Scalar metadata rejects an empty field selection")
    func metadataRejectsEmptyFields() {
        let metadata = scalarMetadata(fields: [])

        #expect(
            throws: IndexKindMetadataError.invalidFieldCount(
                identifier: "scalar",
                expected: "at least 1",
                actual: 0
            )
        ) {
            try IndexDefinition(metadata: metadata)
        }
    }

    @Test("Scalar metadata rejects noncanonical configuration")
    func metadataRejectsUnexpectedConfiguration() {
        let metadata = IndexKindMetadata(
            identifier: "scalar",
            subspaceStructure: .flat,
            fields: [
                IndexFieldMetadata(
                    identity: FieldIdentity(name: "name", number: 2)
                )
            ],
            metadata: ["ordering": .string("ascending")]
        )

        #expect(
            throws: IndexKindMetadataError.unexpectedMetadata(
                identifier: "scalar",
                key: "ordering"
            )
        ) {
            try IndexDefinition(metadata: metadata)
        }
    }

    private func scalarMetadata(
        fields: [FieldIdentity]
    ) -> IndexKindMetadata {
        scalarIndexMetadata(fields: fields)
    }
}

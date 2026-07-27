import AggregationIndex
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
import StorageKit
import Testing

@Suite("Distinct index canonical RDF validation")
struct DistinctIndexCanonicalRDFTests {
    @Test("invalid RDF fails before the transaction is mutated")
    func invalidRDFFailsBeforeMutation() async throws {
        let engine = InMemoryEngine()
        let subspace = Subspace(prefix: Tuple("distinct-rdf").pack())
        let index = Index(
            name: "rdf_distinct",
            kind: distinctIndexMetadata(
                groupingFields: [
                    FieldIdentity(name: "group", number: 2)
                ],
                valueField: FieldIdentity(name: "value", number: 3),
                precision: 14
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: "rdf_distinct",
            itemTypes: [RDFDistinctEntity.persistableType]
        )
        let maintainer = DistinctIndexMaintainer<RDFDistinctEntity>(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            precision: 14
        )
        let transaction = try engine.createTransaction()
        let entity = RDFDistinctEntity(
            id: "invalid",
            group: "calendar",
            value: .rdfTerm(.iri("relative"))
        )

        await #expect(
            throws: FieldValueTupleCodecError.invalidRDFTerm(
                .invalidIRI(.missingScheme)
            )
        ) {
            try await maintainer.updateIndex(
                oldItem: nil,
                newItem: entity,
                transaction: transaction
            )
        }

        let range = subspace.range()
        let rows = try await transaction.collectRange(
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end)
        )
        #expect(rows.isEmpty)
    }
}

private struct RDFDistinctEntity: Persistable {
    typealias ID = String

    let id: String
    let group: String
    let value: FieldValue

    static let persistableType = "RDFDistinctEntity"
    static let allFields = ["id", "group", "value"]
    static let indexDescriptors: [IndexDescriptor] = []

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": id
        case "group": group
        case "value": value
        default: nil
        }
    }

    static func fieldName<Value>(
        for keyPath: KeyPath<RDFDistinctEntity, Value>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<RDFDistinctEntity>)
    }

    static func fieldName(
        for keyPath: PartialKeyPath<RDFDistinctEntity>
    ) -> String {
        switch keyPath {
        case \RDFDistinctEntity.id: "id"
        case \RDFDistinctEntity.group: "group"
        case \RDFDistinctEntity.value: "value"
        default: String(describing: keyPath)
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<RDFDistinctEntity> else {
            return String(describing: keyPath)
        }
        return fieldName(for: keyPath)
    }
}

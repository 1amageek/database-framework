import AggregationIndex
import Core
import DatabaseEngine
import DatabaseValue
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
            kind: DistinctIndexKind<RDFDistinctRecord>(
                groupBy: [\.group],
                value: \.value
            ),
            rootExpression: ConcatenateKeyExpression(children: [
                FieldKeyExpression(fieldName: "group"),
                FieldKeyExpression(fieldName: "value"),
            ]),
            subspaceKey: "rdf_distinct",
            itemTypes: [RDFDistinctRecord.persistableType]
        )
        let maintainer = DistinctIndexMaintainer<RDFDistinctRecord>(
            index: index,
            subspace: subspace,
            idExpression: FieldKeyExpression(fieldName: "id"),
            precision: 14
        )
        let transaction = try engine.createTransaction()
        let record = RDFDistinctRecord(
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
                newItem: record,
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

private struct RDFDistinctRecord: Persistable {
    typealias ID = String

    let id: String
    let group: String
    let value: FieldValue

    static let persistableType = "RDFDistinctRecord"
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
        for keyPath: KeyPath<RDFDistinctRecord, Value>
    ) -> String {
        fieldName(for: keyPath as PartialKeyPath<RDFDistinctRecord>)
    }

    static func fieldName(
        for keyPath: PartialKeyPath<RDFDistinctRecord>
    ) -> String {
        switch keyPath {
        case \RDFDistinctRecord.id: "id"
        case \RDFDistinctRecord.group: "group"
        case \RDFDistinctRecord.value: "value"
        default: String(describing: keyPath)
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        guard let keyPath = keyPath as? PartialKeyPath<RDFDistinctRecord> else {
            return String(describing: keyPath)
        }
        return fieldName(for: keyPath)
    }
}

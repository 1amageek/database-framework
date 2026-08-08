#if !os(WASI)
import Testing
import Foundation
import StorageKit
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine

@Persistable
private struct TupleKeyExpressionEntity {
    var id: String
    var title: String
}

@Polymorphable(identifier: "TupleKeyExpressionPolymorphicEntity")
@PolymorphicDirectory("tuple_key_expression_polymorphic_entities")
private protocol TupleKeyExpressionPolymorphicEntity:
    Polymorphable<TupleKeyExpressionPolymorphicEntityPolymorphicGroup>
{
    var id: String { get }
}

@Persistable
private struct TupleKeyExpressionPolymorphicDocument:
    TupleKeyExpressionPolymorphicEntity
{
    var id: String
    var title: String
}

@Suite("TupleKeyExpression Tests")
struct TupleKeyExpressionTests {
    @Test("TupleKeyExpression preserves a pre-resolved identifier")
    func preservesResolvedIdentifier() throws {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")
        let identifier = try entity.persistableIdentifierTuple()

        let extracted = try DataAccess.extractId(
            from: entity,
            using: TupleKeyExpression(value: identifier)
        )

        #expect(extracted == identifier)
    }

    @Test("TupleKeyExpression rejects a non-canonical identifier")
    func rejectsNonCanonicalResolvedIdentifier() {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")

        #expect(throws: PolymorphicIdentifierKeyError.self) {
            _ = try DataAccess.extractId(
                from: entity,
                using: TupleKeyExpression(value: Tuple("entity-1", "extra"))
            )
        }
    }

    @Test("TupleKeyExpression preserves a canonical polymorphic identifier")
    func preservesResolvedPolymorphicIdentifier() throws {
        let entity = TupleKeyExpressionPolymorphicDocument(
            id: "entity-1",
            title: "Doc"
        )
        let identifier = try entity.persistableIdentifierTuple()
        let polymorphicIdentifier = try PolymorphicIdentifierKey.tuple(
            for: TupleKeyExpressionPolymorphicDocument.self,
            identifier: identifier
        )

        let extracted = try DataAccess.extractId(
            from: entity,
            using: TupleKeyExpression(value: polymorphicIdentifier)
        )

        #expect(extracted.pack() == polymorphicIdentifier.pack())
    }

    @Test("TupleKeyExpression rejects a mismatched polymorphic type code")
    func rejectsMismatchedPolymorphicTypeCode() throws {
        let entity = TupleKeyExpressionPolymorphicDocument(
            id: "entity-1",
            title: "Doc"
        )
        let identifier = try entity.persistableIdentifierTuple()
        let invalidIdentifier = Tuple(Int64.min).appending(identifier)

        #expect(throws: PolymorphicIdentifierKeyError.self) {
            _ = try DataAccess.extractId(
                from: entity,
                using: TupleKeyExpression(value: invalidIdentifier)
            )
        }
    }

    @Test("Field identifier uses the persistence key codec")
    func fieldIdentifierUsesPersistenceKeyCodec() throws {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")

        let extracted = try DataAccess.extractId(
            from: entity,
            using: FieldKeyExpression(fieldName: "id")
        )
        let canonical = try entity.persistableIdentifierTuple()

        #expect(extracted == canonical)
        #expect(extracted.pack() == canonical.pack())
    }

    @Test("Type-independent field identifier uses the persistence key codec")
    func persistedModelIdentifierUsesPersistenceKeyCodec() throws {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")
        let persisted = try PersistedModel(entity)

        let extracted = try DataAccess.extractId(
            from: persisted,
            using: FieldKeyExpression(fieldName: "id")
        )
        let canonical = try entity.persistableIdentifierTuple()

        #expect(extracted == canonical)
        #expect(extracted.pack() == canonical.pack())
    }

    @Test("Every persisted identifier shape preserves storage-key bytes")
    func persistedIdentifierShapesPreserveStorageKeyBytes() throws {
        let uuid = DatabaseTypes.UUID(high: 1, low: 2)
        let bytes = ByteString([0x00, 0x7f, 0xff])
        let cases: [(
            field: FieldValue,
            identifier: ReferenceIdentifier,
            type: PersistableIdentifierType
        )] = [
            (.bool(true), .bool(true), .bool),
            (.int8(-8), .int8(-8), .int8),
            (.int16(-16), .int16(-16), .int16),
            (.int32(-32), .int32(-32), .int32),
            (.int64(-64), .int64(-64), .int64),
            (.uint8(8), .uint8(8), .uint8),
            (.uint16(16), .uint16(16), .uint16),
            (.uint32(32), .uint32(32), .uint32),
            (.uint64(64), .uint64(64), .uint64),
            (.string("entity-1"), .string("entity-1"), .string),
            (.bytes(bytes), .bytes(bytes), .bytes),
            (.uuid(uuid), .uuid(uuid), .uuid),
            (
                .array([.string("tenant"), .uint64(42)]),
                .composite([.string("tenant"), .uint64(42)]),
                .composite([.string, .uint64])
            ),
        ]

        for value in cases {
            let typeIndependent = try PersistableIdentifierKeyCodec.tuple(
                forPersistedIdentifier: value.field
            )
            let typed = try PersistableIdentifierKeyCodec.tuple(
                for: value.identifier,
                expectedType: value.type
            )
            #expect(typeIndependent.pack() == typed.pack())
        }
    }

    @Test("Non-identifier persisted values are rejected")
    func rejectsNonIdentifierPersistedValues() {
        #expect(throws: PersistableIdentifierKeyError.self) {
            _ = try PersistableIdentifierKeyCodec.tuple(
                forPersistedIdentifier: .decimal(
                    ExactDecimal(coefficient: 1, scale: 0)
                )
            )
        }
    }

    @Test("Non-identifier field cannot define persistence identity")
    func rejectsNonIdentifierField() {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")

        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractId(
                from: entity,
                using: FieldKeyExpression(fieldName: "title")
            )
        }
    }
}

#endif

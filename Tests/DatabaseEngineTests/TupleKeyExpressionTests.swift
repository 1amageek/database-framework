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

        #expect(throws: PersistableIdentifierKeyError.self) {
            _ = try DataAccess.extractId(
                from: entity,
                using: TupleKeyExpression(value: Tuple("entity-1", "extra"))
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

#if !os(WASI)
import Testing
import Foundation
import StorageKit
import Core
import DatabaseValue
@testable import DatabaseEngine

private struct TupleKeyExpressionEntity: Persistable, Codable, Sendable {
    typealias ID = String

    var id: String
    var title: String

    static var persistableType: String { "TupleKeyExpressionEntity" }
    static var allFields: [String] { ["id", "title"] }
    static var indexDescriptors: [IndexDescriptor] { [] }
    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "title": return title
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<TupleKeyExpressionEntity, Value>) -> String {
        switch keyPath {
        case \TupleKeyExpressionEntity.id: return "id"
        case \TupleKeyExpressionEntity.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TupleKeyExpressionEntity>) -> String {
        switch keyPath {
        case \TupleKeyExpressionEntity.id: return "id"
        case \TupleKeyExpressionEntity.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TupleKeyExpressionEntity> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("TupleKeyExpression Tests")
struct TupleKeyExpressionTests {
    @Test("TupleKeyExpression preserves composite tuple IDs")
    func preservesCompositeTuple() throws {
        let entity = TupleKeyExpressionEntity(id: "entity-1", title: "Doc")
        let compositeID = Tuple([Int64(42), "entity-1"])

        let extracted = try DataAccess.extractId(
            from: entity,
            using: TupleKeyExpression(value: compositeID)
        )

        #expect(extracted == compositeID)
    }
}

#endif

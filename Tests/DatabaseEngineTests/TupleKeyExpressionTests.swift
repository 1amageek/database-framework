import Testing
import Foundation
import StorageKit
import Core
@testable import DatabaseEngine

private struct TupleKeyExpressionRecord: Persistable, Codable, Sendable {
    typealias ID = String

    var id: String
    var title: String

    static var persistableType: String { "TupleKeyExpressionRecord" }
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

    static func fieldName<Value>(for keyPath: KeyPath<TupleKeyExpressionRecord, Value>) -> String {
        switch keyPath {
        case \TupleKeyExpressionRecord.id: return "id"
        case \TupleKeyExpressionRecord.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<TupleKeyExpressionRecord>) -> String {
        switch keyPath {
        case \TupleKeyExpressionRecord.id: return "id"
        case \TupleKeyExpressionRecord.title: return "title"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<TupleKeyExpressionRecord> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("TupleKeyExpression Tests")
struct TupleKeyExpressionTests {
    @Test("TupleKeyExpression preserves composite tuple IDs")
    func preservesCompositeTuple() throws {
        let record = TupleKeyExpressionRecord(id: "record-1", title: "Doc")
        let compositeID = Tuple([Int64(42), "record-1"])

        let extracted = try DataAccess.extractId(
            from: record,
            using: TupleKeyExpression(value: compositeID)
        )

        #expect(extracted == compositeID)
    }
}

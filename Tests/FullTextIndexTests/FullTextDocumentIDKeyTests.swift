import Testing
import Core
import DatabaseValue
import DatabaseEngine
import StorageKit
@testable import FullTextIndex

private struct FullTextIntIDDocument: Persistable {
    typealias ID = Int64

    let id: Int64
    let body: String

    static var persistableType: String { "FullTextIntIDDocument" }
    static var allFields: [String] { ["id", "body"] }
    static var indexDescriptors: [IndexDescriptor] { [] }

    static func fieldNumber(for fieldName: String) -> Int? { nil }
    static func enumMetadata(for fieldName: String) -> EnumMetadata? { nil }

    subscript(dynamicMember member: String) -> (any Sendable)? {
        switch member {
        case "id": return id
        case "body": return body
        default: return nil
        }
    }

    static func fieldName<Value>(for keyPath: KeyPath<FullTextIntIDDocument, Value>) -> String {
        switch keyPath {
        case \FullTextIntIDDocument.id: return "id"
        case \FullTextIntIDDocument.body: return "body"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: PartialKeyPath<FullTextIntIDDocument>) -> String {
        switch keyPath {
        case \FullTextIntIDDocument.id: return "id"
        case \FullTextIntIDDocument.body: return "body"
        default: return "\(keyPath)"
        }
    }

    static func fieldName(for keyPath: AnyKeyPath) -> String {
        if let partial = keyPath as? PartialKeyPath<FullTextIntIDDocument> {
            return fieldName(for: partial)
        }
        return "\(keyPath)"
    }
}

@Suite("Full-text document ID lookup key")
struct FullTextDocumentIDKeyTests {
    @Test("document lookup key uses DataAccess ID extraction instead of forced casting")
    func documentLookupKeySupportsNonStringIDs() throws {
        let item = FullTextIntIDDocument(id: 42, body: "swift database")

        let expected = FullTextDocumentIDKey.encoded(Tuple(Int64(42)))
        let actual = try FullTextDocumentIDKey.encoded(for: item)

        #expect(actual == expected)
    }
}

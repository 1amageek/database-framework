import Testing
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit
@testable import FullTextIndex

@Persistable
private struct FullTextIntIDDocument {
    let id: Int64
    let body: String
}

@Suite("Full-text document lookup key")
struct FullTextDocumentLookupKeyTests {
    @Test("document lookup key uses DataAccess ID extraction instead of forced casting")
    func documentLookupKeySupportsNonStringIDs() throws {
        let item = FullTextIntIDDocument(id: 42, body: "swift database")

        let expected = FullTextDocumentLookupKey.key(for: Tuple(Int64(42)))
        let actual = try FullTextDocumentLookupKey.key(for: item)

        #expect(actual == expected)
    }
}

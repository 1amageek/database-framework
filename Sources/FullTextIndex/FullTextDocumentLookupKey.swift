// FullTextDocumentLookupKey.swift
// FullTextIndex - Stable lookup key for fetched documents

import DatabaseTypes
import DatabaseKit
import DatabaseEngine
import StorageKit

internal enum FullTextDocumentLookupKey {
    static func key(for id: Tuple) -> ByteString {
        id.pack()
    }

    static func key<Item: Persistable>(
        for item: Item,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) throws -> ByteString {
        let id = try DataAccess.extractId(from: item, using: idExpression)
        return key(for: id)
    }
}

// FullTextDocumentLookupKey.swift
// FullTextIndex - Stable lookup key for fetched documents

import Core
import DatabaseEngine
import StorageKit

internal enum FullTextDocumentLookupKey {
    static func key(for id: Tuple) -> Bytes {
        id.pack()
    }

    static func key<Item: Persistable>(
        for item: Item,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) throws -> Bytes {
        let id = try DataAccess.extractId(from: item, using: idExpression)
        return key(for: id)
    }
}

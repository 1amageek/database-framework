// FullTextDocumentIDKey.swift
// FullTextIndex - Stable lookup key for fetched documents

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit

internal enum FullTextDocumentIDKey {
    static func encoded(_ id: Tuple) -> String {
        Data(id.pack()).base64EncodedString()
    }

    static func encoded<Item: Persistable>(
        for item: Item,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) throws -> String {
        let id = try DataAccess.extractId(from: item, using: idExpression)
        return encoded(id)
    }
}

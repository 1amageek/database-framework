// SpatialPrimaryKey.swift
// SpatialIndex - Stable primary-key extraction for fetched spatial items

import DatabaseKit
import DatabaseEngine
import StorageKit

internal enum SpatialPrimaryKey {
    static func tuple<Item: Persistable>(
        for item: Item,
        idExpression: KeyExpression = FieldKeyExpression(fieldName: "id")
    ) throws -> Tuple {
        try DataAccess.extractId(from: item, using: idExpression)
    }
}

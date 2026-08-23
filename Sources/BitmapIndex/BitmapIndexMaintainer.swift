// BitmapIndexMaintainer.swift
// BitmapIndexLayer - Index maintainer for BITMAP_VALUE indexes
//
// Provides efficient set operations on low-cardinality fields using Roaring Bitmaps.
// Reference: Lemire et al., "Roaring Bitmaps", 2016

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Maintainer for BITMAP_VALUE indexes
///
/// **Functionality**:
/// - Maintain bitmaps for each distinct field value
/// - Fast AND/OR/NOT operations across multiple values
/// - Efficient cardinality counting
///
/// **Index Structure**:
/// ```
/// // Bitmap data per field value
/// Key: [indexSubspace]["data"][fieldValue]
/// Value: Serialized RoaringBitmap
///
/// // Value-to-ID mapping (for sequential ID assignment)
/// Key: [indexSubspace]["meta"]["nextId"]
/// Value: Int64 (next sequential ID)
///
/// // ID-to-primaryKey mapping
/// Key: [indexSubspace]["ids"][sequentialId]
/// Value: primaryKey bytes
///
/// // PrimaryKey-to-ID mapping (reverse lookup)
/// Key: [indexSubspace]["pks"][primaryKey]
/// Value: sequentialId (Int64)
/// ```
///
/// **Design Rationale**:
/// Roaring bitmaps use 32-bit integers for efficiency. Since primary keys can be
/// any type (String, UUID, etc.), we assign sequential 32-bit IDs to each entity
/// and maintain bidirectional mappings.
///
/// **Examples**:
/// ```swift
/// // User status bitmap
/// Key: [I]/User_bitmap_status/["data"]/["active"] = RoaringBitmap{0,1,3,5,...}
/// Key: [I]/User_bitmap_status/["data"]/["inactive"] = RoaringBitmap{2,4,6,...}
/// Key: [I]/User_bitmap_status/["ids"]/[0] = "user-abc"
/// Key: [I]/User_bitmap_status/["ids"]/[1] = "user-def"
/// Key: [I]/User_bitmap_status/["pks"]/["user-abc"] = 0
/// ```
public struct BitmapIndexMaintainer<Item: PersistedEntityValue>: SubspaceIndexMaintainer {
    enum StorageError: Error, Sendable, Equatable {
        case invalidSequentialID(Int64)
    }
    internal enum ValueTransition: Equatable {
        case unchanged
        case add
        case remove
        case replace
    }

    // MARK: - Properties

    /// Index definition
    public let index: ResolvedIndex

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // Subspace keys
    private var dataSubspace: Subspace { subspace.subspace("data") }
    private var idsSubspace: Subspace { subspace.subspace("ids") }
    private var pksSubspace: Subspace { subspace.subspace("pks") }
    private var nextIdKey: ByteString { subspace.subspace("meta").pack(Tuple("nextId")) }
    private var reader: BitmapIndexReader { BitmapIndexReader(subspace: subspace) }

    // MARK: - Initialization

    public init(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
    }

    internal static func valueTransition(oldValueKey: ByteString?, newValueKey: ByteString?) -> ValueTransition {
        switch (oldValueKey, newValueKey) {
        case (nil, nil):
            return .unchanged
        case (nil, _?):
            return .add
        case (_?, nil):
            return .remove
        case (let old?, let new?) where old == new:
            return .unchanged
        case (_?, _?):
            return .replace
        }
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil, the item is not indexed.
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionAccess
    ) async throws {
        // Get primary keys
        let oldPK: Tuple? = try oldItem.map { try DataAccess.extractId(from: $0, using: idExpression) }
        let newPK: Tuple? = try newItem.map { try DataAccess.extractId(from: $0, using: idExpression) }

        // Get field values (sparse index: nil values are not indexed)
        let oldValue: [any TupleElement]?
        if let item = oldItem {
            do {
                oldValue = try evaluateIndexFields(from: item)
            } catch DataAccessError.nilValueCannotBeIndexed {
                oldValue = nil
            }
        } else {
            oldValue = nil
        }

        let newValue: [any TupleElement]?
        if let item = newItem {
            do {
                newValue = try evaluateIndexFields(from: item)
            } catch DataAccessError.nilValueCannotBeIndexed {
                newValue = nil
            }
        } else {
            newValue = nil
        }

        switch (oldPK, newPK) {
        case (nil, let pk?):
            // Insert
            let seqId = try await getOrCreateSequentialId(for: pk, transaction: transaction)
            if let values = newValue {
                try await addToBitmap(fieldValues: values, sequentialId: seqId, transaction: transaction)
            }

        case (let pk?, nil):
            // Delete
            if let seqId = try await getSequentialId(for: pk, transaction: transaction) {
                if let values = oldValue {
                    try await removeFromBitmap(fieldValues: values, sequentialId: seqId, transaction: transaction)
                }
                // Clean up mappings
                try await removeSequentialId(pk: pk, seqId: seqId, transaction: transaction)
            }

        case (let oldPK?, let newPK?):
            // Update
            let oldPKBytes = try packPrimaryKey(oldPK)
            let newPKBytes = try packPrimaryKey(newPK)

            if oldPKBytes == newPKBytes {
                let oldValueKey = try optionalFieldValueKey(oldValue)
                let newValueKey = try optionalFieldValueKey(newValue)

                switch Self.valueTransition(oldValueKey: oldValueKey, newValueKey: newValueKey) {
                case .unchanged:
                    break

                case .add:
                    guard let newValue else { break }
                    let seqId = try await getOrCreateSequentialId(for: oldPK, transaction: transaction)
                    try await addToBitmap(fieldValues: newValue, sequentialId: seqId, transaction: transaction)

                case .remove:
                    guard let oldValue else { break }
                    if let seqId = try await getSequentialId(for: oldPK, transaction: transaction) {
                        try await removeFromBitmap(fieldValues: oldValue, sequentialId: seqId, transaction: transaction)
                    }

                case .replace:
                    guard let oldValue, let newValue else { break }
                    let seqId = try await getOrCreateSequentialId(for: oldPK, transaction: transaction)
                    try await removeFromBitmap(fieldValues: oldValue, sequentialId: seqId, transaction: transaction)
                    try await addToBitmap(fieldValues: newValue, sequentialId: seqId, transaction: transaction)
                }
            } else {
                // Primary key changed (unusual)
                // Remove old, add new
                if let oldSeqId = try await getSequentialId(for: oldPK, transaction: transaction) {
                    if let values = oldValue {
                        try await removeFromBitmap(fieldValues: values, sequentialId: oldSeqId, transaction: transaction)
                    }
                    try await removeSequentialId(pk: oldPK, seqId: oldSeqId, transaction: transaction)
                }

                let newSeqId = try await getOrCreateSequentialId(for: newPK, transaction: transaction)
                if let values = newValue {
                    try await addToBitmap(fieldValues: values, sequentialId: newSeqId, transaction: transaction)
                }
            }

        case (nil, nil):
            break
        }
    }

    /// Build index entries for an item during batch indexing
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil, the item is not indexed.
    public func scanItem(
        _ item: Item,
        id: Tuple,
        transaction: any TransactionAccess
    ) async throws {
        // Sparse index: if field value is nil, skip indexing
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return
        }
        let seqId = try await getOrCreateSequentialId(for: id, transaction: transaction)
        try await addToBitmap(fieldValues: fieldValues, sequentialId: seqId, transaction: transaction)
    }

    /// Compute expected index keys for an item
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil, returns an empty array.
    public func computeIndexKeys(
        for item: Item,
        id: Tuple
    ) async throws -> [ByteString] {
        // Sparse index: if field value is nil, no index entry
        let fieldValues: [any TupleElement]
        do {
            fieldValues = try evaluateIndexFields(from: item)
        } catch DataAccessError.nilValueCannotBeIndexed {
            return []
        }
        let valueKey = try makeFieldValueKey(fieldValues)
        return [dataSubspace.pack(Tuple(valueKey))]
    }

    // MARK: - Private Helpers

    private func optionalFieldValueKey(_ values: [any TupleElement]?) throws -> ByteString? {
        guard let values else {
            return nil
        }
        return try makeFieldValueKey(values)
    }

    /// Get or create a sequential ID for a primary key
    private func getOrCreateSequentialId(
        for pk: Tuple,
        transaction: any TransactionAccess
    ) async throws -> UInt32 {
        // Check if already exists
        let pkKey = pksSubspace.pack(pk)
        if let existing = try await transaction.getValue(for: pkKey) {
            let value = try ByteConversion.bytesToInt64(existing)
            guard let sequentialID = UInt32(exactly: value) else {
                throw StorageError.invalidSequentialID(value)
            }
            return sequentialID
        }

        // Allocate new ID
        let nextId: Int64
        if let currentBytes = try await transaction.getValue(for: nextIdKey) {
            nextId = try ByteConversion.bytesToInt64(currentBytes)
        } else {
            nextId = 0
        }

        // Store mappings
        guard let seqId = UInt32(exactly: nextId) else {
            throw StorageError.invalidSequentialID(nextId)
        }
        let seqIdBytes = ByteConversion.int64ToBytes(Int64(seqId))

        // pk -> seqId
        try transaction.setValue(seqIdBytes, for: pkKey)

        // seqId -> pk
        let idKey = idsSubspace.pack(Tuple(Int(seqId)))
        try transaction.setValue(pk.pack(), for: idKey)

        // Update next ID
        try transaction.setValue(ByteConversion.int64ToBytes(nextId + 1), for: nextIdKey)

        return seqId
    }

    /// Get sequential ID for a primary key (if exists)
    private func getSequentialId(
        for pk: Tuple,
        transaction: any TransactionAccess
    ) async throws -> UInt32? {
        let pkKey = pksSubspace.pack(pk)
        guard let bytes = try await transaction.getValue(for: pkKey) else {
            return nil
        }
        let value = try ByteConversion.bytesToInt64(bytes)
        guard let sequentialID = UInt32(exactly: value) else {
            throw StorageError.invalidSequentialID(value)
        }
        return sequentialID
    }

    /// Remove sequential ID mappings
    private func removeSequentialId(
        pk: Tuple,
        seqId: UInt32,
        transaction: any TransactionAccess
    ) async throws {
        let pkKey = pksSubspace.pack(pk)
        let idKey = idsSubspace.pack(Tuple(Int(seqId)))
        try transaction.clear(key: pkKey)
        try transaction.clear(key: idKey)
    }

    /// Add a sequential ID to a bitmap for given field values
    private func addToBitmap(
        fieldValues: [any TupleElement],
        sequentialId: UInt32,
        transaction: any TransactionAccess
    ) async throws {
        let key = dataSubspace.pack(Tuple(fieldValues))

        var bitmap: RoaringBitmap
        if let existingBytes = try await transaction.getValue(for: key) {
            bitmap = try RoaringBitmap(serializedBytes: existingBytes)
        } else {
            bitmap = RoaringBitmap()
        }

        bitmap.add(UInt32(sequentialId))
        try transaction.setValue(bitmap.serializedBytes(), for: key)
    }

    /// Remove a sequential ID from a bitmap
    private func removeFromBitmap(
        fieldValues: [any TupleElement],
        sequentialId: UInt32,
        transaction: any TransactionAccess
    ) async throws {
        let key = dataSubspace.pack(Tuple(fieldValues))

        guard let existingBytes = try await transaction.getValue(for: key) else {
            return
        }

        var bitmap = try RoaringBitmap(serializedBytes: existingBytes)
        bitmap.remove(UInt32(sequentialId))

        if bitmap.isEmpty {
            try transaction.clear(key: key)
        } else {
            try transaction.setValue(bitmap.serializedBytes(), for: key)
        }
    }

    /// Make a key from field values for comparison
    private func makeFieldValueKey(_ values: [any TupleElement]) throws -> ByteString {
        return Tuple(values).pack()
    }

    /// Pack primary key for comparison
    private func packPrimaryKey(_ pk: Tuple) throws -> ByteString {
        return pk.pack()
    }

    // MARK: - Query Methods

    /// Get the bitmap for a specific field value
    ///
    /// - Parameters:
    ///   - fieldValue: The field value to query
    ///   - transaction: The transaction to use
    /// - Returns: RoaringBitmap of matching entity IDs
    package func getBitmap(
        for fieldValues: [FieldValue],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        try await reader.bitmap(
            for: try FieldValue.toTupleElements(fieldValues),
            transaction: transaction
        )
    }

    /// Get count of entities with a specific field value
    ///
    /// - Parameters:
    ///   - fieldValue: The field value to count
    ///   - transaction: The transaction to use
    /// - Returns: Number of entities with this value
    package func getCount(
        for fieldValues: [FieldValue],
        transaction: any TransactionReadAccess
    ) async throws -> Int {
        let bitmap = try await reader.bitmap(
            for: try FieldValue.toTupleElements(fieldValues),
            transaction: transaction
        )
        return bitmap.cardinality
    }

    /// Perform AND query across multiple values
    ///
    /// - Parameters:
    ///   - values: Array of field values to AND together
    ///   - transaction: The transaction to use
    /// - Returns: Bitmap of entities matching ALL values
    package func andQuery(
        values: [[FieldValue]],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        var encodedValues: [[any TupleElement]] = []
        encodedValues.reserveCapacity(values.count)
        for value in values {
            encodedValues.append(try FieldValue.toTupleElements(value))
        }
        return try await reader.intersection(
            of: encodedValues,
            transaction: transaction
        )
    }

    /// Perform OR query across multiple values
    ///
    /// - Parameters:
    ///   - values: Array of field values to OR together
    ///   - transaction: The transaction to use
    /// - Returns: Bitmap of entities matching ANY value
    package func orQuery(
        values: [[FieldValue]],
        transaction: any TransactionReadAccess
    ) async throws -> RoaringBitmap {
        var encodedValues: [[any TupleElement]] = []
        encodedValues.reserveCapacity(values.count)
        for value in values {
            encodedValues.append(try FieldValue.toTupleElements(value))
        }
        return try await reader.union(
            of: encodedValues,
            transaction: transaction
        )
    }

    /// Convert sequential IDs to primary keys
    ///
    /// - Parameters:
    ///   - bitmap: Bitmap of sequential IDs
    ///   - transaction: The transaction to use
    /// - Returns: Array of primary key tuples
    package func getPrimaryKeys(
        from bitmap: RoaringBitmap,
        transaction: any TransactionReadAccess
    ) async throws -> [Tuple] {
        try await reader.primaryKeys(
            for: bitmap,
            transaction: transaction
        )
    }

    /// Get all distinct field values in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of distinct field values
    package func getAllDistinctValues(
        transaction: any TransactionReadAccess
    ) async throws -> [[FieldValue]] {
        let storedValues = try await reader.distinctValues(
            transaction: transaction
        )
        var values: [[FieldValue]] = []
        values.reserveCapacity(storedValues.count)
        for storedValue in storedValues {
            var decodedValue: [FieldValue] = []
            decodedValue.reserveCapacity(storedValue.count)
            for element in storedValue {
                decodedValue.append(try FieldValue(tupleElement: element))
            }
            values.append(decodedValue)
        }
        return values
    }
}

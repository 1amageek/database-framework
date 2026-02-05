// BitmapIndexMaintainer.swift
// BitmapIndexLayer - Index maintainer for BITMAP_VALUE indexes
//
// Provides efficient set operations on low-cardinality fields using Roaring Bitmaps.
// Reference: Lemire et al., "Roaring Bitmaps", 2016

import Foundation
import Core
import DatabaseEngine
import FoundationDB

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
/// any type (String, UUID, etc.), we assign sequential 32-bit IDs to each record
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
public struct BitmapIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
    // MARK: - Properties

    /// Index definition
    public let index: Index

    /// Subspace for index storage
    public let subspace: Subspace

    /// ID expression for extracting item's unique identifier
    public let idExpression: KeyExpression

    // Subspace keys
    private var dataSubspace: Subspace { subspace.subspace("data") }
    private var idsSubspace: Subspace { subspace.subspace("ids") }
    private var pksSubspace: Subspace { subspace.subspace("pks") }
    private var nextIdKey: FDB.Bytes { subspace.subspace("meta").pack(Tuple("nextId")) }

    // MARK: - Initialization

    public init(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression
    ) {
        self.index = index
        self.subspace = subspace
        self.idExpression = idExpression
    }

    // MARK: - IndexMaintainer

    /// Update index when item changes
    ///
    /// **Sparse index behavior**:
    /// If the field value is nil, the item is not indexed.
    public func updateIndex(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
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
                // Same record, check if value changed
                if let oldVals = oldValue, let newVals = newValue {
                    let oldKey = try makeFieldValueKey(oldVals)
                    let newKey = try makeFieldValueKey(newVals)

                    if oldKey != newKey {
                        // Value changed - update bitmap
                        if let seqId = try await getSequentialId(for: oldPK, transaction: transaction) {
                            try await removeFromBitmap(fieldValues: oldVals, sequentialId: seqId, transaction: transaction)
                            try await addToBitmap(fieldValues: newVals, sequentialId: seqId, transaction: transaction)
                        }
                    }
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
        transaction: any TransactionProtocol
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
    ) async throws -> [FDB.Bytes] {
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

    /// Get or create a sequential ID for a primary key
    private func getOrCreateSequentialId(
        for pk: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> UInt32 {
        // Check if already exists
        let pkKey = pksSubspace.pack(pk)
        if let existing = try await transaction.getValue(for: pkKey) {
            return UInt32(ByteConversion.bytesToInt64(existing))
        }

        // Allocate new ID
        let nextId: Int64
        if let currentBytes = try await transaction.getValue(for: nextIdKey) {
            nextId = ByteConversion.bytesToInt64(currentBytes)
        } else {
            nextId = 0
        }

        // Store mappings
        let seqId = UInt32(nextId & 0xFFFFFFFF)
        let seqIdBytes = ByteConversion.int64ToBytes(Int64(seqId))

        // pk -> seqId
        transaction.setValue(seqIdBytes, for: pkKey)

        // seqId -> pk
        let idKey = idsSubspace.pack(Tuple(Int(seqId)))
        transaction.setValue(pk.pack(), for: idKey)

        // Update next ID
        transaction.setValue(ByteConversion.int64ToBytes(nextId + 1), for: nextIdKey)

        return seqId
    }

    /// Get sequential ID for a primary key (if exists)
    private func getSequentialId(
        for pk: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> UInt32? {
        let pkKey = pksSubspace.pack(pk)
        guard let bytes = try await transaction.getValue(for: pkKey) else {
            return nil
        }
        return UInt32(ByteConversion.bytesToInt64(bytes))
    }

    /// Remove sequential ID mappings
    private func removeSequentialId(
        pk: Tuple,
        seqId: UInt32,
        transaction: any TransactionProtocol
    ) async throws {
        let pkKey = pksSubspace.pack(pk)
        let idKey = idsSubspace.pack(Tuple(Int(seqId)))
        transaction.clear(key: pkKey)
        transaction.clear(key: idKey)
    }

    /// Add a sequential ID to a bitmap for given field values
    private func addToBitmap(
        fieldValues: [any TupleElement],
        sequentialId: UInt32,
        transaction: any TransactionProtocol
    ) async throws {
        let key = dataSubspace.pack(Tuple(fieldValues))

        var bitmap: RoaringBitmap
        if let existingBytes = try await transaction.getValue(for: key) {
            bitmap = try RoaringBitmap.deserialize(Data(existingBytes))
        } else {
            bitmap = RoaringBitmap()
        }

        bitmap.add(UInt32(sequentialId))
        let data = try bitmap.serialize()
        transaction.setValue(Array(data), for: key)
    }

    /// Remove a sequential ID from a bitmap
    private func removeFromBitmap(
        fieldValues: [any TupleElement],
        sequentialId: UInt32,
        transaction: any TransactionProtocol
    ) async throws {
        let key = dataSubspace.pack(Tuple(fieldValues))

        guard let existingBytes = try await transaction.getValue(for: key) else {
            return
        }

        var bitmap = try RoaringBitmap.deserialize(Data(existingBytes))
        bitmap.remove(UInt32(sequentialId))

        if bitmap.isEmpty {
            transaction.clear(key: key)
        } else {
            let data = try bitmap.serialize()
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Make a key from field values for comparison
    private func makeFieldValueKey(_ values: [any TupleElement]) throws -> FDB.Bytes {
        return Tuple(values).pack()
    }

    /// Pack primary key for comparison
    private func packPrimaryKey(_ pk: Tuple) throws -> FDB.Bytes {
        return pk.pack()
    }

    // MARK: - Query Methods

    /// Get the bitmap for a specific field value
    ///
    /// - Parameters:
    ///   - fieldValue: The field value to query
    ///   - transaction: The transaction to use
    /// - Returns: RoaringBitmap of matching record IDs
    public func getBitmap(
        for fieldValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> RoaringBitmap {
        let key = dataSubspace.pack(Tuple(fieldValues))

        guard let bytes = try await transaction.getValue(for: key) else {
            return RoaringBitmap()
        }

        return try RoaringBitmap.deserialize(Data(bytes))
    }

    /// Get count of records with a specific field value
    ///
    /// - Parameters:
    ///   - fieldValue: The field value to count
    ///   - transaction: The transaction to use
    /// - Returns: Number of records with this value
    public func getCount(
        for fieldValues: [any TupleElement],
        transaction: any TransactionProtocol
    ) async throws -> Int {
        let bitmap = try await getBitmap(for: fieldValues, transaction: transaction)
        return bitmap.cardinality
    }

    /// Perform AND query across multiple values
    ///
    /// - Parameters:
    ///   - values: Array of field values to AND together
    ///   - transaction: The transaction to use
    /// - Returns: Bitmap of records matching ALL values
    public func andQuery(
        values: [[any TupleElement]],
        transaction: any TransactionProtocol
    ) async throws -> RoaringBitmap {
        guard !values.isEmpty else { return RoaringBitmap() }

        var result = try await getBitmap(for: values[0], transaction: transaction)
        for value in values.dropFirst() {
            let bitmap = try await getBitmap(for: value, transaction: transaction)
            result = result && bitmap
        }
        return result
    }

    /// Perform OR query across multiple values
    ///
    /// - Parameters:
    ///   - values: Array of field values to OR together
    ///   - transaction: The transaction to use
    /// - Returns: Bitmap of records matching ANY value
    public func orQuery(
        values: [[any TupleElement]],
        transaction: any TransactionProtocol
    ) async throws -> RoaringBitmap {
        guard !values.isEmpty else { return RoaringBitmap() }

        var result = RoaringBitmap()
        for value in values {
            let bitmap = try await getBitmap(for: value, transaction: transaction)
            result = result || bitmap
        }
        return result
    }

    /// Convert sequential IDs to primary keys
    ///
    /// - Parameters:
    ///   - bitmap: Bitmap of sequential IDs
    ///   - transaction: The transaction to use
    /// - Returns: Array of primary key tuples
    public func getPrimaryKeys(
        from bitmap: RoaringBitmap,
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        let ids = bitmap.toArray()
        var results: [Tuple] = []
        results.reserveCapacity(ids.count)

        for seqId in ids {
            let idKey = idsSubspace.pack(Tuple(Int(seqId)))
            if let pkBytes = try await transaction.getValue(for: idKey) {
                let pkElements = try Tuple.unpack(from: pkBytes)
                results.append(Tuple(pkElements))
            }
        }
        return results
    }

    /// Get all distinct field values in this index
    ///
    /// - Parameter transaction: The transaction to use
    /// - Returns: Array of distinct field values
    public func getAllDistinctValues(
        transaction: any TransactionProtocol
    ) async throws -> [[any TupleElement]] {
        let range = dataSubspace.range()
        var results: [[any TupleElement]] = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(range.begin),
            endSelector: .firstGreaterOrEqual(range.end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            guard dataSubspace.contains(key) else { break }

            let keyTuple = try dataSubspace.unpack(key)
            // Avoid pack/unpack cycle: convert Tuple to array directly
            let elements: [any TupleElement] = (0..<keyTuple.count).compactMap { keyTuple[$0] }
            results.append(elements)
        }

        return results
    }
}

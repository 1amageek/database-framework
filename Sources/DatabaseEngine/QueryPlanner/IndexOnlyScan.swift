// IndexOnlyScan.swift
// QueryPlanner - Index-only scan (covering index) optimization
//
// A true Index-Only Scan reconstructs records entirely from index data,
// eliminating the need for record fetches. This is only possible when
// the index contains ALL fields of the record type T.

import Foundation
import Core

// MARK: - Covering Index Metadata

/// Metadata describing a covering index's field layout
///
/// A covering index contains all fields needed to fully reconstruct a record.
/// This struct maps index positions to field names for decoding.
public struct CoveringIndexMetadata: Sendable {
    /// Field names in the index key (in order)
    public let keyFields: [String]

    /// Field names stored in the index value (in order)
    public let storedFields: [String]

    /// Whether this index can fully reconstruct records of type T
    public let isFullyCovering: Bool

    /// All fields available in the index
    public var allFields: Set<String> {
        var fields = Set(keyFields)
        fields.formUnion(storedFields)
        fields.insert("id") // ID is always in the key
        return fields
    }

    public init(keyFields: [String], storedFields: [String], isFullyCovering: Bool) {
        self.keyFields = keyFields
        self.storedFields = storedFields
        self.isFullyCovering = isFullyCovering
    }

    /// Build metadata for an index against a Persistable type
    public static func build<T: Persistable>(for index: IndexDescriptor, type: T.Type) -> CoveringIndexMetadata {
        // Extract key field names
        let keyFields = index.keyPaths.map { T.fieldName(for: $0) }

        // Extract stored field names (from extension)
        let storedFields = index.storedKeyPaths.map { T.fieldName(for: $0) }

        // Check if all fields of T are covered
        let allIndexFields = Set(keyFields + storedFields + ["id"])
        let allModelFields = Set(T.allFields)

        let isFullyCovering = allModelFields.isSubset(of: allIndexFields)

        return CoveringIndexMetadata(
            keyFields: keyFields,
            storedFields: storedFields,
            isFullyCovering: isFullyCovering
        )
    }
}

// MARK: - Index Entry Decoder

/// Decodes a Persistable item from index entry data using Protobuf wire format
///
/// This decoder enables true Index-Only Scan by reconstructing
/// items entirely from IndexEntry values without fetching from storage.
///
/// **Flow**:
/// ```
/// IndexEntry (Tuple) → Protobuf bytes → ProtobufDecoder → Persistable
/// ```
///
/// **Requirements**:
/// - Index must be fully covering (contains ALL fields of T)
/// - IndexEntry must contain values in the expected order
///
/// **Usage**:
/// ```swift
/// let decoder = IndexEntryDecoder<User>(metadata: coveringMetadata)
/// let user = try decoder.decode(from: indexEntry)
/// ```
public struct IndexEntryDecoder<T: Persistable & Codable>: Sendable {
    private let metadata: CoveringIndexMetadata

    /// Field name to field number mapping (uses DJB2 hash to match ProtobufDecoder)
    private let fieldNumberMap: [String: Int]

    public init(metadata: CoveringIndexMetadata) {
        self.metadata = metadata

        // Build field number map using DJB2 hash
        // This matches ProtobufEncoder/Decoder's field number assignment
        var map: [String: Int] = [:]
        for fieldName in T.allFields {
            map[fieldName] = Self.deriveFieldNumber(from: fieldName)
        }
        self.fieldNumberMap = map
    }

    /// Derives a deterministic field number from a field name.
    /// Uses DJB2 hash algorithm for stability across runs.
    /// This must match ProtobufDecoder's implementation exactly.
    private static func deriveFieldNumber(from fieldName: String) -> Int {
        var hash: UInt64 = 5381
        for char in fieldName.utf8 {
            hash = ((hash << 5) &+ hash) &+ UInt64(char)  // hash * 33 + char
        }
        // Protobuf field numbers must be positive and <= 536870911 (2^29-1)
        // Reserved range 19000-19999 should be avoided
        let maxFieldNumber: UInt64 = 536870911
        let rawNumber = Int((hash % (maxFieldNumber - 20000)) + 1)
        // Skip reserved range 19000-19999
        if rawNumber >= 19000 && rawNumber <= 19999 {
            return rawNumber + 1000
        }
        return rawNumber
    }

    /// Decode an item from an index entry
    ///
    /// - Parameter entry: The index entry containing field values
    /// - Returns: The reconstructed item
    /// - Throws: DecodingError if reconstruction fails
    ///
    /// **Decoding process**:
    /// 1. Build Protobuf bytes from IndexEntry values
    /// 2. Use ProtobufDecoder to decode into T
    public func decode(from entry: IndexEntry) throws -> T {
        // Build Protobuf wire format bytes from index entry
        let protobufData = try buildProtobufData(from: entry)

        // Decode using ProtobufDecoder
        let decoder = ProtobufDecoder()
        return try decoder.decode(T.self, from: protobufData)
    }

    /// Check if this decoder can fully decode records
    public var canFullyDecode: Bool {
        metadata.isFullyCovering
    }

    // MARK: - Protobuf Encoding

    /// Build Protobuf wire format data from IndexEntry
    private func buildProtobufData(from entry: IndexEntry) throws -> Data {
        var data = Data()

        // 1. Encode ID field
        if let fieldNumber = fieldNumberMap["id"], let idValue = entry.itemID[0] {
            data.append(contentsOf: encodeField(fieldNumber: fieldNumber, value: idValue))
        }

        // 2. Encode key field values
        for (index, fieldName) in metadata.keyFields.enumerated() {
            if let fieldNumber = fieldNumberMap[fieldName], let value = entry.keyValues[index] {
                data.append(contentsOf: encodeField(fieldNumber: fieldNumber, value: value))
            }
        }

        // 3. Encode stored field values
        for (index, fieldName) in metadata.storedFields.enumerated() {
            if let fieldNumber = fieldNumberMap[fieldName], let value = entry.storedValues[index] {
                data.append(contentsOf: encodeField(fieldNumber: fieldNumber, value: value))
            }
        }

        return data
    }

    /// Encode a single field to Protobuf wire format
    private func encodeField(fieldNumber: Int, value: Any) -> [UInt8] {
        switch value {
        case let s as String:
            return encodeString(fieldNumber: fieldNumber, value: s)
        case let i as Int64:
            return encodeVarint(fieldNumber: fieldNumber, value: UInt64(bitPattern: i))
        case let i as Int:
            return encodeVarint(fieldNumber: fieldNumber, value: UInt64(bitPattern: Int64(i)))
        case let i as Int32:
            return encodeVarint(fieldNumber: fieldNumber, value: UInt64(bitPattern: Int64(i)))
        case let u as UInt64:
            return encodeVarint(fieldNumber: fieldNumber, value: u)
        case let u as UInt:
            return encodeVarint(fieldNumber: fieldNumber, value: UInt64(u))
        case let d as Double:
            return encodeDouble(fieldNumber: fieldNumber, value: d)
        case let f as Float:
            return encodeFloat(fieldNumber: fieldNumber, value: f)
        case let b as Bool:
            return encodeVarint(fieldNumber: fieldNumber, value: b ? 1 : 0)
        case let bytes as Data:
            return encodeBytes(fieldNumber: fieldNumber, value: bytes)
        case let bytes as [UInt8]:
            return encodeBytes(fieldNumber: fieldNumber, value: Data(bytes))
        default:
            // Unknown type - skip
            return []
        }
    }

    // MARK: - Wire Format Encoders

    /// Encode a string field (wire type 2: length-delimited)
    private func encodeString(fieldNumber: Int, value: String) -> [UInt8] {
        let tag = (fieldNumber << 3) | 2  // Length-delimited
        var bytes = encodeVarintValue(UInt64(tag))
        let stringData = value.data(using: .utf8) ?? Data()
        bytes.append(contentsOf: encodeVarintValue(UInt64(stringData.count)))
        bytes.append(contentsOf: stringData)
        return bytes
    }

    /// Encode a varint field (wire type 0)
    private func encodeVarint(fieldNumber: Int, value: UInt64) -> [UInt8] {
        let tag = (fieldNumber << 3) | 0  // Varint
        var bytes = encodeVarintValue(UInt64(tag))
        bytes.append(contentsOf: encodeVarintValue(value))
        return bytes
    }

    /// Encode a double field (wire type 1: 64-bit)
    private func encodeDouble(fieldNumber: Int, value: Double) -> [UInt8] {
        let tag = (fieldNumber << 3) | 1  // 64-bit
        var bytes = encodeVarintValue(UInt64(tag))
        let bits = value.bitPattern
        bytes.append(UInt8(truncatingIfNeeded: bits))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 8))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 16))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 24))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 32))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 40))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 48))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 56))
        return bytes
    }

    /// Encode a float field (wire type 5: 32-bit)
    private func encodeFloat(fieldNumber: Int, value: Float) -> [UInt8] {
        let tag = (fieldNumber << 3) | 5  // 32-bit
        var bytes = encodeVarintValue(UInt64(tag))
        let bits = value.bitPattern
        bytes.append(UInt8(truncatingIfNeeded: bits))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 8))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 16))
        bytes.append(UInt8(truncatingIfNeeded: bits >> 24))
        return bytes
    }

    /// Encode a bytes field (wire type 2: length-delimited)
    private func encodeBytes(fieldNumber: Int, value: Data) -> [UInt8] {
        let tag = (fieldNumber << 3) | 2  // Length-delimited
        var bytes = encodeVarintValue(UInt64(tag))
        bytes.append(contentsOf: encodeVarintValue(UInt64(value.count)))
        bytes.append(contentsOf: value)
        return bytes
    }

    /// Encode a UInt64 as varint
    private func encodeVarintValue(_ value: UInt64) -> [UInt8] {
        var result: [UInt8] = []
        var n = value
        while n >= 0x80 {
            result.append(UInt8(n & 0x7F) | 0x80)
            n >>= 7
        }
        result.append(UInt8(n))
        return result
    }
}

// MARK: - Index-Only Scan Analyzer

/// Analyzer for index-only scan opportunities
///
/// **True Index-Only Scan**:
/// Only possible when an index contains ALL fields of the record type T,
/// allowing complete record reconstruction without fetching from storage.
///
/// **Requirements for True Index-Only**:
/// - Index key fields + stored fields must cover all fields in T.allFields
/// - ID is always available (it's part of the index key)
///
/// **Example**:
/// ```swift
/// // Model with fields: id, email, name, createdAt
/// // Index on (email, name, createdAt) with id in key
/// // → Can use true index-only scan
///
/// // Index on (email) only
/// // → Cannot use index-only (missing name, createdAt)
/// // → Falls back to index scan + record fetch
/// ```
///
/// ## Current Limitation: Full Model Coverage Required
///
/// The current implementation requires the index to cover ALL fields of T,
/// because the API returns complete records `[T]`. This means even if a query
/// only uses certain fields (e.g., `SELECT email, name FROM users`), we still
/// need all fields to construct the full `T` instance.
///
/// ## Future Enhancement: Partial Coverage with Projection
///
/// To support partial coverage + projection (where only query-referenced fields
/// need to be covered), the following changes would be needed:
///
/// 1. **Query Projection API**: Add `select(_:)` method to Query
///    ```swift
///    query.select(\.email, \.name)  // Only need these fields
///    ```
///
/// 2. **Analyzer Check**: Check only projected fields instead of all fields
///    ```swift
///    let requiredFields = query.projectedFields ?? T.allFields
///    let canUseIndexOnly = requiredFields.isSubset(of: indexFields)
///    ```
///
/// 3. **Return Type**: Change to `PartialResult<T>` or dictionary
///    ```swift
///    func execute(plan: QueryPlan<T>) async throws -> [PartialResult<T>]
///    ```
///
/// This enhancement would enable index-only scans for a wider range of queries,
/// improving performance when the full record isn't needed.
public struct IndexOnlyScanAnalyzer<T: Persistable> {

    public init() {}

    /// Analyze if a query can use true index-only scan
    ///
    /// Returns analysis result indicating whether the index fully covers
    /// the record type T, enabling record reconstruction from index data.
    public func analyze(
        query: Query<T>,
        analysis: QueryAnalysis<T>,
        index: IndexDescriptor
    ) -> IndexOnlyScanResult {
        let metadata = CoveringIndexMetadata.build(for: index, type: T.self)

        // For true index-only, we need ALL fields of T, not just query fields
        let allModelFields = Set(T.allFields)
        let indexFields = metadata.allFields

        let coveredFields = allModelFields.intersection(indexFields)
        let uncoveredFields = allModelFields.subtracting(indexFields)

        // True index-only scan is only possible when all fields are covered
        let canUseIndexOnlyScan = metadata.isFullyCovering

        return IndexOnlyScanResult(
            canUseIndexOnlyScan: canUseIndexOnlyScan,
            index: index,
            metadata: metadata,
            coveredFields: coveredFields,
            uncoveredFields: uncoveredFields,
            estimatedSavings: canUseIndexOnlyScan ? estimateSavings(analysis: analysis) : 0
        )
    }

    /// Estimate cost savings from index-only scan
    private func estimateSavings(analysis: QueryAnalysis<T>) -> Double {
        // True index-only eliminates all record fetches
        // Savings are significant: typically 80-95% I/O reduction
        0.90
    }
}

// MARK: - Result

/// Result of index-only scan analysis
public struct IndexOnlyScanResult: Sendable {
    /// Whether true index-only scan is possible (no record fetch needed)
    public let canUseIndexOnlyScan: Bool

    /// The index being analyzed
    public let index: IndexDescriptor

    /// Covering index metadata for decoding
    public let metadata: CoveringIndexMetadata

    /// Fields covered by the index
    public let coveredFields: Set<String>

    /// Fields not covered (prevents true index-only scan)
    public let uncoveredFields: Set<String>

    /// Estimated cost savings (0.0 - 1.0)
    public let estimatedSavings: Double
}

// MARK: - Index-Only Scan Operator

/// Operator for index-only scan execution
///
/// When executed, this operator scans the index and reconstructs
/// records directly from index data without fetching from storage.
public struct IndexOnlyScanOperator<T: Persistable>: @unchecked Sendable {
    /// The index to scan
    public let index: IndexDescriptor

    /// Covering index metadata for decoding
    public let metadata: CoveringIndexMetadata

    /// Scan bounds
    public let bounds: IndexScanBounds

    /// Whether to scan in reverse
    public let reverse: Bool

    /// Fields to extract from index
    public let projectedFields: Set<String>

    /// Conditions satisfied by this scan
    public let satisfiedConditions: [any FieldConditionProtocol<T>]

    /// Estimated matching entries
    public let estimatedEntries: Int

    /// Maximum number of entries to return (pushed down from LIMIT)
    /// When set, the scan will stop early after fetching this many entries.
    public let limit: Int?

    public init(
        index: IndexDescriptor,
        metadata: CoveringIndexMetadata,
        bounds: IndexScanBounds,
        reverse: Bool = false,
        projectedFields: Set<String>,
        satisfiedConditions: [any FieldConditionProtocol<T>] = [],
        estimatedEntries: Int,
        limit: Int? = nil
    ) {
        self.index = index
        self.metadata = metadata
        self.bounds = bounds
        self.reverse = reverse
        self.projectedFields = projectedFields
        self.satisfiedConditions = satisfiedConditions
        self.estimatedEntries = estimatedEntries
        self.limit = limit
    }
}

// MARK: - Covering Index Suggestion

/// Suggests covering indexes for queries
public struct CoveringIndexSuggester<T: Persistable> {

    public init() {}

    /// Suggest a covering index for a query
    public func suggest(
        query: Query<T>,
        analysis: QueryAnalysis<T>,
        existingIndexes: [IndexDescriptor]
    ) -> CoveringIndexSuggestion? {
        let allModelFields = Set(T.allFields)

        // Check if any existing index covers all fields
        for index in existingIndexes {
            let metadata = CoveringIndexMetadata.build(for: index, type: T.self)
            if metadata.isFullyCovering {
                return nil // Already have a covering index
            }
        }

        // Find the best index to extend
        var bestCandidate: (index: IndexDescriptor, missingFields: Set<String>)?

        for index in existingIndexes {
            let metadata = CoveringIndexMetadata.build(for: index, type: T.self)
            let missing = allModelFields.subtracting(metadata.allFields)

            // Check if this index is usable for the query conditions
            let conditionFields = Set(analysis.fieldConditions.map { $0.fieldName })
            let indexKeyFields = Set(metadata.keyFields)

            // Index must have at least one condition field as key
            guard !conditionFields.isDisjoint(with: indexKeyFields) else { continue }

            if bestCandidate == nil || missing.count < bestCandidate!.missingFields.count {
                bestCandidate = (index, missing)
            }
        }

        guard let candidate = bestCandidate else {
            // Suggest a new index with all fields
            return CoveringIndexSuggestion(
                type: .newIndex,
                indexName: nil,
                keyFields: Array(Set(analysis.fieldConditions.map { $0.fieldName })),
                storedFields: Array(allModelFields),
                reason: "Create new covering index with all fields"
            )
        }

        if candidate.missingFields.isEmpty {
            return nil
        }

        return CoveringIndexSuggestion(
            type: .extendExisting,
            indexName: candidate.index.name,
            keyFields: CoveringIndexMetadata.build(for: candidate.index, type: T.self).keyFields,
            storedFields: Array(candidate.missingFields),
            reason: "Add stored fields: \(candidate.missingFields.sorted().joined(separator: ", "))"
        )
    }
}

/// Suggestion for a covering index
public struct CoveringIndexSuggestion: Sendable {
    public enum SuggestionType: Sendable {
        case newIndex
        case extendExisting
    }

    public let type: SuggestionType
    public let indexName: String?
    public let keyFields: [String]
    public let storedFields: [String]
    public let reason: String
}

// MARK: - Cost Model Extension

extension CostModel {
    /// Calculate cost savings from true index-only scan
    ///
    /// True index-only eliminates ALL record fetches, providing
    /// maximum I/O savings.
    public func indexOnlySavings(records: Double) -> Double {
        records * recordFetchWeight
    }
}

// MARK: - Query Extension

extension Query {
    /// Projected fields (nil means all fields)
    var projectedFields: Set<String>? {
        nil
    }
}

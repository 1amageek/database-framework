// ContinuationState.swift
// DatabaseEngine - Internal state management for continuation tokens
//
// Reference: FDB Record Layer continuation serialization

import Foundation
import FoundationDB

// MARK: - ContinuationState

/// Internal state for continuation serialization
///
/// This struct holds all state needed to resume a query.
/// Serialized to/from ContinuationToken using Tuple encoding.
///
/// **Serialization Format**:
/// ```
/// Tuple(
///     version: Int64,
///     scanType: Int64,
///     lastKey: Bytes,
///     reverse: Bool,
///     remainingLimit: Int64,    // -1 = unlimited
///     originalLimit: Int64,     // -1 = unlimited
///     planFingerprint: Bytes,
///     [operatorState: Bytes]    // optional
/// )
/// ```
internal struct ContinuationState: Sendable {
    /// Token format version
    let version: UInt8

    /// Type of scan being continued
    let scanType: ScanType

    /// Last processed key (packed Tuple bytes)
    ///
    /// For index scans, this is the last index entry key.
    /// For table scans, this is the last primary key.
    let lastKey: [UInt8]

    /// Scan direction
    let reverse: Bool

    /// Remaining limit (nil = unlimited)
    ///
    /// Decremented as results are returned.
    let remainingLimit: Int?

    /// Original limit (for computing progress)
    let originalLimit: Int?

    /// Query plan fingerprint (for validation)
    ///
    /// Hash of the query structure to ensure token matches query.
    let planFingerprint: [UInt8]

    /// Operator-specific state (for complex plans like union)
    let operatorState: OperatorContinuationState?

    // MARK: - Scan Types

    /// Scan type enumeration
    enum ScanType: UInt8, Sendable {
        case tableScan = 0
        case indexScan = 1
        case indexSeek = 2
        case indexOnlyScan = 3
        case fullTextScan = 4
        case vectorSearch = 5
        case spatialScan = 6
        case union = 7
        case intersection = 8
        case rankScan = 9

        var name: String {
            switch self {
            case .tableScan: return "tableScan"
            case .indexScan: return "indexScan"
            case .indexSeek: return "indexSeek"
            case .indexOnlyScan: return "indexOnlyScan"
            case .fullTextScan: return "fullTextScan"
            case .vectorSearch: return "vectorSearch"
            case .spatialScan: return "spatialScan"
            case .union: return "union"
            case .intersection: return "intersection"
            case .rankScan: return "rankScan"
            }
        }
    }

    // MARK: - Initialization

    init(
        version: UInt8 = ContinuationToken.currentVersion,
        scanType: ScanType,
        lastKey: [UInt8],
        reverse: Bool = false,
        remainingLimit: Int? = nil,
        originalLimit: Int? = nil,
        planFingerprint: [UInt8],
        operatorState: OperatorContinuationState? = nil
    ) {
        self.version = version
        self.scanType = scanType
        self.lastKey = lastKey
        self.reverse = reverse
        self.remainingLimit = remainingLimit
        self.originalLimit = originalLimit
        self.planFingerprint = planFingerprint
        self.operatorState = operatorState
    }

    // MARK: - Serialization

    /// Serialize to ContinuationToken
    func toToken() -> ContinuationToken {
        var elements: [any TupleElement] = [
            Int64(version),
            Int64(scanType.rawValue),
            lastKey,
            reverse,
            Int64(remainingLimit ?? -1),  // -1 as sentinel for unlimited
            Int64(originalLimit ?? -1),
            planFingerprint
        ]

        if let opState = operatorState {
            elements.append(opState.serialize())
        }

        let tuple = Tuple(elements)
        return ContinuationToken(data: tuple.pack())
    }

    /// Deserialize from ContinuationToken
    static func fromToken(_ token: ContinuationToken) throws -> ContinuationState {
        guard !token.isEndOfResults else {
            throw ContinuationError.invalidTokenFormat
        }

        let tuple = try Tuple.unpack(from: token.data)

        guard tuple.count >= 7 else {
            throw ContinuationError.corruptedToken
        }

        // Helper to extract integer from various int types that Tuple might return
        func extractInt(_ value: Any) -> Int64? {
            if let v = value as? Int64 { return v }
            if let v = value as? Int { return Int64(v) }
            if let v = value as? Int32 { return Int64(v) }
            if let v = value as? UInt64 { return Int64(v) }
            return nil
        }

        // Helper to extract byte array (handles empty arrays)
        func extractBytes(_ value: Any) -> [UInt8]? {
            if let v = value as? [UInt8] { return v }
            if let v = value as? Data { return Array(v) }
            // Empty tuple element might come back as nil or empty
            return []
        }

        guard let versionInt = extractInt(tuple[0]),
              let scanTypeInt = extractInt(tuple[1]),
              let lastKeyBytes = extractBytes(tuple[2]),
              let reverseFlag = tuple[3] as? Bool,
              let remainingLimitInt = extractInt(tuple[4]),
              let originalLimitInt = extractInt(tuple[5]) else {
            throw ContinuationError.corruptedToken
        }

        // Fingerprint can be empty
        let fingerprintBytes = extractBytes(tuple[6]) ?? []

        let version = UInt8(versionInt)
        guard version == ContinuationToken.currentVersion else {
            throw ContinuationError.versionMismatch(
                expected: ContinuationToken.currentVersion,
                actual: version
            )
        }

        guard let scanType = ScanType(rawValue: UInt8(scanTypeInt)) else {
            throw ContinuationError.corruptedToken
        }

        let remainingLimit = remainingLimitInt >= 0 ? Int(remainingLimitInt) : nil
        let originalLimit = originalLimitInt >= 0 ? Int(originalLimitInt) : nil

        var operatorState: OperatorContinuationState? = nil
        if tuple.count > 7 {
            if let stateBytes = extractBytes(tuple[7]), !stateBytes.isEmpty {
                operatorState = try OperatorContinuationState.deserialize(stateBytes)
            }
        }

        return ContinuationState(
            version: version,
            scanType: scanType,
            lastKey: lastKeyBytes,
            reverse: reverseFlag,
            remainingLimit: remainingLimit,
            originalLimit: originalLimit,
            planFingerprint: fingerprintBytes,
            operatorState: operatorState
        )
    }

    // MARK: - Progress

    /// Calculate progress percentage (0.0 - 1.0)
    var progress: Double? {
        guard let original = originalLimit, let remaining = remainingLimit, original > 0 else {
            return nil
        }
        return Double(original - remaining) / Double(original)
    }
}

// MARK: - OperatorContinuationState

/// Operator-specific continuation state
///
/// Used for complex operators like union/intersection that need to track
/// state across multiple child streams.
internal struct OperatorContinuationState: Sendable {
    /// For union: which child index we're currently on
    let unionChildIndex: Int?

    /// For union: continuation of current child
    let childContinuation: [UInt8]?

    /// For union: exhausted child indices
    let exhaustedChildren: [Int]?

    /// For intersection: accumulated IDs from previous children
    let intersectionIds: [[UInt8]]?

    init(
        unionChildIndex: Int? = nil,
        childContinuation: [UInt8]? = nil,
        exhaustedChildren: [Int]? = nil,
        intersectionIds: [[UInt8]]? = nil
    ) {
        self.unionChildIndex = unionChildIndex
        self.childContinuation = childContinuation
        self.exhaustedChildren = exhaustedChildren
        self.intersectionIds = intersectionIds
    }

    /// Serialize to bytes using Tuple encoding
    func serialize() -> [UInt8] {
        var elements: [any TupleElement] = [
            Int64(unionChildIndex ?? -1),
            childContinuation ?? [UInt8]()
        ]

        // Encode exhausted children as additional indices
        if let exhausted = exhaustedChildren, !exhausted.isEmpty {
            for idx in exhausted {
                elements.append(Int64(idx))
            }
        }

        // Note: intersectionIds encoding is more complex and can be added if needed

        let tuple = Tuple(elements)
        return tuple.pack()
    }

    /// Deserialize from bytes
    static func deserialize(_ bytes: [UInt8]) throws -> OperatorContinuationState {
        guard !bytes.isEmpty else {
            return OperatorContinuationState()
        }

        let tuple = try Tuple.unpack(from: bytes)

        guard tuple.count >= 2 else {
            throw ContinuationError.corruptedToken
        }

        // Helper to extract integer from various int types that Tuple might return
        func extractInt(_ value: Any) -> Int64? {
            if let v = value as? Int64 { return v }
            if let v = value as? Int { return Int64(v) }
            if let v = value as? Int32 { return Int64(v) }
            if let v = value as? UInt64 { return Int64(v) }
            return nil
        }

        let unionChildIndex: Int?
        if let idx = extractInt(tuple[0]), idx >= 0 {
            unionChildIndex = Int(idx)
        } else {
            unionChildIndex = nil
        }

        let childContinuation: [UInt8]?
        if let cont = tuple[1] as? [UInt8], !cont.isEmpty {
            childContinuation = cont
        } else {
            childContinuation = nil
        }

        // Decode exhausted children
        var exhaustedChildren: [Int]? = nil
        if tuple.count > 2 {
            var exhausted: [Int] = []
            for i in 2..<tuple.count {
                if let idx = extractInt(tuple[i]) {
                    exhausted.append(Int(idx))
                }
            }
            if !exhausted.isEmpty {
                exhaustedChildren = exhausted
            }
        }

        return OperatorContinuationState(
            unionChildIndex: unionChildIndex,
            childContinuation: childContinuation,
            exhaustedChildren: exhaustedChildren,
            intersectionIds: nil
        )
    }
}

// MARK: - Plan Fingerprint

/// Utility for computing plan fingerprints
internal struct PlanFingerprint {
    /// Compute fingerprint from plan components
    ///
    /// The fingerprint captures:
    /// - Operator types
    /// - Index names used
    /// - Sort order
    /// - Filter structure (not values)
    static func compute(
        operatorDescription: String,
        indexNames: [String],
        sortFields: [String]
    ) -> [UInt8] {
        var hasher = Hasher()
        hasher.combine(operatorDescription)
        for name in indexNames.sorted() {
            hasher.combine(name)
        }
        for field in sortFields {
            hasher.combine(field)
        }
        let hash = hasher.finalize()
        return withUnsafeBytes(of: hash) { Array($0) }
    }
}

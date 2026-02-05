/// AnyIndexDescriptor.swift
/// Type-erased IndexDescriptor and IndexKind for catalog persistence
///
/// **Design Principles**:
/// 1. Separate IndexKind (AnyIndexKind) from IndexDescriptor (AnyIndexDescriptor)
/// 2. IndexKind-specific metadata in AnyIndexKind
/// 3. CommonIndexOptions metadata in AnyIndexDescriptor
/// 4. Adding new IndexKind types requires no changes (Open-Closed Principle)
///
/// **Usage**:
/// - TypeCatalog stores `[AnyIndexDescriptor]` for index metadata
/// - SchemaRegistry persists TypeCatalog as JSON (requires Codable)
/// - CLI uses AnyIndexDescriptor to inspect index configurations

import Core
import Foundation

// MARK: - AnyIndexKind

/// Type-erased IndexKind
///
/// Contains IndexKind protocol requirements and kind-specific metadata.
/// - `identifier`: IndexKind.identifier
/// - `subspaceStructure`: IndexKind.subspaceStructure
/// - `fieldNames`: IndexKind.fieldNames
/// - `metadata`: Kind-specific properties (dimensions, metric, strategy, etc.)
public struct AnyIndexKind: Sendable, Hashable, Codable {

    /// Index kind identifier (e.g., "scalar", "vector", "com.mycompany.bloom_filter")
    public let identifier: String

    /// Subspace structure for index storage
    public let subspaceStructure: SubspaceStructure

    /// Field names for indexed KeyPaths
    public let fieldNames: [String]

    /// Kind-specific metadata:
    /// - Vector: "dimensions", "metric"
    /// - Graph: "fromField", "edgeField", "toField", "graphField", "strategy"
    /// - FullText: "tokenizer", "storePositions", "ngramSize", "minTermLength"
    /// - Spatial: "encoding", "level"
    /// - Rank: "scoreTypeName", "bucketSize"
    /// - etc.
    public let metadata: [String: IndexMetadataValue]

    // MARK: - Init from IndexKind

    public init(_ kind: any IndexKind) {
        self.identifier = type(of: kind).identifier
        self.subspaceStructure = type(of: kind).subspaceStructure
        self.fieldNames = kind.fieldNames
        self.metadata = Self.extractMetadata(from: kind)
    }

    // MARK: - Init for Codable reconstruction

    public init(
        identifier: String,
        subspaceStructure: SubspaceStructure,
        fieldNames: [String],
        metadata: [String: IndexMetadataValue]
    ) {
        self.identifier = identifier
        self.subspaceStructure = subspaceStructure
        self.fieldNames = fieldNames
        self.metadata = metadata
    }

    // MARK: - Metadata Extraction

    private static func extractMetadata(from kind: any IndexKind) -> [String: IndexMetadataValue] {
        do {
            let data = try JSONEncoder().encode(kind)
            let jsonObject = try JSONSerialization.jsonObject(with: data)
            guard let dict = jsonObject as? [String: Any] else { return [:] }
            // Filter out fieldNames (already a direct property)
            return dict.compactMapValues { IndexMetadataValue(from: $0) }
                .filter { $0.key != "fieldNames" }
        } catch {
            return [:]
        }
    }
}

// MARK: - AnyIndexDescriptor

/// Type-erased IndexDescriptor
///
/// Combines AnyIndexKind with CommonIndexOptions metadata.
/// - `name`: Index identifier
/// - `kind`: Type-erased IndexKind
/// - `commonMetadata`: CommonIndexOptions (unique, sparse, storedFieldNames, userMetadata.*)
///
/// Replaces the previous `IndexCatalog` struct with a unified representation
/// that preserves full type information from `IndexDescriptor`.
public struct AnyIndexDescriptor: Sendable, Hashable, Codable {

    /// Index name (unique identifier)
    public let name: String

    /// Type-erased IndexKind
    public let kind: AnyIndexKind

    /// CommonIndexOptions metadata:
    /// - "unique": Bool - Uniqueness constraint
    /// - "sparse": Bool - Sparse index
    /// - "storedFieldNames": [String] - Covering index fields
    /// - "userMetadata.*": User-defined metadata
    public let commonMetadata: [String: IndexMetadataValue]

    // MARK: - Init from IndexDescriptor

    public init(_ descriptor: IndexDescriptor) {
        self.name = descriptor.name
        self.kind = AnyIndexKind(descriptor.kind)
        self.commonMetadata = Self.extractCommonMetadata(from: descriptor)
    }

    // MARK: - Init for Codable reconstruction

    public init(
        name: String,
        kind: AnyIndexKind,
        commonMetadata: [String: IndexMetadataValue]
    ) {
        self.name = name
        self.kind = kind
        self.commonMetadata = commonMetadata
    }

    // MARK: - Convenience Accessors (Kind shortcuts)

    /// Index kind identifier (shortcut for kind.identifier)
    public var kindIdentifier: String {
        kind.identifier
    }

    /// Field names (shortcut for kind.fieldNames)
    public var fieldNames: [String] {
        kind.fieldNames
    }

    /// Subspace structure (shortcut for kind.subspaceStructure)
    public var subspaceStructure: SubspaceStructure {
        kind.subspaceStructure
    }

    // MARK: - Convenience Accessors (CommonOptions)

    /// Uniqueness constraint (convenience accessor)
    public var unique: Bool {
        commonMetadata["unique"]?.boolValue ?? false
    }

    /// Sparse index flag (convenience accessor)
    public var sparse: Bool {
        commonMetadata["sparse"]?.boolValue ?? false
    }

    /// Stored field names for covering index (convenience accessor)
    public var storedFieldNames: [String] {
        commonMetadata["storedFieldNames"]?.stringArrayValue ?? []
    }

    // MARK: - Metadata Extraction

    private static func extractCommonMetadata(from descriptor: IndexDescriptor) -> [String: IndexMetadataValue] {
        var result: [String: IndexMetadataValue] = [:]

        // CommonIndexOptions
        result["unique"] = .bool(descriptor.commonOptions.unique)
        result["sparse"] = .bool(descriptor.commonOptions.sparse)

        // storedFieldNames
        if !descriptor.storedFieldNames.isEmpty {
            result["storedFieldNames"] = .stringArray(descriptor.storedFieldNames)
        }

        // User-defined metadata (with prefix to avoid conflicts)
        for (key, value) in descriptor.commonOptions.metadata {
            if let converted = IndexMetadataValue(from: value) {
                result["userMetadata.\(key)"] = converted
            }
        }

        return result
    }
}

// MARK: - IndexMetadataValue

/// Sendable, Hashable, and Codable metadata value
///
/// Supports common types that can appear in IndexKind Codable representations.
/// Used for both IndexKind-specific metadata (e.g., dimensions, metric)
/// and CommonIndexOptions (e.g., unique, sparse, storedFieldNames).
public enum IndexMetadataValue: Sendable, Hashable, Codable {
    case string(String)
    case int(Int)
    case double(Double)
    case bool(Bool)
    case stringArray([String])
    case intArray([Int])

    // MARK: - Init from Any

    public init?(from any: Any) {
        switch any {
        case let s as String:
            self = .string(s)
        case let i as Int:
            self = .int(i)
        case let d as Double:
            self = .double(d)
        case let b as Bool:
            self = .bool(b)
        case let arr as [String]:
            self = .stringArray(arr)
        case let arr as [Int]:
            self = .intArray(arr)
        default:
            return nil
        }
    }

    // MARK: - Codable

    private enum CodingKeys: String, CodingKey {
        case type, value
    }

    private enum ValueType: String, Codable {
        case string, int, double, bool, stringArray, intArray
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(ValueType.self, forKey: .type)

        switch type {
        case .string:
            let value = try container.decode(String.self, forKey: .value)
            self = .string(value)
        case .int:
            let value = try container.decode(Int.self, forKey: .value)
            self = .int(value)
        case .double:
            let value = try container.decode(Double.self, forKey: .value)
            self = .double(value)
        case .bool:
            let value = try container.decode(Bool.self, forKey: .value)
            self = .bool(value)
        case .stringArray:
            let value = try container.decode([String].self, forKey: .value)
            self = .stringArray(value)
        case .intArray:
            let value = try container.decode([Int].self, forKey: .value)
            self = .intArray(value)
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case .string(let value):
            try container.encode(ValueType.string, forKey: .type)
            try container.encode(value, forKey: .value)
        case .int(let value):
            try container.encode(ValueType.int, forKey: .type)
            try container.encode(value, forKey: .value)
        case .double(let value):
            try container.encode(ValueType.double, forKey: .type)
            try container.encode(value, forKey: .value)
        case .bool(let value):
            try container.encode(ValueType.bool, forKey: .type)
            try container.encode(value, forKey: .value)
        case .stringArray(let value):
            try container.encode(ValueType.stringArray, forKey: .type)
            try container.encode(value, forKey: .value)
        case .intArray(let value):
            try container.encode(ValueType.intArray, forKey: .type)
            try container.encode(value, forKey: .value)
        }
    }

    // MARK: - Value Accessors

    public var stringValue: String? {
        if case .string(let v) = self { return v }
        return nil
    }

    public var intValue: Int? {
        if case .int(let v) = self { return v }
        return nil
    }

    public var doubleValue: Double? {
        if case .double(let v) = self { return v }
        return nil
    }

    public var boolValue: Bool? {
        if case .bool(let v) = self { return v }
        return nil
    }

    public var stringArrayValue: [String]? {
        if case .stringArray(let v) = self { return v }
        return nil
    }

    public var intArrayValue: [Int]? {
        if case .intArray(let v) = self { return v }
        return nil
    }
}

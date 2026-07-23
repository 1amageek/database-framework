import Core
import DatabaseValue
import DatabaseWire
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit

/// Builds and reads canonical field projections stored in index values.
public enum CoveringValueBuilder {
    public static let formatVersion: UInt16 = 1

    private static let magic: [UInt8] = [0x44, 0x42, 0x49, 0x58]
    private static func wireLimits() throws -> DatabaseWireLimits {
        try DatabaseWireLimits(
            maximumFrameBytes: databaseMaximumValueSize,
            maximumStringBytes: databaseMaximumValueSize,
            maximumByteStringBytes: databaseMaximumValueSize,
            maximumCollectionCount: 10_000,
            maximumNestingDepth: 64,
            maximumObjectCount: 100_000
        )
    }

    /// Builds a canonical projection for an index entry.
    ///
    /// Non-covering indexes without stored fields retain an empty value. A fully
    /// covering index always stores a DBIX frame, including key-only indexes, so
    /// decoding never has to infer entity values from tuple encodings.
    public static func build<Item: Persistable>(
        for item: Item,
        index: Index
    ) throws -> Bytes {
        let schemas = try validatedSchemas(for: Item.self)
        let modelFields = Set(schemas.map(\.name))
        let requestedPaths = ["id"] + index.kind.fieldNames + index.storedFieldNames
        let projectedNames = orderedUnique(requestedPaths.map(rootFieldName))

        for field in projectedNames where !modelFields.contains(field) {
            throw CanonicalIndexProjectionError.unknownField(
                entity: Item.persistableType,
                index: index.name,
                field: field
            )
        }

        let fullyCovering = modelFields.isSubset(of: Set(projectedNames))
        guard !index.storedFieldNames.isEmpty || fullyCovering else {
            return []
        }

        let encodedFields = try PersistableFieldEncoder.encode(item)
        let fieldsByName = Dictionary(
            uniqueKeysWithValues: encodedFields.map { ($0.name, $0) }
        )
        let selectedFields = try projectedNames.map { fieldName in
            guard let field = fieldsByName[fieldName] else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: Item.persistableType,
                    reason: "encoded field '\(fieldName)' is missing"
                )
            }
            return field
        }.sorted { $0.number < $1.number }

        let bytes = try PersistableFieldFrameCodec.encode(
            magic: magic,
            version: formatVersion,
            entity: Item.persistableType,
            fields: selectedFields,
            limits: try wireLimits()
        )
        try validateValueSize(bytes)
        return bytes
    }

    /// Decodes the stored-field portion for graph and specialized index readers.
    public static func decode(
        _ bytes: Bytes,
        storedFieldNames: [String]
    ) throws -> [String: DatabaseValue] {
        guard !storedFieldNames.isEmpty else { return [:] }
        guard !bytes.isEmpty else {
            throw CanonicalIndexProjectionError.missingProjection(index: "unknown")
        }

        let rootFieldNames = Set(storedFieldNames.map(rootFieldName))
        let frame = try PersistableFieldFrameCodec.decodeSelected(
            bytes,
            magic: magic,
            version: formatVersion,
            selectedFieldNames: rootFieldNames,
            limits: try wireLimits()
        )
        var properties: [String: DatabaseValue] = [:]
        properties.reserveCapacity(storedFieldNames.count)
        for fieldPath in storedFieldNames {
            guard let value = value(at: fieldPath, in: frame.fieldsByName) else {
                throw CanonicalIndexProjectionError.projectionFieldMismatch(
                    entity: frame.entity,
                    missingFields: [fieldPath],
                    unexpectedFields: []
                )
            }
            properties[fieldPath] = value
        }
        return properties
    }

    package static func decodeFields(
        _ bytes: Bytes,
        expectedEntity: String
    ) throws -> [PersistableField] {
        guard !bytes.isEmpty else {
            throw CanonicalIndexProjectionError.missingProjection(index: "unknown")
        }
        return try PersistableFieldFrameCodec.decode(
            bytes,
            magic: magic,
            version: formatVersion,
            expectedEntity: expectedEntity,
            limits: try wireLimits()
        ).fields
    }

    private static func validatedSchemas<Item: Persistable>(
        for type: Item.Type
    ) throws -> [FieldSchema] {
        guard !type.fieldSchemas.isEmpty else {
            throw CanonicalIndexProjectionError.missingCompiledSchema(
                entity: type.persistableType
            )
        }

        var names = Set<String>()
        var numbers = Set<Int>()
        for schema in type.fieldSchemas {
            guard schema.fieldNumber > 0 else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: type.persistableType,
                    reason: "field '\(schema.name)' has invalid number \(schema.fieldNumber)"
                )
            }
            guard names.insert(schema.name).inserted else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: type.persistableType,
                    reason: "field name '\(schema.name)' is duplicated"
                )
            }
            guard numbers.insert(schema.fieldNumber).inserted else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: type.persistableType,
                    reason: "field number \(schema.fieldNumber) is duplicated"
                )
            }
        }
        return type.fieldSchemas
    }

    private static func rootFieldName(_ path: String) -> String {
        String(path.split(separator: ".", maxSplits: 1).first ?? "")
    }

    private static func orderedUnique(_ values: [String]) -> [String] {
        var seen = Set<String>()
        return values.filter { seen.insert($0).inserted }
    }

    private static func value(
        at path: String,
        in fieldsByName: [String: DatabaseValue]
    ) -> DatabaseValue? {
        let components = path.split(separator: ".").map(String.init)
        guard let first = components.first,
              var current = fieldsByName[first] else {
            return nil
        }
        for component in components.dropFirst() {
            guard case .object(let fields) = current,
                  let next = fields.first(where: { $0.name == component }) else {
                return nil
            }
            current = next.value
        }
        return current
    }

}

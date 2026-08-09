import DatabaseKit
import DatabaseTypes
import StorageKit

/// Builds and reads canonical field projections stored in index values.
public enum CoveringValueBuilder {
    public static let formatVersion: UInt16 = 1

    private static let magic: [UInt8] = [0x44, 0x42, 0x49, 0x58]
    private static func storageLimits() throws -> StorageFrameLimits {
        try StorageFrameLimits(
            maximumFrameBytes: databaseMaximumValueSize,
            maximumStringBytes: databaseMaximumValueSize,
            maximumByteStringBytes: databaseMaximumValueSize,
            maximumCollectionCount: 10_000,
            maximumNestingDepth: 64
        )
    }

    /// Builds a canonical projection for an index entry.
    ///
    /// Non-covering indexes without stored fields retain an empty value. A fully
    /// covering index always stores a DBIX frame, including key-only indexes, so
    /// decoding never has to infer entity values from tuple encodings.
    public static func build<Item: PersistedEntityValue>(
        for item: Item,
        index: Index
    ) throws -> ByteString {
        let encodedFields = try validatedFields(for: item)
        let modelFields = Set(encodedFields.map { $0.name })
        let requestedPaths = ["id"] + index.kind.fieldNames + index.storedFieldNames
        let projectedNames = orderedUnique(requestedPaths.map(rootFieldName))

        for field in projectedNames where !modelFields.contains(field) {
            throw CanonicalIndexProjectionError.unknownField(
                entity: item.persistedEntityName,
                index: index.name,
                field: field
            )
        }

        let fullyCovering = modelFields.isSubset(of: Set(projectedNames))
        guard !index.storedFieldNames.isEmpty || fullyCovering else {
            return []
        }

        let fieldsByName = Dictionary(
            uniqueKeysWithValues: encodedFields.map { ($0.name, $0) }
        )
        let selectedFields = try projectedNames.map { fieldName in
            guard let field = fieldsByName[fieldName] else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: item.persistedEntityName,
                    reason: "encoded field '\(fieldName)' is missing"
                )
            }
            return field
        }.sorted { $0.number < $1.number }

        let bytes = try PersistableFieldFrameCodec.encode(
            magic: magic,
            version: formatVersion,
            entity: item.persistedEntityName,
            fields: selectedFields,
            limits: try storageLimits()
        )
        try validateValueSize(bytes)
        return bytes
    }

    /// Decodes the stored-field portion for graph and specialized index readers.
    public static func decode(
        _ bytes: ByteString,
        storedFieldNames: [String]
    ) throws -> [String: FieldValue] {
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
            limits: try storageLimits()
        )
        var properties: [String: FieldValue] = [:]
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
        _ bytes: ByteString,
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
            limits: try storageLimits()
        ).fields
    }

    private static func validatedFields<Item: PersistedEntityValue>(
        for item: Item
    ) throws -> [PersistableField] {
        let fields = try item.persistedFields()
        guard !fields.isEmpty else {
            throw CanonicalIndexProjectionError.missingCompiledSchema(
                entity: item.persistedEntityName
            )
        }

        var names = Set<String>()
        var numbers = Set<UInt32>()
        for field in fields {
            guard names.insert(field.name).inserted else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: item.persistedEntityName,
                    reason: "field name '\(field.name)' is duplicated"
                )
            }
            guard numbers.insert(field.number).inserted else {
                throw CanonicalIndexProjectionError.invalidSchema(
                    entity: item.persistedEntityName,
                    reason: "field number \(field.number) is duplicated"
                )
            }
        }
        return fields
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
        in fieldsByName: [String: FieldValue]
    ) -> FieldValue? {
        let components = path.split(separator: ".").map(String.init)
        guard let first = components.first,
              var current = fieldsByName[first] else {
            return nil
        }
        for component in components.dropFirst() {
            guard case .object(let fields) = current,
                  let next = fields[component] else {
                return nil
            }
            current = next
        }
        return current
    }

}

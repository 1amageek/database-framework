import Core

/// Decodes a complete typed entity from bounded canonical DBIX bytes.
public struct IndexEntryDecoder<T: Persistable>: Sendable {
    private let metadata: CoveringIndexMetadata

    public init(metadata: CoveringIndexMetadata) throws {
        guard !T.fieldSchemas.isEmpty else {
            throw CanonicalIndexProjectionError.missingCompiledSchema(
                entity: T.persistableType
            )
        }
        guard metadata.isFullyCovering else {
            let missing = Set(T.fieldSchemas.map(\.name))
                .subtracting(metadata.allFields)
            throw CanonicalIndexProjectionError.incompleteProjection(
                entity: T.persistableType,
                missingFields: missing.sorted()
            )
        }
        self.metadata = metadata
    }

    public func decode(from entry: IndexEntry) throws -> T {
        let fields = try CoveringValueBuilder.decodeFields(
            entry.coveringValue,
            expectedEntity: T.persistableType
        )
        let expectedNames = Set(T.fieldSchemas.map(\.name))
        let actualNames = Set(fields.map(\.name))
        guard expectedNames == actualNames else {
            throw CanonicalIndexProjectionError.projectionFieldMismatch(
                entity: T.persistableType,
                missingFields: expectedNames.subtracting(actualNames).sorted(),
                unexpectedFields: actualNames.subtracting(expectedNames).sorted()
            )
        }
        return try T.decodePersistedFields(fields)
    }
}

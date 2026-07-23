import Core

/// Validated metadata describing a canonical covering-index projection.
public struct CoveringIndexMetadata: Sendable {
    public let keyFields: [String]
    public let storedFields: [String]
    public let isFullyCovering: Bool

    public var allFields: Set<String> {
        Set((["id"] + keyFields + storedFields).map(Self.rootFieldName))
    }

    public static func build<T: Persistable>(
        for index: IndexDescriptor,
        type: T.Type
    ) -> CoveringIndexMetadata {
        let schemas = T.fieldSchemas
        let schemaNames = Set(schemas.map(\.name))
        let schemaNumbers = Set(schemas.map(\.fieldNumber))
        let schemaIsValid = !schemas.isEmpty
            && schemaNames.count == schemas.count
            && schemaNumbers.count == schemas.count
            && schemas.allSatisfy { $0.fieldNumber > 0 }
        let projectedFields = Set(
            (["id"] + index.fieldNames + index.storedFieldNames)
                .map(rootFieldName)
        )

        return CoveringIndexMetadata(
            keyFields: index.fieldNames,
            storedFields: index.storedFieldNames,
            isFullyCovering: schemaIsValid
                && projectedFields.isSubset(of: schemaNames)
                && schemaNames.isSubset(of: projectedFields)
        )
    }

    private init(
        keyFields: [String],
        storedFields: [String],
        isFullyCovering: Bool
    ) {
        self.keyFields = keyFields
        self.storedFields = storedFields
        self.isFullyCovering = isFullyCovering
    }

    private static func rootFieldName(_ path: String) -> String {
        String(path.split(separator: ".", maxSplits: 1).first ?? "")
    }
}

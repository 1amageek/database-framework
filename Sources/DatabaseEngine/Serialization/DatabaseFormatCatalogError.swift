public enum DatabaseFormatCatalogError: Error, Sendable, Equatable {
    case missingDescriptor
    case descriptorMissingInNonEmptyDatabase
    case descriptorMismatch(
        stored: DatabaseFormatDescriptor,
        expected: DatabaseFormatDescriptor
    )
}

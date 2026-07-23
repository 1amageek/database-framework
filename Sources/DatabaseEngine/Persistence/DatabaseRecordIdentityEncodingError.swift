public enum DatabaseRecordIdentityEncodingError: Error, Sendable, Equatable {
    case invalidCompiledSchema(entity: String, reason: String)
    case identifierNotRepresentable(entity: String)
}

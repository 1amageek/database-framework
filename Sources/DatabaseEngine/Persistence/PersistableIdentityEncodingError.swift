public enum PersistableIdentityEncodingError: Error, Sendable, Equatable {
    case invalidCompiledSchema(entity: String, reason: String)
    case identifierNotRepresentable(entity: String)
}

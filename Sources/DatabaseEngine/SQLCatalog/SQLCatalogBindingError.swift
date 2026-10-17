public enum SQLCatalogBindingError: Error, Sendable, Equatable {
    case missingEntity(String)
    case missingRuntime(String)
    case runtimeContractMismatch(String)
    case missingField(entity: String, number: Int)
    case missingNamespace(String)
    case missingRelation(namespace: String?, name: String)
    case physicalFingerprintMismatch
    case digestMismatch
    case invalidProtection(String)
    case protectedRelationRemoved(String)
    case relationRetargeted(String)
    case columnRetargeted(relation: String, column: String)
    case protectedColumnRemoved(relation: String, column: String)
    case protectedPrimaryKeyChanged(String)
    case primaryKeyColumnRemoved(relation: String, column: String)
    case missingCatalog
}

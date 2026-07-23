public enum SPARQLFunctionRegistryError: Error, Sendable, Equatable {
    case duplicateFunction(String)
    case unknownFunction(String)
    case nonCanonicalResult(String)
    case functionFailed(identifier: String, detail: String)
}

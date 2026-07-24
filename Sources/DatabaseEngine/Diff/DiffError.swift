/// Failures produced while computing model or version-history differences.
public enum DiffError: Error, Sendable, Hashable {
    case modelNotFoundAtVersion(id: String, version: String)
    case fieldNotFound(fieldPath: String, typeName: String)
    case conversionFailed(fieldPath: String, valueType: String)
    case typeMismatch(expected: String, actual: String)
    case invalidFieldPath(String)
    case versionHistoryNotAvailable(typeName: String)
    case insufficientVersionHistory(
        id: String,
        required: Int,
        available: Int
    )
}

extension DiffError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .modelNotFoundAtVersion(let id, let version):
            "Model '\(id)' was not found at version '\(version)'"
        case .fieldNotFound(let fieldPath, let typeName):
            "Field '\(fieldPath)' was not found in type '\(typeName)'"
        case .conversionFailed(let fieldPath, let valueType):
            "Field '\(fieldPath)' of type '\(valueType)' cannot be represented as FieldValue"
        case .typeMismatch(let expected, let actual):
            "Expected model type '\(expected)', received '\(actual)'"
        case .invalidFieldPath(let fieldPath):
            "Field path '\(fieldPath)' is invalid"
        case .versionHistoryNotAvailable(let typeName):
            "Version history is unavailable for type '\(typeName)'"
        case .insufficientVersionHistory(let id, let required, let available):
            "Model '\(id)' requires \(required) versions, but only \(available) are available"
        }
    }
}

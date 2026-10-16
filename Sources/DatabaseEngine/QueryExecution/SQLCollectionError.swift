/// Failures of selected SQL array and JSON collection operations.
///
/// Argument and path positions are one-based. Lexical offsets are zero-based
/// UTF-8 offsets and do not identify an outer expression occurrence.
public enum SQLCollectionError: Error, Sendable, Equatable, CustomStringConvertible {
    case invalidArrayShape
    case arraySubscriptOutOfRange
    case invalidArrayInput(offset: UInt64, reason: String)
    case invalidArrayArgument(function: String)
    case nullArraySubscript(function: String)
    case jsonArrayRootRequired(function: String)
    case jsonObjectRootRequired(function: String)
    case jsonContainerRootRequired(operation: String)
    case oddJSONObjectArgumentCount(actual: Int)
    case nullJSONObjectKey(argument: Int)
    case invalidJSONObjectKey(argument: Int)
    case multidimensionalJSONMutationPath(rank: Int)
    case nullJSONMutationPathElement(position: Int)
    case invalidJSONMutationPathIndex(position: Int, text: String)
    case existingJSONInsertKey(key: String)

    public var description: String {
        switch self {
        case .invalidArrayShape:
            return "SQL array shape is incompatible with the selected operation"
        case .arraySubscriptOutOfRange:
            return "SQL array subscript arithmetic is outside the supported range"
        case .invalidArrayInput(let offset, let reason):
            return "Invalid SQL array input at UTF-8 offset \(offset): \(reason)"
        case .invalidArrayArgument(let function):
            return "Invalid SQL array argument for '\(function)'"
        case .nullArraySubscript(let function):
            return "SQL array subscript for '\(function)' cannot be NULL"
        case .jsonArrayRootRequired(let function):
            return "SQL JSON function '\(function)' requires an array root"
        case .jsonObjectRootRequired(let function):
            return "SQL JSON function '\(function)' requires an object root"
        case .jsonContainerRootRequired(let operation):
            return "SQL JSON operation '\(operation)' requires a container root"
        case .oddJSONObjectArgumentCount(let actual):
            return "SQL JSON object construction requires paired arguments; received \(actual)"
        case .nullJSONObjectKey(let argument):
            return "SQL JSON object key at argument \(argument) cannot be NULL"
        case .invalidJSONObjectKey(let argument):
            return "SQL JSON object key at argument \(argument) is invalid"
        case .multidimensionalJSONMutationPath(let rank):
            return "SQL JSON mutation requires a one-dimensional path; received rank \(rank)"
        case .nullJSONMutationPathElement(let position):
            return "SQL JSON mutation path element \(position) cannot be NULL"
        case .invalidJSONMutationPathIndex(let position, let text):
            return "SQL JSON mutation path element \(position) is not a valid index: '\(text)'"
        case .existingJSONInsertKey(let key):
            return "SQL JSON insertion key '\(key)' already exists"
        }
    }
}

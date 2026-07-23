import DatabaseWire

enum SchemaEntityRecordCodecError: Error, Sendable, CustomStringConvertible {
    case invalidMagic(UInt32)
    case unsupportedVersion(UInt16)
    case invalidInteger(field: String, value: Int64)
    case invalidEnum(type: String, value: String)
    case invalidMetadataTag(UInt8)
    case duplicateMapKey(context: String, key: String)
    case invalidDefinition(String)

    var description: String {
        switch self {
        case .invalidMagic(let value):
            return "Invalid schema entity catalog magic: \(value)"
        case .unsupportedVersion(let value):
            return "Unsupported schema entity catalog version: \(value)"
        case .invalidInteger(let field, let value):
            return "Schema entity catalog integer '\(field)' is out of range: \(value)"
        case .invalidEnum(let type, let value):
            return "Schema entity catalog contains invalid \(type) value '\(value)'"
        case .invalidMetadataTag(let value):
            return "Schema entity catalog contains invalid metadata tag \(value)"
        case .duplicateMapKey(let context, let key):
            return "Schema entity catalog contains duplicate \(context) key '\(key)'"
        case .invalidDefinition(let reason):
            return "Schema entity catalog definition is invalid: \(reason)"
        }
    }
}

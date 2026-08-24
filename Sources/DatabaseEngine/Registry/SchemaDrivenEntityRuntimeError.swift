import DatabaseKit

public enum SchemaDrivenEntityRuntimeError: Error, Sendable, Equatable,
    CustomStringConvertible {
    case entityMismatch(expected: String, actual: String)
    case unknownField(entity: String, field: String)
    case fieldIdentityMismatch(
        entity: String,
        field: String,
        expectedNumber: UInt32,
        actualNumber: UInt32
    )
    case missingRequiredField(entity: String, field: String)
    case nullRequiredField(entity: String, field: String)
    case invalidFieldValue(
        entity: String,
        field: String,
        expected: FieldSchemaType
    )
    case invalidFieldNumber(entity: String, field: String, number: Int)
    case invalidIdentifier(entity: String)
    case invalidPartition(entity: String, field: String)

    public var description: String {
        switch self {
        case .entityMismatch(let expected, let actual):
            return "Schema-driven runtime expected entity '\(expected)', got '\(actual)'"
        case .unknownField(let entity, let field):
            return "Entity '\(entity)' does not declare field '\(field)'"
        case .fieldIdentityMismatch(
            let entity,
            let field,
            let expectedNumber,
            let actualNumber
        ):
            return "Entity '\(entity)' field '\(field)' expected number \(expectedNumber), got \(actualNumber)"
        case .missingRequiredField(let entity, let field):
            return "Entity '\(entity)' is missing required field '\(field)'"
        case .nullRequiredField(let entity, let field):
            return "Entity '\(entity)' required field '\(field)' cannot be null"
        case .invalidFieldValue(let entity, let field, let expected):
            return "Entity '\(entity)' field '\(field)' does not match '\(expected.rawValue)'"
        case .invalidFieldNumber(let entity, let field, let number):
            return "Entity '\(entity)' field '\(field)' has invalid number \(number)"
        case .invalidIdentifier(let entity):
            return "Entity '\(entity)' has an invalid canonical identifier"
        case .invalidPartition(let entity, let field):
            return "Entity '\(entity)' has an invalid canonical partition field '\(field)'"
        }
    }
}

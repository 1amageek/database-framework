import DatabaseKit

public enum DatabaseExpressionEvaluationError: Error, Sendable, Equatable, CustomStringConvertible {
    case missingColumn(String)
    case ambiguousColumn(String)
    case unboundParameter
    case typeMismatch(operation: String)
    case divisionByZero
    case inexactDecimalResult
    case numericOverflow
    case numericUnderflow
    case nonFiniteValue
    case numericDomain(operation: String)
    case unsupportedExpression(String)
    case unsupportedFunction(String)
    case invalidFunctionArgument(function: String, reason: String)
    case invalidPattern(offset: UInt64, reason: String)
    case unsupportedPattern(offset: UInt64, reason: String)
    case invalidCast(String)
    case invalidTemporalInput
    case temporalValueOutOfRange
    case timeZone(SQLTimeZoneError)
    case missingTemporalEnvironment
    case collection(SQLCollectionError)
    case queryValue(QueryValueError)
    case invalidRDFLiteral(datatype: String)

    public var description: String {
        switch self {
        case .missingColumn(let name):
            return "Expression references unknown column '\(name)'"
        case .ambiguousColumn(let name):
            return "Expression references ambiguous column '\(name)'"
        case .unboundParameter:
            return "Expression contains a parameter that was not bound"
        case .typeMismatch(let operation):
            return "Expression operands are incompatible with \(operation)"
        case .divisionByZero:
            return "Expression divides by zero"
        case .inexactDecimalResult:
            return "Expression produces a decimal that cannot be represented exactly"
        case .numericOverflow:
            return "Expression numeric result is outside the canonical value range"
        case .numericUnderflow:
            return "Expression nonzero numeric result underflows its selected type"
        case .nonFiniteValue:
            return "Expression contains a non-finite numeric value"
        case .numericDomain(let operation):
            return "Expression numeric operand is outside the domain of \(operation)"
        case .unsupportedExpression(let expression):
            return "Expression is not supported by the database evaluator: \(expression)"
        case .unsupportedFunction(let function):
            return "Scalar function '\(function)' is not supported by the database evaluator"
        case .invalidFunctionArgument(let function, let reason):
            return "Invalid argument for scalar function '\(function)': \(reason)"
        case .invalidPattern(let offset, let reason):
            return "Invalid pattern at offset \(offset): \(reason)"
        case .unsupportedPattern(let offset, let reason):
            return "Unsupported pattern at offset \(offset): \(reason)"
        case .invalidCast(let type):
            return "Expression cannot be cast to \(type)"
        case .invalidTemporalInput:
            return "Expression contains invalid temporal input"
        case .temporalValueOutOfRange:
            return "Expression temporal value is outside the selected range"
        case .timeZone:
            return "Expression cannot resolve its selected time zone"
        case .missingTemporalEnvironment:
            return "Expression requires an explicit temporal environment"
        case .queryValue(let cause):
            return Self.queryValueDescription(cause)
        case .collection(let cause):
            return "Expression collection operation failed: \(cause.description)"
        case .invalidRDFLiteral(let datatype):
            return "Expression contains an invalid RDF literal for \(datatype)"
        }
    }

    private static func queryValueDescription(_ cause: QueryValueError) -> String {
        switch cause {
        case .invalidNumeric(let offset, let reason): return "Invalid query numeric at UTF-8 offset \(offset): \(reason)"
        case .invalidUUID(let offset, let reason): return "Invalid query UUID at UTF-8 offset \(offset): \(reason)"
        case .invalidBinary(let offset, let reason): return "Invalid query binary at UTF-8 offset \(offset): \(reason)"
        case .invalidNumericDisplayScale(let scale): return "Invalid query numeric display scale \(scale)"
        case .numericOutOfRange: return "Query numeric value is outside the supported range"
        case .invalidJSON(let offset, let reason): return "Invalid query JSON at UTF-8 offset \(offset): \(reason)"
        case .invalidUTF8(let offset): return "Invalid query UTF-8 at offset \(offset)"
        case .nonCanonicalJSONB: return "Query JSONB is not canonical"
        case .malformedArrayInput(let offset, let reason): return "Malformed query array at UTF-8 offset \(offset): \(reason)"
        case .invalidArrayInputIndex(let index, let count): return "Query array input index \(index) is outside \(count) inputs"
        case .invalidDimensionCount(let count): return "Invalid query array dimension count \(count)"
        case .invalidDimension(let lowerBound, let length): return "Invalid query array dimension with lower bound \(lowerBound) and length \(length)"
        case .dimensionProductOverflow: return "Query array dimension product overflows"
        case .elementCountMismatch(let expected, let actual): return "Query array requires \(expected) elements; received \(actual)"
        case .elementTypeMismatch(let index): return "Query array element \(index) has an incompatible type"
        case .incompatibleType: return "Query value has an incompatible type"
        case .invalidColumnNumber(let number): return "Invalid query column number \(number)"
        case .invalidPersistenceDescriptor: return "Invalid query persistence descriptor"
        case .invalidPersistencePayload(let reason): return "Invalid query persistence payload: \(reason)"
        case .intervalOutOfRange: return "Query interval is outside the supported range"
        case .timeOutOfRange: return "Query time is outside the supported range"
        case .unknownRetainedByteCount: return "Query retained byte count is unknown"
        case .resourceLimit(let resource, let actual, let maximum): return "Query value resource '\(resource)' requires \(actual), exceeding \(maximum)"
        case .arithmeticOverflow: return "Query value arithmetic overflows"
        }
    }
}

enum SPARQLNumericError: Error, Sendable, Equatable {
    case numericOverflow
    case divisionByZero
    case inexactDecimalResult
    case invalidResultLiteral
    case resultLiteralTooLarge(requiredUTF8Count: UInt64, maximumUTF8Count: UInt64)
}

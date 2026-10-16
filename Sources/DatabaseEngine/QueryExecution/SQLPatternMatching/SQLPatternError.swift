enum SQLPatternError: Error, Sendable, Equatable {
    case invalidPattern(offset: Int, reason: String)
    case unsupportedPattern(offset: Int, construct: String)
    case invalidFlag(offset: Int, flag: Unicode.Scalar)
    case unsupportedFlag(offset: Int, flag: Unicode.Scalar)
    case invalidEscape
    case invalidReplacement(offset: Int, reason: String)
    case invalidStart
    case invalidOccurrence
    case invalidCaptureIndex
    case meterMismatch
}

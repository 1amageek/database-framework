enum SPARQLExecutionLimits {
    static let maximumLiteralUTF8Count = 1_048_576

    static let maximumRegularExpressionPatternUTF8Count = 16_384
    static let maximumRegularExpressionPatternScalarCount = 16_384
    static let maximumRegularExpressionFlagScalarCount = 16
    static let maximumRegularExpressionNestingDepth = 64
    static let maximumRegularExpressionASTNodeCount = 8_192
    static let maximumRegularExpressionNFAStateCount = 16_384
    static let maximumRegularExpressionQuantifier = 4_096
    static let maximumRegularExpressionCaptureGroupCount = 9
    static let maximumRegularExpressionActiveTransitionWork = 16_000_000
    static let maximumRegularExpressionReplacementTokenCount = 8_192
    static let maximumRegularExpressionReplacementMatchCount = 100_000
}

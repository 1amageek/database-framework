/// Resource limits applied while compiling and evaluating XSD datatypes.
///
/// XSD value spaces are unbounded. A runtime must therefore report an explicit
/// resource failure when a valid value exceeds its configured capacity instead
/// of misclassifying that value as lexically invalid.
public struct XSDValidationLimits: Sendable, Equatable {
    public var maxLexicalUTF8Bytes: Int
    public var maxUnicodeScalars: Int
    public var maxNumericDigits: Int
    public var maxYearDigits: Int
    public var maxFractionDigits: Int
    public var maxDurationComponentDigits: Int
    public var maxFacetCount: Int
    public var maxDataRangeDepth: Int
    public var maxDataRangeNodes: Int
    public var maxDataOneOfLiterals: Int
    public var maxDataOneOfPayloadUTF8Bytes: Int
    public var maxRationalComparisonWork: Int
    public var maxLanguageSubtags: Int
    public var maxXMLDepth: Int
    public var maxXMLNodes: Int
    public var maxXMLAttributesPerElement: Int
    public var maxXMLNamespaceBindings: Int
    public var maxXMLParsingWork: Int
    public var maxXMLComparisonWork: Int
    public var maxPatternUTF8Bytes: Int
    public var maxPatternScalars: Int
    public var maxRegexNestingDepth: Int
    public var maxRegexASTNodes: Int
    public var maxRegexNFAStates: Int
    public var maxRegexQuantifier: Int
    public var maxRegexTransitionWork: Int

    public init(
        maxLexicalUTF8Bytes: Int = 1_048_576,
        maxUnicodeScalars: Int = 1_048_576,
        maxNumericDigits: Int = 100_000,
        maxYearDigits: Int = 18,
        maxFractionDigits: Int = 100_000,
        maxDurationComponentDigits: Int = 10,
        maxFacetCount: Int = 256,
        maxDataRangeDepth: Int = 64,
        maxDataRangeNodes: Int = 8_192,
        maxDataOneOfLiterals: Int = 4_096,
        maxDataOneOfPayloadUTF8Bytes: Int = 8_388_608,
        maxRationalComparisonWork: Int = 1_000_000,
        maxLanguageSubtags: Int = 128,
        maxXMLDepth: Int = 64,
        maxXMLNodes: Int = 8_192,
        maxXMLAttributesPerElement: Int = 256,
        maxXMLNamespaceBindings: Int = 1_024,
        maxXMLParsingWork: Int = 4_000_000,
        maxXMLComparisonWork: Int = 1_000_000,
        maxPatternUTF8Bytes: Int = 16_384,
        maxPatternScalars: Int = 16_384,
        maxRegexNestingDepth: Int = 64,
        maxRegexASTNodes: Int = 8_192,
        maxRegexNFAStates: Int = 16_384,
        maxRegexQuantifier: Int = 4_096,
        maxRegexTransitionWork: Int = 1_000_000
    ) {
        precondition(maxLexicalUTF8Bytes >= 0)
        precondition(maxUnicodeScalars >= 0)
        precondition(maxNumericDigits >= 0)
        precondition(maxYearDigits >= 4)
        precondition(maxYearDigits <= 18)
        precondition(maxFractionDigits >= 0)
        precondition(maxDurationComponentDigits >= 1)
        precondition(maxDurationComponentDigits <= 10)
        precondition(maxFacetCount >= 0)
        precondition(maxDataRangeDepth >= 0)
        precondition(maxDataRangeNodes >= 0)
        precondition(maxDataOneOfLiterals >= 0)
        precondition(maxDataOneOfPayloadUTF8Bytes >= 0)
        precondition(maxRationalComparisonWork >= 0)
        precondition(maxLanguageSubtags >= 1)
        precondition(maxXMLDepth >= 0)
        precondition(maxXMLNodes >= 0)
        precondition(maxXMLAttributesPerElement >= 0)
        precondition(maxXMLNamespaceBindings >= 0)
        precondition(maxXMLParsingWork >= 0)
        precondition(maxXMLComparisonWork >= 0)
        precondition(maxPatternUTF8Bytes >= 0)
        precondition(maxPatternScalars >= 0)
        precondition(maxRegexNestingDepth >= 0)
        precondition(maxRegexASTNodes >= 0)
        precondition(maxRegexNFAStates >= 0)
        precondition(maxRegexQuantifier >= 0)
        precondition(maxRegexTransitionWork >= 0)
        self.maxLexicalUTF8Bytes = maxLexicalUTF8Bytes
        self.maxUnicodeScalars = maxUnicodeScalars
        self.maxNumericDigits = maxNumericDigits
        self.maxYearDigits = maxYearDigits
        self.maxFractionDigits = maxFractionDigits
        self.maxDurationComponentDigits = maxDurationComponentDigits
        self.maxFacetCount = maxFacetCount
        self.maxDataRangeDepth = maxDataRangeDepth
        self.maxDataRangeNodes = maxDataRangeNodes
        self.maxDataOneOfLiterals = maxDataOneOfLiterals
        self.maxDataOneOfPayloadUTF8Bytes = maxDataOneOfPayloadUTF8Bytes
        self.maxRationalComparisonWork = maxRationalComparisonWork
        self.maxLanguageSubtags = maxLanguageSubtags
        self.maxXMLDepth = maxXMLDepth
        self.maxXMLNodes = maxXMLNodes
        self.maxXMLAttributesPerElement = maxXMLAttributesPerElement
        self.maxXMLNamespaceBindings = maxXMLNamespaceBindings
        self.maxXMLParsingWork = maxXMLParsingWork
        self.maxXMLComparisonWork = maxXMLComparisonWork
        self.maxPatternUTF8Bytes = maxPatternUTF8Bytes
        self.maxPatternScalars = maxPatternScalars
        self.maxRegexNestingDepth = maxRegexNestingDepth
        self.maxRegexASTNodes = maxRegexASTNodes
        self.maxRegexNFAStates = maxRegexNFAStates
        self.maxRegexQuantifier = maxRegexQuantifier
        self.maxRegexTransitionWork = maxRegexTransitionWork
    }

    public static let `default` = XSDValidationLimits()
}

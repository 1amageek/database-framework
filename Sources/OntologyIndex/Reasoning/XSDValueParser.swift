import DatabaseTypes
import DatabaseKit

/// Strict parser from an RDF literal lexical form to an XSD value.
package struct XSDValueParser: Sendable {
    package let profile: XSDDatatypeProfile
    package let limits: XSDValidationLimits

    package init(
        profile: XSDDatatypeProfile,
        limits: XSDValidationLimits
    ) {
        self.profile = profile
        self.limits = limits
    }

    package func parse(
        _ literal: RDFLiteral
    ) throws(XSDValidationFailure) -> XSDParsedValue {
        let lexicalForm = literal.lexicalForm
        let datatype = literal.datatypeIRI.rawValue
        let language = literal.languageTag?.rawValue
        guard lexicalForm.utf8.count <= limits.maxLexicalUTF8Bytes else {
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "lexicalUTF8Bytes",
                limit: limits.maxLexicalUTF8Bytes,
                actual: lexicalForm.utf8.count
            )
        }
        try enforceScalarLimit(lexicalForm)

        guard let kind = XSDDatatypeKind(iri: datatype),
              profile.supports(kind) else {
            throw XSDValidationFailure.unsupportedDatatype(datatype)
        }
        guard literal.baseDirection == nil else {
            throw invalid(
                literal,
                code: "unexpectedDirection",
                message: "this datatype does not admit an RDF base direction"
            )
        }
        guard language == nil || kind == .rdfLangString else {
            throw invalid(
                literal,
                code: "unexpectedLanguage",
                message: "only rdf:langString admits an RDF language tag"
            )
        }

        switch kind {
        case .owlReal, .rdfsLiteral:
            throw invalid(
                literal,
                code: "noLexicalSpace",
                message: "this OWL datatype does not directly provide lexical forms"
            )

        case .owlRational:
            try enforceNumericLimits(lexicalForm)
            guard let value = XSDRationalValue(
                lexicalForm: lexicalForm,
                comparisonWorkLimit: limits.maxRationalComparisonWork
            ) else {
                throw invalid(
                    literal,
                    code: "rational",
                    message: "expected integer/positive-integer without denominator sign"
                )
            }
            return .rational(value)

        case .string, .anyURI:
            guard XSDUnicodeRules.allXMLCharacters(lexicalForm) else {
                throw invalid(literal, code: "invalidXMLCharacter",
                              message: "value contains a character outside XML Char")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .rdfPlainLiteral:
            guard let value = XSDTextValue(plainLiteral: lexicalForm),
                  XSDUnicodeRules.allXMLCharacters(value.value) else {
                throw invalid(
                    literal,
                    code: "plainLiteral",
                    message: "rdf:PlainLiteral requires a final @ followed by an empty or valid language tag"
                )
            }
            if let language = value.language {
                try validateRDFLanguageTag(language, literal: literal)
            }
            return .text(value)

        case .rdfXMLLiteral:
            do throws(XSDXMLLiteralValue.ParseFailure) {
                return .xmlLiteral(try XSDXMLLiteralValue(
                    lexicalForm: lexicalForm,
                    limits: limits
                ))
            } catch let failure {
                switch failure {
                case .invalid(let reason):
                    throw invalid(
                        literal,
                        code: "xmlLiteral",
                        message: reason
                    )
                case .resource(let name, let limit, let actual):
                    throw XSDValidationFailure.resourceLimitExceeded(
                        resource: name,
                        limit: limit,
                        actual: actual
                    )
                }
            }

        case .normalizedString:
            guard XSDUnicodeRules.isNormalizedString(lexicalForm) else {
                throw invalid(literal, code: "normalizedString",
                              message: "TAB, LF, and CR are not permitted")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .token:
            guard XSDUnicodeRules.isToken(lexicalForm) else {
                throw invalid(literal, code: "token",
                              message: "whitespace is not in collapsed form")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .language:
            guard XSDUnicodeRules.isLanguage(lexicalForm) else {
                throw invalid(literal, code: "language",
                              message: "value is not an XSD language identifier")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .nmtoken:
            guard XSDUnicodeRules.isNMTOKEN(lexicalForm) else {
                throw invalid(literal, code: "nmtoken",
                              message: "value is not an XML NMTOKEN")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .name:
            guard XSDUnicodeRules.isName(lexicalForm, allowsColon: true) else {
                throw invalid(literal, code: "name",
                              message: "value is not an XML Name")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .ncname:
            guard XSDUnicodeRules.isName(lexicalForm, allowsColon: false) else {
                throw invalid(literal, code: "ncname",
                              message: "value is not an XML NCName")
            }
            return .text(XSDTextValue(kind: kind, value: lexicalForm))

        case .rdfLangString:
            guard XSDUnicodeRules.allXMLCharacters(lexicalForm),
                  let language,
                  XSDUnicodeRules.isLanguage(language) else {
                throw invalid(literal, code: "langString",
                              message: "rdf:langString requires a valid language tag")
            }
            try validateRDFLanguageTag(language[...], literal: literal)
            return .text(XSDTextValue(
                kind: kind,
                value: lexicalForm,
                language: language
            ))

        case .boolean:
            switch lexicalForm {
            case "true", "1":
                return .boolean(true)
            case "false", "0":
                return .boolean(false)
            default:
                throw invalid(literal, code: "boolean",
                              message: "expected true, false, 1, or 0")
            }

        case .decimal:
            try enforceNumericLimits(lexicalForm)
            guard let value = XSDDecimalValue(decimal: lexicalForm) else {
                throw invalid(literal, code: "decimal",
                              message: "value is not an XSD decimal")
            }
            return .decimal(kind: kind, value: value)

        case .integer, .nonPositiveInteger, .negativeInteger,
             .nonNegativeInteger, .positiveInteger, .long, .int, .short,
             .byte, .unsignedLong, .unsignedInt, .unsignedShort, .unsignedByte:
            try enforceNumericLimits(lexicalForm)
            guard let value = XSDDecimalValue(integer: lexicalForm),
                  integerValue(value, belongsTo: kind) else {
                throw invalid(literal, code: "integer",
                              message: "value is outside the datatype value space")
            }
            return .decimal(kind: kind, value: value)

        case .float, .double:
            try enforceNumericLimits(lexicalForm)
            guard let value = XSDFloatingPointValue(
                lexicalForm: lexicalForm,
                isFloat: kind == .float
            ) else {
                throw invalid(literal, code: "floatingPoint",
                              message: "value is not an XSD floating-point literal")
            }
            return .floating(kind: kind, value: value)

        case .duration:
            switch XSDDurationValue.parse(
                lexicalForm,
                componentDigitLimit: limits.maxDurationComponentDigits,
                fractionDigitLimit: limits.maxFractionDigits
            ) {
            case .success(let value):
                return .duration(value)
            case .failure(.invalid):
                throw invalid(literal, code: "duration",
                              message: "value is not an XSD duration")
            case .failure(.componentLimit(let actual, let limit)):
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "durationComponentDigits",
                    limit: limit,
                    actual: actual
                )
            case .failure(.arithmeticLimit):
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "durationArithmetic",
                    limit: limits.maxDurationComponentDigits,
                    actual: limits.maxDurationComponentDigits
                )
            }

        case .dateTime, .dateTimeStamp, .time, .date:
            try enforceTemporalLimits(lexicalForm, kind: kind)
            let temporalKind: XSDTemporalValue.Kind
            switch kind {
            case .date: temporalKind = .date
            case .time: temporalKind = .time
            default: temporalKind = .dateTime
            }
            guard let value = XSDTemporalValue(
                lexicalForm: lexicalForm,
                kind: temporalKind
            ), kind != .dateTimeStamp || value.timezoneOffsetMinutes != nil else {
                throw invalid(literal, code: "temporal",
                              message: "value is not in the datatype lexical space")
            }
            return .temporal(kind: kind, value: value)

        case .base64Binary:
            guard let value = XSDBinaryValue(base64: lexicalForm) else {
                throw invalid(literal, code: "base64Binary",
                              message: "invalid alphabet, padding, or unused pad bits")
            }
            return .binary(value)

        case .hexBinary:
            guard let value = XSDBinaryValue(hexadecimal: lexicalForm) else {
                throw invalid(literal, code: "hexBinary",
                              message: "expected an even number of ASCII hex digits")
            }
            return .binary(value)
        }
    }

    private func validateRDFLanguageTag(
        _ language: Substring,
        literal: RDFLiteral
    ) throws(XSDValidationFailure) {
        switch RDFLanguageTagValidator.validate(
            language,
            maximumSubtags: limits.maxLanguageSubtags
        ) {
        case .valid:
            return
        case .invalid:
            throw invalid(
                literal,
                code: "languageTag",
                message: "language tag is not well-formed BCP 47"
            )
        case .subtagLimit(let limit, let actual):
            throw XSDValidationFailure.resourceLimitExceeded(
                resource: "languageSubtags",
                limit: limit,
                actual: actual
            )
        }
    }

    private func enforceScalarLimit(
        _ source: String
    ) throws(XSDValidationFailure) {
        var count = 0
        for _ in source.unicodeScalars {
            count += 1
            guard count <= limits.maxUnicodeScalars else {
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "unicodeScalars",
                    limit: limits.maxUnicodeScalars,
                    actual: count
                )
            }
        }
    }

    private func enforceNumericLimits(
        _ source: String
    ) throws(XSDValidationFailure) {
        var digitCount = 0
        var fractionDigitCount = 0
        var afterDecimalPoint = false
        var beforeExponent = true
        for byte in source.utf8 {
            if byte == 69 || byte == 101 {
                beforeExponent = false
                afterDecimalPoint = false
            } else if byte == 46, beforeExponent {
                afterDecimalPoint = true
            } else if byte >= 48, byte <= 57 {
                digitCount += 1
                if afterDecimalPoint, beforeExponent { fractionDigitCount += 1 }
            }
            guard digitCount <= limits.maxNumericDigits else {
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "numericDigits",
                    limit: limits.maxNumericDigits,
                    actual: digitCount
                )
            }
            guard fractionDigitCount <= limits.maxFractionDigits else {
                throw XSDValidationFailure.resourceLimitExceeded(
                    resource: "fractionDigits",
                    limit: limits.maxFractionDigits,
                    actual: fractionDigitCount
                )
            }
        }
    }

    private func enforceTemporalLimits(
        _ source: String,
        kind: XSDDatatypeKind
    ) throws(XSDValidationFailure) {
        let bytes = source.utf8
        var index = bytes.startIndex
        if kind != .time {
            if index != bytes.endIndex, bytes[index] == 45 {
                bytes.formIndex(after: &index)
            }
            var yearDigits = 0
            while index != bytes.endIndex {
                let byte = bytes[index]
                guard byte >= 48, byte <= 57 else { break }
                yearDigits += 1
                guard yearDigits <= limits.maxYearDigits else {
                    throw XSDValidationFailure.resourceLimitExceeded(
                        resource: "yearDigits",
                        limit: limits.maxYearDigits,
                        actual: yearDigits
                    )
                }
                bytes.formIndex(after: &index)
            }
        }

        var fractionDigits = 0
        var inFraction = false
        for byte in source.utf8 {
            if byte == 46 {
                inFraction = true
                continue
            }
            if inFraction {
                if byte >= 48, byte <= 57 {
                    fractionDigits += 1
                    guard fractionDigits <= limits.maxFractionDigits else {
                        throw XSDValidationFailure.resourceLimitExceeded(
                            resource: "fractionDigits",
                            limit: limits.maxFractionDigits,
                            actual: fractionDigits
                        )
                    }
                } else {
                    inFraction = false
                }
            }
        }
    }

    private func integerValue(
        _ value: XSDDecimalValue,
        belongsTo kind: XSDDatatypeKind
    ) -> Bool {
        switch kind {
        case .integer:
            return true
        case .nonPositiveInteger:
            return value.sign <= 0
        case .negativeInteger:
            return value.sign < 0
        case .nonNegativeInteger:
            return value.sign >= 0
        case .positiveInteger:
            return value.sign > 0
        case .long:
            return value.isWithin(
                minimum: "-9223372036854775808",
                maximum: "9223372036854775807"
            )
        case .int:
            return value.isWithin(minimum: "-2147483648", maximum: "2147483647")
        case .short:
            return value.isWithin(minimum: "-32768", maximum: "32767")
        case .byte:
            return value.isWithin(minimum: "-128", maximum: "127")
        case .unsignedLong:
            return value.isWithin(minimum: "0", maximum: "18446744073709551615")
        case .unsignedInt:
            return value.isWithin(minimum: "0", maximum: "4294967295")
        case .unsignedShort:
            return value.isWithin(minimum: "0", maximum: "65535")
        case .unsignedByte:
            return value.isWithin(minimum: "0", maximum: "255")
        default:
            return false
        }
    }

    private func invalid(
        _ literal: RDFLiteral,
        code: String,
        message: String
    ) -> XSDValidationFailure {
        .invalidLexicalForm(
            lexicalForm: literal.lexicalForm,
            datatype: literal.datatypeIRI.rawValue,
            diagnostic: XSDDiagnostic(code: code, message: message)
        )
    }
}

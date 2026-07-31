// ExpressionEvaluator.swift
// GraphIndex - Evaluates DatabaseKit.Expression against VariableBinding

import DatabaseWire
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import OntologyIndex

/// Evaluates DatabaseKit.Expression against a VariableBinding (SPARQL solution row).
///
/// Applies VariableBinding-based evaluation to QueryIR's unified expressions.
/// Used for FILTER clause evaluation
/// when expressions are represented as QueryIR types rather than FilterExpression.
///
/// Follows SPARQL §17.2 Effective Boolean Value and expression-error semantics.
/// Local SPARQL expression errors become `false` only at a FILTER boundary.
/// Resource exhaustion, unsupported expressions, and runtime failures propagate.
public struct ExpressionEvaluator: Sendable {
    private struct OperandView: RandomAccessCollection {
        typealias Index = Int
        typealias Element = FieldValue

        private let storage: ArraySlice<FieldValue>

        init(_ storage: consuming ArraySlice<FieldValue>) {
            self.storage = consume storage
        }

        var startIndex: Int { 0 }
        var endIndex: Int { storage.count }

        func index(after index: Int) -> Int {
            index + 1
        }

        func index(before index: Int) -> Int {
            index - 1
        }

        subscript(position: Int) -> FieldValue {
            storage[storage.startIndex + position]
        }
    }

    private init() {}

    // MARK: - Boolean Evaluation (FILTER)

    /// Evaluate an expression as a boolean for FILTER.
    ///
    /// Per SPARQL §17.2, evaluation errors yield `false`.
    /// This is the primary entry point for FILTER clause evaluation.
    public static func evaluateAsBoolean(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> Bool {
        do throws(SPARQLExpressionEvaluationError) {
            return try effectiveBooleanValue(
                evaluate(expr, binding: binding)
            )
        } catch let error
            where error.isSPARQLEvaluationError {
            return false
        }
    }

    /// Evaluates an already compiled expression plan without rebuilding its
    /// flat program for each solution.
    public static func evaluateAsBoolean(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> Bool {
        do throws(SPARQLExpressionEvaluationError) {
            return try effectiveBooleanValue(
                evaluate(plan, binding: binding)
            )
        } catch let error
            where error.isSPARQLEvaluationError {
            return false
        }
    }

    /// Evaluates an ORDER BY expression. SPARQL expression errors produce an
    /// unbound sort key; resource and runtime failures remain thrown.
    public static func evaluateForOrdering(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue? {
        do throws(SPARQLExpressionEvaluationError) {
            return try evaluate(expr, binding: binding)
        } catch let error
            where error.isSPARQLEvaluationError {
            return nil
        }
    }

    // MARK: - General Evaluation

    /// Evaluate an expression to a FieldValue.
    ///
    /// DatabaseKit.Expression errors are typed and never collapsed into an absent value.
    public static func evaluate(
        _ expr: consuming DatabaseKit.Expression,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        let program: SPARQLExpressionProgram
        do throws(SPARQLExpressionCompilationError) {
            try SPARQLExpressionValidator.validate(
                expr,
                limits: .default
            )
            program = try SPARQLExpressionProgram(
                consume expr,
                limits: .default,
                compileExistsPatterns: false
            )
        } catch let error {
            switch error {
            case .structural(
                .resourceLimitExceeded(
                    let resource,
                    let actual,
                    let maximum
                )
            ):
                throw .resourceLimitExceeded(
                    stage: "compilation.\(resource.rawValue)",
                    required: actual,
                    maximum: maximum
                )
            case .resourceLimitExceeded(
                let resource,
                let actual,
                let maximum
            ):
                throw .resourceLimitExceeded(
                    stage: "compilation.\(resource.rawValue)",
                    required: actual,
                    maximum: maximum
                )
            case .invalidFunctionArity(let function, _, _):
                throw .invalidFunctionArguments(function)
            default:
                throw .unsupportedExpression(error.description)
            }
        }
        return try evaluate(program, binding: binding)
    }

    public static func evaluate(
        _ plan: SPARQLExpressionPlan,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        try evaluate(plan.program, binding: binding)
    }

    private static func evaluate(
        _ program: SPARQLExpressionProgram,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        var machine = SPARQLExpressionEvaluationMachine(
            program: program,
            binding: binding
        )
        while true {
            switch machine.advance() {
            case .finished(.value(let value)):
                return value
            case .finished(.expressionError(let error)):
                throw error
            case .action(.exists):
                throw .unsupportedExpression(
                    "EXISTS requires a runtime resolver"
                )
            case .action(.function(let name, _)):
                throw .unsupportedExpression(
                    "function \(name) requires a runtime resolver"
                )
            }
        }
    }

    // MARK: - Built-in Functions

    static func evaluateBuiltIn(
        _ builtIn: SPARQLFunctionIdentifier.BuiltIn,
        arguments: consuming ArraySlice<FieldValue>
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        let args = OperandView(consume arguments)
        let name = builtIn.rawValue

        switch name {
        case "STR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let string = stringRepresentation(value) else {
                throw typeError("STR requires an RDF term or scalar value")
            }
            return try stringValue(string)
        case "STRLEN":
            try requireArgumentCount(args, count: 1, function: name)
            let string = try evaluateAsString(args[0])
            guard let length = Int64(exactly: string.unicodeScalars.count) else {
                throw resourceLimit(stage: "STRLEN", required: UInt64(string.utf8.count))
            }
            return try integerValue(length)
        case "UCASE":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluateStringValue(args[0])
            return try stringValue(
                value.replacingLexicalForm(value.lexicalForm.uppercased())
            )
        case "LCASE":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluateStringValue(args[0])
            return try stringValue(
                value.replacingLexicalForm(value.lexicalForm.lowercased())
            )
        case "CONTAINS":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateStringValue(args[0])
            let substring = try evaluateStringValue(args[1])
            guard string.acceptsArgument(substring) else {
                throw typeError("CONTAINS arguments have incompatible language annotations")
            }
            return try booleanValue(
                TextSearch.contains(
                    substring.lexicalForm,
                    in: string.lexicalForm
                )
            )
        case "STRSTARTS":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateStringValue(args[0])
            let prefix = try evaluateStringValue(args[1])
            guard string.acceptsArgument(prefix) else {
                throw typeError("STRSTARTS arguments have incompatible language annotations")
            }
            return try booleanValue(
                string.lexicalForm.hasPrefix(prefix.lexicalForm)
            )
        case "STRENDS":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateStringValue(args[0])
            let suffix = try evaluateStringValue(args[1])
            guard string.acceptsArgument(suffix) else {
                throw typeError("STRENDS arguments have incompatible language annotations")
            }
            return try booleanValue(
                string.lexicalForm.hasSuffix(suffix.lexicalForm)
            )
        case "SUBSTR":
            guard args.count == 2 || args.count == 3 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            let string = try evaluateStringValue(args[0])
            let start = try evaluateAsInt64(args[1])
            guard let platformStart = Int(exactly: start) else {
                throw typeError("SUBSTR start is outside the platform index range")
            }
            let startOffset = max(platformStart - 1, 0)
            let scalars = string.lexicalForm.unicodeScalars
            guard startOffset <= scalars.count else {
                return try stringValue(string.replacingLexicalForm(""))
            }
            let from = scalars.index(scalars.startIndex, offsetBy: startOffset)
            if args.count == 3 {
                let length = try evaluateAsInt64(args[2])
                guard length >= 0, let platformLength = Int(exactly: length) else {
                    throw typeError("SUBSTR length must be a representable non-negative integer")
                }
                let remaining = scalars.count - startOffset
                let endOffset = startOffset + min(platformLength, remaining)
                let to = scalars.index(scalars.startIndex, offsetBy: endOffset)
                return try stringValue(
                    string.replacingLexicalForm(String(scalars[from..<to]))
                )
            }
            return try stringValue(
                string.replacingLexicalForm(String(scalars[from...]))
            )
        case "CONCAT":
            var result = ""
            var values: [SPARQLStringValue] = []
            values.reserveCapacity(args.count)
            for argument in args {
                values.append(
                    try evaluateStringValue(argument)
                )
            }
            var requiredUTF8Count: UInt64 = 0
            for value in values {
                let count = UInt64(value.lexicalForm.utf8.count)
                let (sum, overflow) = requiredUTF8Count
                    .addingReportingOverflow(count)
                guard !overflow else {
                    throw resourceLimit(stage: "CONCAT", required: UInt64.max)
                }
                requiredUTF8Count = sum
            }
            guard requiredUTF8Count <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
                throw resourceLimit(stage: "CONCAT", required: requiredUTF8Count)
            }
            if let capacity = Int(exactly: requiredUTF8Count) {
                result.reserveCapacity(capacity)
            }
            for value in values {
                result.append(value.lexicalForm)
            }
            return try stringValue(
                SPARQLStringValue(
                    lexicalForm: result,
                    kind: SPARQLStringValue.concatenationKind(values)
                )
            )
        case "REPLACE":
            guard args.count == 3 || args.count == 4 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            let string = try evaluateStringValue(args[0])
            let pattern = try evaluateStringValue(args[1])
            let replacement = try evaluateStringValue(args[2])
            guard pattern.kind == .string, replacement.kind == .string else {
                throw typeError("REPLACE pattern and replacement must be xsd:string")
            }
            let flags = args.count == 4
                ? try evaluateAsString(args[3])
                : nil
            return try stringValue(
                string.replacingLexicalForm(
                    replaceRegex(
                    string.lexicalForm,
                    pattern: pattern.lexicalForm,
                    replacement: replacement.lexicalForm,
                    flags: flags
                    )
                )
            )

        case "STRBEFORE", "STRAFTER":
            try requireArgumentCount(args, count: 2, function: name)
            let source = try evaluateStringValue(args[0])
            let search = try evaluateStringValue(args[1])
            guard source.acceptsArgument(search) else {
                throw typeError("\(name) arguments have incompatible language annotations")
            }
            guard let range = TextSearch.firstRange(
                of: search.lexicalForm,
                in: source.lexicalForm
            ) else {
                return try stringValue("")
            }
            let lexicalForm = name == "STRBEFORE"
                ? String(source.lexicalForm[..<range.lowerBound])
                : String(source.lexicalForm[range.upperBound...])
            return try stringValue(
                source.replacingLexicalForm(lexicalForm)
            )

        case "ENCODE_FOR_URI":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluateStringValue(args[0])
            guard value.kind == .string else {
                throw typeError("ENCODE_FOR_URI requires an xsd:string argument")
            }
            return try stringValue(percentEncodeForURI(value.lexicalForm))

        case "IRI", "URI":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            if case .rdfTerm(.iri) = value { return value }
            guard let string = SPARQLStringValue(value),
                  string.kind == .string else {
                throw typeError("\(name) requires an IRI or xsd:string argument")
            }
            do throws(RDFIRIError) {
                return .rdfTerm(.iri(try RDFIRI(string.lexicalForm)))
            } catch {
                throw typeError("\(name) requires an absolute IRI when no base IRI is configured")
            }

        case "LANGMATCHES":
            try requireArgumentCount(args, count: 2, function: name)
            let language = try evaluateAsString(args[0])
            let range = try evaluateAsString(args[1])
            return try booleanValue(languageMatches(language, range: range))

        case "SAMETERM":
            try requireArgumentCount(args, count: 2, function: name)
            let lhs = args[0]
            let rhs = args[1]
            guard case .rdfTerm(let lhsTerm) = lhs,
                  case .rdfTerm(let rhsTerm) = rhs else {
                throw typeError("sameTerm requires RDF terms")
            }
            return try booleanValue(lhsTerm == rhsTerm)

        case "STRDT":
            try requireArgumentCount(args, count: 2, function: name)
            let lexical = try evaluateStringValue(args[0])
            let datatype = args[1]
            guard lexical.kind == .string,
                  case .rdfTerm(.iri(let datatypeIRI)) = datatype else {
                throw typeError("STRDT requires xsd:string and IRI arguments")
            }
            do {
                return try Literal.typedLiteral(
                    value: lexical.lexicalForm,
                    datatype: datatypeIRI.rawValue
                ).toSPARQLFieldValue()
            } catch let error {
                throw mapLiteralConversionError(error)
            }

        case "STRLANG":
            try requireArgumentCount(args, count: 2, function: name)
            let lexical = try evaluateStringValue(args[0])
            let language = try evaluateStringValue(args[1])
            guard lexical.kind == .string, language.kind == .string else {
                throw typeError("STRLANG requires xsd:string arguments")
            }
            do throws(SPARQLLiteralConversionError) {
                return try Literal.langLiteral(
                    value: lexical.lexicalForm,
                    language: language.lexicalForm
                ).toSPARQLFieldValue()
            } catch let error {
                throw mapLiteralConversionError(error)
            }

        case "REGEX":
            guard args.count == 2 || args.count == 3 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            let text = try evaluateAsString(args[0])
            let pattern = try evaluateStringValue(args[1])
            guard pattern.kind == .string else {
                throw typeError("REGEX pattern must be xsd:string")
            }
            let flags: String?
            if args.count == 3 {
                let flagValue = try evaluateStringValue(args[2])
                guard flagValue.kind == .string else {
                    throw typeError("REGEX flags must be xsd:string")
                }
                flags = flagValue.lexicalForm
            } else {
                flags = nil
            }
            return try booleanValue(
                matchRegex(text, pattern: pattern.lexicalForm, flags: flags)
            )

        case "ABS":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("ABS requires a numeric operand")
            }
            do {
                return try numeric.magnitude().fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }
        case "ROUND":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("ROUND requires a numeric operand")
            }
            do {
                return try numeric.rounded(.round).fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }
        case "CEIL":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("CEIL requires a numeric operand")
            }
            do {
                return try numeric.rounded(.ceiling).fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }
        case "FLOOR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("FLOOR requires a numeric operand")
            }
            do {
                return try numeric.rounded(.floor).fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }

        case "ISIRI", "ISURI":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.iri) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISBLANK":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.blankNode) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISLITERAL":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.literal) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISNUMERIC":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            return try booleanValue(SPARQLNumericValue(value) != nil)

        case "MD5", "SHA1", "SHA256", "SHA384", "SHA512":
            try requireArgumentCount(args, count: 1, function: name)
            let string = try evaluateStringValue(args[0])
            guard string.kind == .string else {
                throw typeError("\(name) requires an xsd:string argument")
            }
            guard
                let digest = digestHexadecimalString(
                    named: name,
                    for: string.lexicalForm
                )
            else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "registered hash function was not implemented: \(name)"
                )
            }
            return try stringValue(digest)

        case "DATATYPE":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard let datatype = xsdDatatype(value) else {
                throw typeError("DATATYPE requires a literal")
            }
            do {
                return .rdfTerm(.iri(try RDFIRI(datatype)))
            } catch {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "DATATYPE produced an invalid canonical IRI"
                )
            }

        case "LANG":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.literal(let literal)) = value else {
                throw typeError("LANG requires an RDF literal")
            }
            return try stringValue(literal.languageTag?.rawValue ?? "")

        case "LANGDIR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.literal(let literal)) = value else {
                throw typeError("LANGDIR requires an RDF literal")
            }
            return try stringValue(literal.baseDirection?.rawValue ?? "")

        case "HASLANG":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.literal(let literal)) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(literal.languageTag != nil)

        case "HASLANGDIR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = args[0]
            guard case .rdfTerm(.literal(let literal)) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(literal.baseDirection != nil)

        case "TRIGRAM_SIM":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateAsString(args[0])
            let pattern = try evaluateAsString(args[1])
            guard let numeric = SPARQLNumericValue(
                .float64(TrigramSimilarity.score(string, pattern))
            ) else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "TRIGRAM_SIM result was not numeric"
                )
            }
            do throws(SPARQLNumericError) {
                return try numeric.fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }

        case "STRLANGDIR":
            try requireArgumentCount(args, count: 3, function: name)
            let string = try evaluateAsString(args[0])
            let languageValue = try evaluateAsString(args[1])
            let directionValue = try evaluateAsString(args[2])
            let language: RDFLanguageTag
            do {
                language = try RDFLanguageTag(languageValue)
            } catch {
                throw typeError("STRLANGDIR language tag is invalid")
            }
            guard let direction = RDFDirection(rawValue: directionValue) else {
                throw SPARQLExpressionEvaluationError.typeError(
                    "STRLANGDIR direction must be ltr or rtl"
                )
            }
            return .rdfTerm(
                .literal(
                    RDFLiteral(
                        lexicalForm: string,
                        language: language,
                        direction: direction
                    )
                )
            )

        case "IF", "BOUND", "COALESCE":
            throw SPARQLExpressionEvaluationError
                .invalidFunctionArguments(name)

        default:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                "function \(name)"
            )
        }
    }

    // MARK: - Helpers

    private static func evaluateAsString(
        _ value: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> String {
        try evaluateStringValue(value).lexicalForm
    }

    private static func evaluateStringValue(
        _ value: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> SPARQLStringValue {
        guard let string = SPARQLStringValue(value) else {
            throw typeError("string function requires a string literal")
        }
        return string
    }

    private static func evaluateAsInt64(
        _ value: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> Int64 {
        guard let integer = SPARQLNumericValue(value)?.exactInteger else {
            throw typeError("integer argument is required")
        }
        return integer
    }

    private static func stringRepresentation(_ value: FieldValue) -> String? {
        switch value {
        case .string(let s): return s
        case .int8(let v): return String(v)
        case .int16(let v): return String(v)
        case .int32(let v): return String(v)
        case .int64(let v): return String(v)
        case .uint8(let v): return String(v)
        case .uint16(let v): return String(v)
        case .uint32(let v): return String(v)
        case .uint64(let v): return String(v)
        case .float32(let v): return String(v)
        case .float64(let v): return String(v)
        case .decimal(let decimal):
            do {
                return try decimal.decimalLexicalForm(
                    maximumUTF8Count:
                        SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            } catch {
                return nil
            }
        case .bool(let v): return v ? "true" : "false"
        case .date(let value):
            return QueryLiteralEncoding.iso8601(value)
        case .timestamp(let value):
            return QueryLiteralEncoding.iso8601(value)
        case .uuid(let value):
            return value.description
        case .rdfTerm(.iri(let iri)): return iri.rawValue
        case .rdfTerm(.literal(let literal)): return literal.lexicalForm
        case .bytes, .time, .dateTime, .timeSpan, .calendarPeriod,
             .geographicPoint, .geographicPosition, .vector, .object,
             .reference, .rdfTerm, .null, .array:
            return nil
        }
    }

    private static let xsdNamespace = "http://www.w3.org/2001/XMLSchema#"

    static func effectiveBooleanValue(_ value: FieldValue) throws(SPARQLExpressionEvaluationError) -> Bool {
        switch value {
        case .bool(let v): return v
        case .string(let s): return !s.isEmpty
        case .rdfTerm(.literal(let literal)):
            if literal.datatypeIRI.rawValue == xsdNamespace + "boolean" {
                switch literal.lexicalForm {
                case "true", "1": return true
                case "false", "0": return false
                default:
                    throw typeError("invalid xsd:boolean lexical form")
                }
            }
            if literal.datatypeIRI.rawValue == xsdNamespace + "string"
            {
                return !literal.lexicalForm.isEmpty
            }
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("literal datatype has no effective boolean value")
            }
            return !numeric.isZero && !numeric.isNaN
        case .null:
            throw typeError("null has no SPARQL effective boolean value")
        default:
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("value has no SPARQL effective boolean value")
            }
            return !numeric.isZero && !numeric.isNaN
        }
    }

    private static func compareFieldValues(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> SPARQLComparisonOrder? {
        guard left != .null, right != .null else {
            throw typeError("null values are not comparable")
        }
        if case .rdfTerm(.literal(let leftLiteral)) = left,
           case .rdfTerm(.literal(let rightLiteral)) = right {
            let comparison: SPARQLValueComparison
            do throws(XSDValidationFailure) {
                comparison = try SPARQLValueComparator().compare(
                    leftLiteral,
                    rightLiteral
                )
            } catch let failure {
                throw mapXSDValidationFailure(failure)
            }
            switch comparison {
            case .less: return .ascending
            case .equal: return .same
            case .greater: return .descending
            case .unordered: return nil
            case .typeError:
                throw typeError("RDF literals are not order-comparable")
            }
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right) {
            guard let comparison = leftNumeric.compare(to: rightNumeric) else {
                return nil
            }
            return comparison
        }
        if case .rdfTerm = left {
            throw typeError("RDF terms are not relationally comparable")
        }
        if case .rdfTerm = right {
            throw typeError("RDF terms are not relationally comparable")
        }
        guard let comparison = left.compare(to: right) else {
            throw typeError("values are not order-comparable")
        }
        switch comparison {
        case .lessThan:
            return .ascending
        case .equal:
            return .same
        case .greaterThan:
            return .descending
        }
    }

    static func equalFieldValues(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws(SPARQLExpressionEvaluationError) -> Bool {
        guard left != .null, right != .null else {
            throw typeError("null values are not comparable")
        }
        if case .rdfTerm(let leftTerm) = left,
           case .rdfTerm(let rightTerm) = right {
            return try equalRDFTerms(leftTerm, rightTerm)
        }
        if let leftNumeric = SPARQLNumericValue(left),
           let rightNumeric = SPARQLNumericValue(right) {
            guard let comparison = leftNumeric.compare(to: rightNumeric) else {
                throw typeError("numeric values are unordered")
            }
            return comparison == .same
        }
        return left == right
    }

    private static func equalRDFTerms(
        _ left: RDFTerm,
        _ right: RDFTerm
    ) throws(SPARQLExpressionEvaluationError) -> Bool {
        var pending = [(left, right)]
        while let (lhs, rhs) = pending.popLast() {
            switch (lhs, rhs) {
            case (.iri(let leftIRI), .iri(let rightIRI)):
                guard leftIRI == rightIRI else {
                    return false
                }
            case (
                .blankNode(let leftIdentifier),
                .blankNode(let rightIdentifier)
            ):
                guard leftIdentifier == rightIdentifier else {
                    return false
                }
            case (
                .literal(let leftLiteral),
                .literal(let rightLiteral)
            ):
                if leftLiteral.languageTag != nil
                    || rightLiteral.languageTag != nil
                    || leftLiteral.baseDirection != nil
                    || rightLiteral.baseDirection != nil {
                    guard leftLiteral == rightLiteral else {
                        return false
                    }
                    continue
                }
                let comparison: SPARQLValueComparison
                do throws(XSDValidationFailure) {
                    comparison = try SPARQLValueComparator().compare(
                        leftLiteral,
                        rightLiteral
                    )
                } catch let failure {
                    throw mapXSDValidationFailure(failure)
                }
                switch comparison {
                case .equal:
                    continue
                case .less, .greater, .unordered:
                    return false
                case .typeError:
                    throw typeError(
                        "RDF literals are not value-comparable"
                    )
                }
            case (
                .tripleTerm(
                    let leftSubject,
                    let leftPredicate,
                    let leftObject
                ),
                .tripleTerm(
                    let rightSubject,
                    let rightPredicate,
                    let rightObject
                )
            ):
                pending.append((leftObject, rightObject))
                pending.append(
                    (leftPredicate.term, rightPredicate.term)
                )
                pending.append((leftSubject.term, rightSubject.term))
            default:
                return false
            }
        }
        return true
    }

    private static func requireArgumentCount<Arguments: Collection>(
        _ arguments: borrowing Arguments,
        count: Int,
        function: String
    ) throws(SPARQLExpressionEvaluationError) {
        guard arguments.count == count else {
            throw SPARQLExpressionEvaluationError
                .invalidFunctionArguments(function)
        }
    }

    private static func booleanValue(_ value: Bool) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        try rdfLiteralValue(
            value ? "true" : "false",
            datatype: xsdNamespace + "boolean"
        )
    }

    private static func integerValue(_ value: Int64) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        try rdfLiteralValue(
            String(value),
            datatype: xsdNamespace + "integer"
        )
    }

    private static func stringValue(_ value: String) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        let count = UInt64(value.utf8.count)
        guard count <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
            throw resourceLimit(stage: "string result", required: count)
        }
        return try rdfLiteralValue(value, datatype: xsdNamespace + "string")
    }

    private static func stringValue(
        _ value: SPARQLStringValue
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        let count = UInt64(value.lexicalForm.utf8.count)
        guard count <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
            throw resourceLimit(stage: "string result", required: count)
        }
        return value.fieldValue()
    }

    private static func rdfLiteralValue(
        _ lexicalForm: String,
        datatype: String
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        do {
            return .rdfTerm(
                .literal(
                    try RDFLiteral(
                        lexicalForm: lexicalForm,
                        datatype: datatype
                    )
                )
            )
        } catch {
            throw SPARQLExpressionEvaluationError.runtimeInvariant(
                "canonical RDF datatype IRI was rejected: \(datatype)"
            )
        }
    }

    private static func typeError(
        _ detail: String
    ) -> SPARQLExpressionEvaluationError {
        .typeError(detail)
    }

    private static func resourceLimit(
        stage: String,
        required: UInt64? = nil,
        maximum: UInt64? = UInt64(SPARQLExecutionLimits.maximumLiteralUTF8Count)
    ) -> SPARQLExpressionEvaluationError {
        .resourceLimitExceeded(
            stage: stage,
            required: required,
            maximum: maximum
        )
    }

    private static func mapLiteralConversionError(
        _ error: SPARQLLiteralConversionError
    ) -> SPARQLExpressionEvaluationError {
        switch error {
        case .literalTooLarge(let required, let maximum):
            return .resourceLimitExceeded(
                stage: "literal conversion",
                required: required,
                maximum: maximum
            )
        case .nullTermUnsupported, .arrayTermUnsupported, .invalidLexicalForm:
            return .typeError("The numeric value could not be converted")
        }
    }

    private static func mapNumericError(
        _ error: SPARQLNumericError
    ) -> SPARQLExpressionEvaluationError {
        switch error {
        case .resultLiteralTooLarge(let required, let maximum):
            return .resourceLimitExceeded(
                stage: "numeric result",
                required: required,
                maximum: maximum
            )
        case .numericOverflow, .divisionByZero, .inexactDecimalResult,
             .invalidResultLiteral:
            return .typeError("The numeric operation could not be completed")
        }
    }

    private static func mapXSDValidationFailure(
        _ error: XSDValidationFailure
    ) -> SPARQLExpressionEvaluationError {
        switch error {
        case .resourceLimitExceeded(let resource, let limit, let actual):
            return .resourceLimitExceeded(
                stage: resource,
                required: UInt64(actual),
                maximum: UInt64(limit)
            )
        case .invalidLexicalForm, .unsupportedDatatype:
            return .typeError(error.description)
        case .invalidRestriction:
            return .runtimeInvariant(error.description)
        }
    }

    private static func percentEncodeForURI(_ value: String) -> String {
        var encodedByteCount = 0
        for byte in value.utf8 {
            encodedByteCount += isURIUnreserved(byte) ? 1 : 3
        }
        return String(unsafeUninitializedCapacity: encodedByteCount) { output in
            var offset = 0
            for byte in value.utf8 {
                if isURIUnreserved(byte) {
                    output[offset] = byte
                    offset += 1
                } else {
                    output[offset] = 37
                    output[offset + 1] = hexadecimalCharacter(byte >> 4)
                    output[offset + 2] = hexadecimalCharacter(byte & 0x0f)
                    offset += 3
                }
            }
            return offset
        }
    }

    private static func isURIUnreserved(_ byte: UInt8) -> Bool {
        switch byte {
        case 65...90, 97...122, 48...57, 45, 46, 95, 126:
            return true
        default:
            return false
        }
    }

    private static func hexadecimalCharacter(_ value: UInt8) -> UInt8 {
        value < 10 ? 48 + value : 55 + value
    }

    private static func languageMatches(
        _ language: String,
        range: String
    ) -> Bool {
        let normalizedLanguage = language.lowercased()
        let normalizedRange = range.lowercased()
        if normalizedRange == "*" { return !normalizedLanguage.isEmpty }
        guard !normalizedRange.isEmpty else { return normalizedLanguage.isEmpty }
        return normalizedLanguage == normalizedRange
            || normalizedLanguage.hasPrefix(normalizedRange + "-")
    }

    // MARK: - Regex

    private static func matchRegex(
        _ string: String,
        pattern: String,
        flags: String?
    ) throws(SPARQLExpressionEvaluationError) -> Bool {
        try SPARQLRegularExpression.evaluateMatch(
            string,
            pattern: pattern,
            flags: flags
        )
    }

    private static func replaceRegex(
        _ string: String,
        pattern: String,
        replacement: String,
        flags: String?
    ) throws(SPARQLExpressionEvaluationError) -> String {
        try SPARQLRegularExpression.evaluateReplacement(
            string,
            pattern: pattern,
            replacement: replacement,
            flags: flags
        )
    }

    private static func likeToRegex(_ pattern: String) throws(SPARQLExpressionEvaluationError) -> String {
        let maximum = UInt64(
            SPARQLExecutionLimits.maximumRegularExpressionPatternUTF8Count
        )
        var inputByteCount: UInt64 = 0
        for scalar in pattern.unicodeScalars {
            let (next, overflow) = inputByteCount.addingReportingOverflow(
                UInt64(regexUTF8Width(of: scalar.value))
            )
            guard !overflow, next <= maximum else {
                throw resourceLimit(
                    stage: "LIKE regular expression pattern",
                    required: overflow ? UInt64.max : next,
                    maximum: maximum
                )
            }
            inputByteCount = next
        }

        var requiredByteCount: UInt64 = 2
        for scalar in pattern.unicodeScalars {
            let scalarByteCount = UInt64(regexUTF8Width(of: scalar.value))
            let escapedByteCount: UInt64
            switch scalar.value {
            case 0x25:
                escapedByteCount = 2
            case 0x5F:
                escapedByteCount = 1
            case 0x2E, 0x5C, 0x5B, 0x5D, 0x28, 0x29, 0x7B, 0x7D,
                 0x5E, 0x24, 0x2B, 0x3F, 0x7C, 0x2A:
                escapedByteCount = scalarByteCount + 1
            default:
                escapedByteCount = scalarByteCount
            }
            let (next, overflow) = requiredByteCount.addingReportingOverflow(
                escapedByteCount
            )
            guard !overflow, next <= maximum else {
                throw resourceLimit(
                    stage: "LIKE regular expression pattern",
                    required: overflow ? UInt64.max : next,
                    maximum: maximum
                )
            }
            requiredByteCount = next
        }

        guard let capacity = Int(exactly: requiredByteCount) else {
            throw resourceLimit(
                stage: "LIKE regular expression pattern",
                required: requiredByteCount,
                maximum: maximum
            )
        }

        // The bounded regex source is the required compilation boundary copy;
        // the LIKE input itself remains a Unicode-scalar view during conversion.
        var result = ""
        result.reserveCapacity(capacity)
        result.append("^")
        for scalar in pattern.unicodeScalars {
            switch scalar.value {
            case 0x25:
                result.append(".*")
            case 0x5F:
                result.append(".")
            case 0x2E, 0x5C, 0x5B, 0x5D, 0x28, 0x29, 0x7B, 0x7D,
                 0x5E, 0x24, 0x2B, 0x3F, 0x7C, 0x2A:
                result.append("\\")
                result.unicodeScalars.append(scalar)
            default:
                result.unicodeScalars.append(scalar)
            }
        }
        result.append("$")
        return result
    }

    private static func regexUTF8Width(of value: UInt32) -> Int {
        switch value {
        case 0...0x7F: return 1
        case 0x80...0x7FF: return 2
        case 0x800...0xFFFF: return 3
        default: return 4
        }
    }

    // MARK: - Type Helpers

    private static func xsdDatatype(_ value: FieldValue) -> String? {
        switch value {
        case .bool: return "http://www.w3.org/2001/XMLSchema#boolean"
        case .int8, .int16, .int32, .int64:
            return "http://www.w3.org/2001/XMLSchema#integer"
        case .uint8, .uint16, .uint32, .uint64:
            return "http://www.w3.org/2001/XMLSchema#unsignedLong"
        case .decimal: return "http://www.w3.org/2001/XMLSchema#decimal"
        case .float32: return "http://www.w3.org/2001/XMLSchema#float"
        case .float64: return "http://www.w3.org/2001/XMLSchema#double"
        case .string: return "http://www.w3.org/2001/XMLSchema#string"
        case .bytes: return "http://www.w3.org/2001/XMLSchema#base64Binary"
        case .date: return "http://www.w3.org/2001/XMLSchema#date"
        case .timestamp:
            return "http://www.w3.org/2001/XMLSchema#dateTime"
        case .uuid, .time, .dateTime, .timeSpan, .calendarPeriod,
             .geographicPoint, .geographicPosition, .vector, .object,
             .reference:
            return nil
        case .rdfTerm(.literal(let literal)):
            return literal.datatypeIRI.rawValue
        case .rdfTerm, .null, .array: return nil
        }
    }

    private static func digestHexadecimalString(
        named name: String,
        for input: String
    ) -> String? {
        switch name {
        case "MD5":
            var accumulator = MD5Accumulator()
            accumulator.update(utf8: input)
            return accumulator.withUnsafeDigestBytes(
                lowercaseHexadecimalString
            )
        case "SHA1":
            var accumulator = SHA1Accumulator()
            accumulator.update(utf8: input)
            return accumulator.withUnsafeDigestBytes(
                lowercaseHexadecimalString
            )
        case "SHA256":
            var accumulator = SHA256Accumulator()
            accumulator.update(utf8: input)
            return accumulator.withUnsafeDigestBytes(
                lowercaseHexadecimalString
            )
        case "SHA384":
            var accumulator = SHA384Accumulator()
            accumulator.update(utf8: input)
            return accumulator.withUnsafeDigestBytes(
                lowercaseHexadecimalString
            )
        case "SHA512":
            var accumulator = SHA512Accumulator()
            accumulator.update(utf8: input)
            return accumulator.withUnsafeDigestBytes(
                lowercaseHexadecimalString
            )
        default:
            return nil
        }
    }

    private static func lowercaseHexadecimalString(
        from source: UnsafeRawBufferPointer
    ) -> String {
        // Hex text is the output boundary, so initialize the String storage directly.
        String(unsafeUninitializedCapacity: source.count * 2) { destination in
            var destinationIndex = 0
            for byte in source {
                destination[destinationIndex] = lowercaseHexDigit(byte >> 4)
                destination[destinationIndex + 1] = lowercaseHexDigit(
                    byte & 0x0F
                )
                destinationIndex += 2
            }
            return destinationIndex
        }
    }

    private static func lowercaseHexDigit(_ nibble: UInt8) -> UInt8 {
        nibble < 10 ? 48 + nibble : 87 + nibble
    }
}

extension ExpressionEvaluator {
    static func evaluateImmediate(
        _ opcode: SPARQLExpressionProgram.Opcode,
        operands: ArraySlice<FieldValue>,
        binding: VariableBinding
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        func operand(
            _ offset: Int
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
            guard offset >= 0, offset < operands.count else {
                throw .runtimeInvariant(
                    "expression opcode received an invalid operand count"
                )
            }
            return operands[operands.startIndex + offset]
        }

        func requireOperandCount(
            _ expected: Int
        ) throws(SPARQLExpressionEvaluationError) {
            guard operands.count == expected else {
                throw .runtimeInvariant(
                    "expression opcode expected \(expected) operands; "
                        + "actual=\(operands.count)"
                )
            }
        }

        func arithmetic(
            _ operation: SPARQLNumericValue.ArithmeticOperation
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
            try requireOperandCount(2)
            guard let left = SPARQLNumericValue(try operand(0)),
                  let right = SPARQLNumericValue(try operand(1)) else {
                throw typeError("arithmetic requires numeric operands")
            }
            do {
                return try left.applying(operation, to: right).fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }
        }

        func comparison(
            _ predicate: (SPARQLComparisonOrder?) -> Bool
        ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
            try requireOperandCount(2)
            return try booleanValue(
                predicate(
                    try compareFieldValues(
                        operand(0),
                        operand(1)
                    )
                )
            )
        }

        switch opcode {
        case .literal(let literal):
            try requireOperandCount(0)
            do throws(SPARQLLiteralConversionError) {
                return try literal.toSPARQLFieldValue()
            } catch let error {
                throw mapLiteralConversionError(error)
            }

        case .column(let column):
            try requireOperandCount(0)
            guard let value = binding[column] else {
                throw .unboundVariable(column)
            }
            return value

        case .variable(let variable):
            try requireOperandCount(0)
            let key = "?\(variable)"
            guard let value = binding[key] else {
                throw .unboundVariable(key)
            }
            return value

        case .parameter:
            throw .unsupportedExpression("unbound parameter")

        case .add:
            return try arithmetic(.add)
        case .subtract:
            return try arithmetic(.subtract)
        case .multiply:
            return try arithmetic(.multiply)
        case .divide:
            return try arithmetic(.divide)
        case .modulo:
            return try arithmetic(.modulo)
        case .negate:
            try requireOperandCount(1)
            guard let numeric = SPARQLNumericValue(try operand(0)) else {
                throw typeError(
                    "unary negation requires a numeric operand"
                )
            }
            do {
                return try numeric.negated().fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }

        case .equal:
            try requireOperandCount(2)
            return try booleanValue(
                equalFieldValues(operand(0), operand(1))
            )
        case .notEqual:
            try requireOperandCount(2)
            return try booleanValue(
                !equalFieldValues(operand(0), operand(1))
            )
        case .lessThan:
            return try comparison { $0 == .ascending }
        case .lessThanOrEqual:
            return try comparison {
                $0 == .ascending || $0 == .same
            }
        case .greaterThan:
            return try comparison { $0 == .descending }
        case .greaterThanOrEqual:
            return try comparison {
                $0 == .descending || $0 == .same
            }

        case .negation:
            try requireOperandCount(1)
            return try booleanValue(
                !effectiveBooleanValue(operand(0))
            )

        case .isNull:
            try requireOperandCount(1)
            return try booleanValue(operand(0) == .null)
        case .isNotNull:
            try requireOperandCount(1)
            return try booleanValue(operand(0) != .null)
        case .bound(let variable):
            try requireOperandCount(0)
            return try booleanValue(binding.isBound("?\(variable)"))

        case .like(let pattern):
            try requireOperandCount(1)
            let string = try evaluateAsString(operand(0))
            return try booleanValue(
                matchRegex(
                    string,
                    pattern: likeToRegex(pattern),
                    flags: nil
                )
            )
        case .regularExpression(let pattern, let flags):
            try requireOperandCount(1)
            let string = try evaluateAsString(operand(0))
            return try booleanValue(
                matchRegex(string, pattern: pattern, flags: flags)
            )
        case .between:
            try requireOperandCount(3)
            let lower = try compareFieldValues(
                operand(0),
                operand(1)
            )
            let upper = try compareFieldValues(
                operand(0),
                operand(2)
            )
            return try booleanValue(
                lower != .ascending && upper != .descending
            )

        case .function(
            let name,
            let identifier,
            let distinct
        ):
            guard !distinct else {
                throw .invalidFunctionArguments(name)
            }
            switch identifier {
            case .builtIn(let builtIn):
                return try evaluateBuiltIn(
                    builtIn,
                    arguments: operands
                )
            case .datatypeConstructor(let datatype):
                try requireOperandCount(1)
                return try SPARQLDatatypeConstructor.evaluate(
                    identifier: datatype,
                    argument: operand(0)
                )
            case .extensionFunction:
                throw .runtimeInvariant(
                    "extension function reached immediate evaluation"
                )
            }

        case .nullIf:
            try requireOperandCount(2)
            let left = try operand(0)
            return try equalFieldValues(left, operand(1))
                ? .null
                : left

        case .triple:
            try requireOperandCount(3)
            guard case .rdfTerm(let subject) = try operand(0),
                  case .rdfTerm(let predicate) = try operand(1),
                  case .rdfTerm(let object) = try operand(2) else {
                throw typeError(
                    "TRIPLE requires RDF subject, predicate, and object terms"
                )
            }
            let validatedSubject: RDFSubject
            switch subject {
            case .iri(let iri):
                validatedSubject = .iri(iri)
            case .blankNode(let identifier):
                validatedSubject = .blankNode(identifier)
            case .literal, .tripleTerm:
                throw typeError(
                    "TRIPLE requires an IRI or blank-node subject"
                )
            }
            guard case .iri(let predicateIRI) = predicate else {
                throw typeError("TRIPLE requires an IRI predicate")
            }
            return .rdfTerm(
                .tripleTerm(
                    subject: validatedSubject,
                    predicate: RDFPredicateIRI(predicateIRI),
                    object: object
                )
            )

        case .isTriple:
            try requireOperandCount(1)
            guard case .rdfTerm(.tripleTerm) = try operand(0) else {
                return try booleanValue(false)
            }
            return try booleanValue(true)

        case .subject:
            try requireOperandCount(1)
            guard case .rdfTerm(
                .tripleTerm(let subject, _, _)
            ) = try operand(0) else {
                throw typeError(
                    "SUBJECT requires an RDF triple term"
                )
            }
            return .rdfTerm(subject.term)

        case .predicate:
            try requireOperandCount(1)
            guard case .rdfTerm(
                .tripleTerm(_, let predicate, _)
            ) = try operand(0) else {
                throw typeError(
                    "PREDICATE requires an RDF triple term"
                )
            }
            return .rdfTerm(predicate.term)

        case .object:
            try requireOperandCount(1)
            guard case .rdfTerm(
                .tripleTerm(_, _, let object)
            ) = try operand(0) else {
                throw typeError("OBJECT requires an RDF triple term")
            }
            return .rdfTerm(object)

        case .cast(let target):
            throw .unsupportedExpression("cast to \(target)")
        case .unsupported(let name):
            throw .unsupportedExpression(name)

        case .conjunction, .disjunction, .membership,
             .conditional, .coalesce, .caseSelection, .exists:
            throw .runtimeInvariant(
                "lazy expression opcode reached immediate evaluation"
            )
        }
    }

    static func canonicalBoolean(
        _ value: Bool
    ) throws(SPARQLExpressionEvaluationError) -> FieldValue {
        try booleanValue(value)
    }
}

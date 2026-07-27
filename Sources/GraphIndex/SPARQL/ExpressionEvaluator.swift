// ExpressionEvaluator.swift
// GraphIndex - Evaluates DatabaseKit.Expression against VariableBinding

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
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

    private init() {}

    /// Convert a canonical sigil-free QueryIR variable into a binding key.
    private static func bindingKey(_ v: Variable) -> String {
        "?\(v.name)"
    }

    // MARK: - Boolean Evaluation (FILTER)

    /// Evaluate an expression as a boolean for FILTER.
    ///
    /// Per SPARQL §17.2, evaluation errors yield `false`.
    /// This is the primary entry point for FILTER clause evaluation.
    public static func evaluateAsBoolean(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> Bool {
        do {
            return try effectiveBooleanValue(
                evaluate(expr, binding: binding)
            )
        } catch let error as SPARQLExpressionEvaluationError
            where error.isSPARQLEvaluationError {
            return false
        }
    }

    /// Evaluates an ORDER BY expression. SPARQL expression errors produce an
    /// unbound sort key; resource and runtime failures remain thrown.
    public static func evaluateForOrdering(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> FieldValue? {
        do {
            return try evaluate(expr, binding: binding)
        } catch let error as SPARQLExpressionEvaluationError
            where error.isSPARQLEvaluationError {
            return nil
        }
    }

    // MARK: - General Evaluation

    /// Evaluate an expression to a FieldValue.
    ///
    /// DatabaseKit.Expression errors are typed and never collapsed into an absent value.
    public static func evaluate(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> FieldValue {
        switch expr {
        // Identifiers
        case .variable(let v):
            let key = bindingKey(v)
            guard let value = binding[key] else {
                throw SPARQLExpressionEvaluationError.unboundVariable(key)
            }
            return value
        case .column(let col):
            guard let value = binding[col.column] else {
                throw SPARQLExpressionEvaluationError.unboundVariable(col.column)
            }
            return value
        case .literal(let lit):
            do {
                return try lit.toSPARQLFieldValue()
            } catch let error as SPARQLLiteralConversionError {
                throw mapLiteralConversionError(error)
            }

        // Arithmetic
        case .add(let lhs, let rhs):
            return try numericBinary(lhs, rhs, binding: binding, operation: .add)
        case .subtract(let lhs, let rhs):
            return try numericBinary(lhs, rhs, binding: binding, operation: .subtract)
        case .multiply(let lhs, let rhs):
            return try numericBinary(lhs, rhs, binding: binding, operation: .multiply)
        case .divide(let lhs, let rhs):
            return try numericBinary(lhs, rhs, binding: binding, operation: .divide)
        case .modulo(let lhs, let rhs):
            return try numericBinary(lhs, rhs, binding: binding, operation: .modulo)
        case .negate(let inner):
            let value = try evaluate(inner, binding: binding)
            guard let numeric = SPARQLNumericValue(value) else {
                throw typeError("unary negation requires a numeric operand")
            }
            do {
                return try numeric.negated().fieldValue()
            } catch let error {
                throw mapNumericError(error)
            }

        // Comparisons → Bool
        case .equal(let lhs, let rhs):
            return try booleanValue(equalValues(lhs, rhs, binding: binding))
        case .notEqual(let lhs, let rhs):
            return try booleanValue(!equalValues(lhs, rhs, binding: binding))
        case .lessThan(let lhs, let rhs):
            let comparison = try compareValues(lhs, rhs, binding: binding)
            return try booleanValue(comparison == .orderedAscending)
        case .lessThanOrEqual(let lhs, let rhs):
            let comparison = try compareValues(lhs, rhs, binding: binding)
            return try booleanValue(
                comparison == .orderedAscending || comparison == .orderedSame
            )
        case .greaterThan(let lhs, let rhs):
            let comparison = try compareValues(lhs, rhs, binding: binding)
            return try booleanValue(comparison == .orderedDescending)
        case .greaterThanOrEqual(let lhs, let rhs):
            let comparison = try compareValues(lhs, rhs, binding: binding)
            return try booleanValue(
                comparison == .orderedDescending || comparison == .orderedSame
            )

        // Logical
        case .and(let lhs, let rhs):
            return try evaluateLogicalAnd(lhs, rhs, binding: binding)
        case .or(let lhs, let rhs):
            return try evaluateLogicalOr(lhs, rhs, binding: binding)
        case .not(let inner):
            return try booleanValue(
                !effectiveBooleanValue(evaluate(inner, binding: binding))
            )

        // Null / Bound checks
        case .isNull(let inner):
            return try booleanValue(evaluate(inner, binding: binding) == .null)
        case .isNotNull(let inner):
            return try booleanValue(evaluate(inner, binding: binding) != .null)
        case .bound(let v):
            return try booleanValue(binding.isBound(bindingKey(v)))

        // Pattern matching
        case .regex(let inner, let pattern, let flags):
            let string = try evaluateAsString(inner, binding: binding)
            return try booleanValue(
                matchRegex(string, pattern: pattern, flags: flags)
            )
        case .like(let inner, let pattern):
            let string = try evaluateAsString(inner, binding: binding)
            let regex = try likeToRegex(pattern)
            return try booleanValue(
                matchRegex(string, pattern: regex, flags: nil)
            )

        // IN list
        case .inList(let inner, let values):
            return try evaluateInList(
                inner,
                values: values,
                binding: binding,
                negated: false
            )

        // NOT IN list
        case .notInList(let inner, let values):
            return try evaluateInList(
                inner,
                values: values,
                binding: binding,
                negated: true
            )

        // Functions
        case .function(let call):
            return try evaluateFunction(call, binding: binding)

        // Conditional
        case .caseWhen(let cases, let elseResult):
            for pair in cases {
                let condition = try effectiveBooleanValue(
                    evaluate(pair.condition, binding: binding)
                )
                if condition {
                    return try evaluate(pair.result, binding: binding)
                }
            }
            if let elseExpr = elseResult {
                return try evaluate(elseExpr, binding: binding)
            }
            return .null

        case .coalesce(let exprs):
            for expr in exprs {
                do {
                    let value = try evaluate(expr, binding: binding)
                    if value != .null { return value }
                } catch let error as SPARQLExpressionEvaluationError
                    where error.isSPARQLEvaluationError {
                    continue
                }
            }
            throw typeError("COALESCE has no expression without an error")

        case .nullIf(let lhs, let rhs):
            let left = try evaluate(lhs, binding: binding)
            let right = try evaluate(rhs, binding: binding)
            return try equalFieldValues(left, right) ? .null : left

        // RDF-star operations (W3C RDF-star / SPARQL-star)
        case .triple(let s, let p, let o):
            let sv = try evaluate(s, binding: binding)
            let pv = try evaluate(p, binding: binding)
            let ov = try evaluate(o, binding: binding)
            guard
                  case .rdfTerm(let subject) = sv,
                  case .rdfTerm(let predicate) = pv,
                  case .rdfTerm(let object) = ov else {
                throw typeError("TRIPLE requires RDF subject, predicate, and object terms")
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

        case .isTriple(let e):
            let value = try evaluate(e, binding: binding)
            guard case .rdfTerm(.tripleTerm) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)

        case .subject(let e):
            let value = try evaluate(e, binding: binding)
            guard case .rdfTerm(.tripleTerm(let subject, _, _)) = value else {
                throw typeError("SUBJECT requires an RDF triple term")
            }
            return .rdfTerm(subject.term)

        case .predicate(let e):
            let value = try evaluate(e, binding: binding)
            guard case .rdfTerm(.tripleTerm(_, let predicate, _)) = value else {
                throw typeError("PREDICATE requires an RDF triple term")
            }
            return .rdfTerm(predicate.term)

        case .object(let e):
            let value = try evaluate(e, binding: binding)
            guard case .rdfTerm(.tripleTerm(_, _, let object)) = value else {
                throw typeError("OBJECT requires an RDF triple term")
            }
            return .rdfTerm(object)

        case .between(let value, let low, let high):
            let lower = try compareValues(value, low, binding: binding)
            let upper = try compareValues(value, high, binding: binding)
            return try booleanValue(
                lower != .orderedAscending && upper != .orderedDescending
            )

        case .parameter, .inSubquery, .aggregate, .cast, .subquery, .exists:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                String(describing: expr)
            )
        }
    }

    // MARK: - Built-in Functions

    private static func evaluateFunction(
        _ call: FunctionCall,
        binding: VariableBinding
    ) throws -> FieldValue {
        let args = call.arguments
        let identifier: SPARQLFunctionIdentifier
        do {
            identifier = try SPARQLFunctionIdentifier.resolve(call.name)
        } catch {
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                "function \(call.name)"
            )
        }

        switch identifier {
        case .extensionFunction:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                "extension function \(call.name) requires a runtime resolver"
            )
        case .datatypeConstructor(let datatype):
            try requireArgumentCount(
                args,
                count: 1,
                function: datatype.rawValue
            )
            return try SPARQLDatatypeConstructor.evaluate(
                identifier: datatype,
                argument: evaluate(args[0], binding: binding)
            )
        case .builtIn(let builtIn):
            return try evaluateBuiltIn(
                builtIn,
                arguments: args,
                binding: binding
            )
        }
    }

    private static func evaluateBuiltIn(
        _ builtIn: SPARQLFunctionIdentifier.BuiltIn,
        arguments args: [DatabaseKit.Expression],
        binding: VariableBinding
    ) throws -> FieldValue {
        let name = builtIn.rawValue

        switch name {
        case "STR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard let string = stringRepresentation(value) else {
                throw typeError("STR requires an RDF term or scalar value")
            }
            return try stringValue(string)
        case "STRLEN":
            try requireArgumentCount(args, count: 1, function: name)
            let string = try evaluateAsString(args[0], binding: binding)
            guard let length = Int64(exactly: string.unicodeScalars.count) else {
                throw resourceLimit(stage: "STRLEN", required: UInt64(string.utf8.count))
            }
            return try integerValue(length)
        case "UCASE":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluateStringValue(args[0], binding: binding)
            return try stringValue(
                value.replacingLexicalForm(value.lexicalForm.uppercased())
            )
        case "LCASE":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluateStringValue(args[0], binding: binding)
            return try stringValue(
                value.replacingLexicalForm(value.lexicalForm.lowercased())
            )
        case "CONTAINS":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateStringValue(args[0], binding: binding)
            let substring = try evaluateStringValue(args[1], binding: binding)
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
            let string = try evaluateStringValue(args[0], binding: binding)
            let prefix = try evaluateStringValue(args[1], binding: binding)
            guard string.acceptsArgument(prefix) else {
                throw typeError("STRSTARTS arguments have incompatible language annotations")
            }
            return try booleanValue(
                string.lexicalForm.hasPrefix(prefix.lexicalForm)
            )
        case "STRENDS":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateStringValue(args[0], binding: binding)
            let suffix = try evaluateStringValue(args[1], binding: binding)
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
            let string = try evaluateStringValue(args[0], binding: binding)
            let start = try evaluateAsInt64(args[1], binding: binding)
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
                let length = try evaluateAsInt64(args[2], binding: binding)
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
            let values = try args.map {
                try evaluateStringValue($0, binding: binding)
            }
            let requiredUTF8Count = try values.reduce(UInt64(0)) { partial, value in
                let count = UInt64(value.lexicalForm.utf8.count)
                let (sum, overflow) = partial.addingReportingOverflow(count)
                guard !overflow else {
                    throw resourceLimit(stage: "CONCAT", required: UInt64.max)
                }
                return sum
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
            let string = try evaluateStringValue(args[0], binding: binding)
            let pattern = try evaluateStringValue(args[1], binding: binding)
            let replacement = try evaluateStringValue(args[2], binding: binding)
            guard pattern.kind == .string, replacement.kind == .string else {
                throw typeError("REPLACE pattern and replacement must be xsd:string")
            }
            let flags = args.count == 4
                ? try evaluateAsString(args[3], binding: binding)
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
            let source = try evaluateStringValue(args[0], binding: binding)
            let search = try evaluateStringValue(args[1], binding: binding)
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
            let value = try evaluateStringValue(args[0], binding: binding)
            guard value.kind == .string else {
                throw typeError("ENCODE_FOR_URI requires an xsd:string argument")
            }
            return try stringValue(percentEncodeForURI(value.lexicalForm))

        case "IRI", "URI":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            if case .rdfTerm(.iri) = value { return value }
            guard let string = SPARQLStringValue(value),
                  string.kind == .string else {
                throw typeError("\(name) requires an IRI or xsd:string argument")
            }
            do {
                return .rdfTerm(.iri(try RDFIRI(string.lexicalForm)))
            } catch {
                throw typeError("\(name) requires an absolute IRI when no base IRI is configured")
            }

        case "LANGMATCHES":
            try requireArgumentCount(args, count: 2, function: name)
            let language = try evaluateAsString(args[0], binding: binding)
            let range = try evaluateAsString(args[1], binding: binding)
            return try booleanValue(languageMatches(language, range: range))

        case "SAMETERM":
            try requireArgumentCount(args, count: 2, function: name)
            let lhs = try evaluate(args[0], binding: binding)
            let rhs = try evaluate(args[1], binding: binding)
            guard case .rdfTerm(let lhsTerm) = lhs,
                  case .rdfTerm(let rhsTerm) = rhs else {
                throw typeError("sameTerm requires RDF terms")
            }
            return try booleanValue(lhsTerm == rhsTerm)

        case "STRDT":
            try requireArgumentCount(args, count: 2, function: name)
            let lexical = try evaluateStringValue(args[0], binding: binding)
            let datatype = try evaluate(args[1], binding: binding)
            guard lexical.kind == .string,
                  case .rdfTerm(.iri(let datatypeIRI)) = datatype else {
                throw typeError("STRDT requires xsd:string and IRI arguments")
            }
            do {
                return try Literal.typedLiteral(
                    value: lexical.lexicalForm,
                    datatype: datatypeIRI.rawValue
                ).toSPARQLFieldValue()
            } catch let error as SPARQLLiteralConversionError {
                throw mapLiteralConversionError(error)
            }

        case "STRLANG":
            try requireArgumentCount(args, count: 2, function: name)
            let lexical = try evaluateStringValue(args[0], binding: binding)
            let language = try evaluateStringValue(args[1], binding: binding)
            guard lexical.kind == .string, language.kind == .string else {
                throw typeError("STRLANG requires xsd:string arguments")
            }
            do {
                return try Literal.langLiteral(
                    value: lexical.lexicalForm,
                    language: language.lexicalForm
                ).toSPARQLFieldValue()
            } catch let error as SPARQLLiteralConversionError {
                throw mapLiteralConversionError(error)
            }

        case "REGEX":
            guard args.count == 2 || args.count == 3 else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            let text = try evaluateAsString(args[0], binding: binding)
            let pattern = try evaluateStringValue(args[1], binding: binding)
            guard pattern.kind == .string else {
                throw typeError("REGEX pattern must be xsd:string")
            }
            let flags: String?
            if args.count == 3 {
                let flagValue = try evaluateStringValue(args[2], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.iri) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISBLANK":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.blankNode) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISLITERAL":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.literal) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(true)
        case "ISNUMERIC":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            return try booleanValue(SPARQLNumericValue(value) != nil)

        case "MD5", "SHA1", "SHA256", "SHA384", "SHA512":
            try requireArgumentCount(args, count: 1, function: name)
            let string = try evaluateStringValue(args[0], binding: binding)
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
            let value = try evaluate(args[0], binding: binding)
            guard let datatype = xsdDatatype(value) else {
                throw typeError("DATATYPE requires a literal")
            }
            return .rdfTerm(.iri(try RDFIRI(datatype)))

        case "LANG":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.literal(let literal)) = value else {
                throw typeError("LANG requires an RDF literal")
            }
            return try stringValue(literal.languageTag?.rawValue ?? "")

        case "LANGDIR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.literal(let literal)) = value else {
                throw typeError("LANGDIR requires an RDF literal")
            }
            return try stringValue(literal.baseDirection?.rawValue ?? "")

        case "HASLANG":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.literal(let literal)) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(literal.languageTag != nil)

        case "HASLANGDIR":
            try requireArgumentCount(args, count: 1, function: name)
            let value = try evaluate(args[0], binding: binding)
            guard case .rdfTerm(.literal(let literal)) = value else {
                return try booleanValue(false)
            }
            return try booleanValue(literal.baseDirection != nil)

        case "TRIGRAM_SIM":
            try requireArgumentCount(args, count: 2, function: name)
            let string = try evaluateAsString(args[0], binding: binding)
            let pattern = try evaluateAsString(args[1], binding: binding)
            guard let numeric = SPARQLNumericValue(
                .float64(TrigramSimilarity.score(string, pattern))
            ) else {
                throw SPARQLExpressionEvaluationError.runtimeInvariant(
                    "TRIGRAM_SIM result was not numeric"
                )
            }
            return try numeric.fieldValue()

        case "STRLANGDIR":
            try requireArgumentCount(args, count: 3, function: name)
            let string = try evaluateAsString(args[0], binding: binding)
            let languageValue = try evaluateAsString(args[1], binding: binding)
            let directionValue = try evaluateAsString(args[2], binding: binding)
            do {
                let language = try RDFLanguageTag(languageValue)
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
            } catch let error as SPARQLExpressionEvaluationError {
                throw error
            } catch {
                throw typeError("STRLANGDIR language tag is invalid")
            }

        case "IF":
            try requireArgumentCount(args, count: 3, function: name)
            let condition = try effectiveBooleanValue(
                evaluate(args[0], binding: binding)
            )
            return try evaluate(args[condition ? 1 : 2], binding: binding)

        case "BOUND":
            try requireArgumentCount(args, count: 1, function: name)
            guard case .variable(let variable) = args[0] else {
                throw SPARQLExpressionEvaluationError.invalidFunctionArguments(name)
            }
            return try booleanValue(binding.isBound(bindingKey(variable)))

        case "COALESCE":
            for arg in args {
                do {
                    let value = try evaluate(arg, binding: binding)
                    if value != .null { return value }
                } catch let error as SPARQLExpressionEvaluationError
                    where error.isSPARQLEvaluationError {
                    continue
                }
            }
            throw typeError("COALESCE has no expression without an error")

        default:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                "function \(name)"
            )
        }
    }

    // MARK: - Helpers

    private static func evaluateAsString(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> String {
        try evaluateStringValue(expr, binding: binding).lexicalForm
    }

    private static func evaluateStringValue(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> SPARQLStringValue {
        let value = try evaluate(expr, binding: binding)
        guard let string = SPARQLStringValue(value) else {
            throw typeError("string function requires a string literal")
        }
        return string
    }

    private static func evaluateAsInt64(
        _ expr: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> Int64 {
        let value = try evaluate(expr, binding: binding)
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

    static func effectiveBooleanValue(_ value: FieldValue) throws -> Bool {
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

    private enum EffectiveBooleanResult {
        case value(Bool)
        case expressionError(SPARQLExpressionEvaluationError)
    }

    private static func recoverableEffectiveBooleanValue(
        _ expression: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> EffectiveBooleanResult {
        do {
            return try .value(
                effectiveBooleanValue(evaluate(expression, binding: binding))
            )
        } catch let error as SPARQLExpressionEvaluationError
            where error.isSPARQLEvaluationError {
            return .expressionError(error)
        }
    }

    private static func evaluateLogicalAnd(
        _ lhs: DatabaseKit.Expression,
        _ rhs: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> FieldValue {
        let left = try recoverableEffectiveBooleanValue(lhs, binding: binding)
        switch left {
        case .value(false):
            return try booleanValue(false)
        case .value(true):
            return try booleanValue(
                effectiveBooleanValue(evaluate(rhs, binding: binding))
            )
        case .expressionError(let leftError):
            let right = try recoverableEffectiveBooleanValue(rhs, binding: binding)
            if case .value(false) = right {
                return try booleanValue(false)
            }
            throw leftError
        }
    }

    private static func evaluateLogicalOr(
        _ lhs: DatabaseKit.Expression,
        _ rhs: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> FieldValue {
        let left = try recoverableEffectiveBooleanValue(lhs, binding: binding)
        switch left {
        case .value(true):
            return try booleanValue(true)
        case .value(false):
            return try booleanValue(
                effectiveBooleanValue(evaluate(rhs, binding: binding))
            )
        case .expressionError(let leftError):
            let right = try recoverableEffectiveBooleanValue(rhs, binding: binding)
            if case .value(true) = right {
                return try booleanValue(true)
            }
            throw leftError
        }
    }

    private static func evaluateInList(
        _ expression: DatabaseKit.Expression,
        values: [DatabaseKit.Expression],
        binding: VariableBinding,
        negated: Bool
    ) throws -> FieldValue {
        let value = try evaluate(expression, binding: binding)
        var firstExpressionError: SPARQLExpressionEvaluationError?
        for candidateExpression in values {
            do {
                let candidate = try evaluate(candidateExpression, binding: binding)
                if try equalFieldValues(value, candidate) {
                    return try booleanValue(!negated)
                }
            } catch let error as SPARQLExpressionEvaluationError
                where error.isSPARQLEvaluationError {
                if firstExpressionError == nil { firstExpressionError = error }
            }
        }
        if let firstExpressionError { throw firstExpressionError }
        return try booleanValue(negated)
    }

    private static func equalValues(
        _ lhs: DatabaseKit.Expression,
        _ rhs: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> Bool {
        try equalFieldValues(
            evaluate(lhs, binding: binding),
            evaluate(rhs, binding: binding)
        )
    }

    private static func compareValues(
        _ lhs: DatabaseKit.Expression,
        _ rhs: DatabaseKit.Expression,
        binding: VariableBinding
    ) throws -> ComparisonResult? {
        try compareFieldValues(
            evaluate(lhs, binding: binding),
            evaluate(rhs, binding: binding)
        )
    }

    private static func compareFieldValues(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> ComparisonResult? {
        guard left != .null, right != .null else {
            throw typeError("null values are not comparable")
        }
        if case .rdfTerm(.literal(let leftLiteral)) = left,
           case .rdfTerm(.literal(let rightLiteral)) = right {
            do {
                switch try SPARQLValueComparator().compare(
                    leftLiteral,
                    rightLiteral
                ) {
                case .less: return .orderedAscending
                case .equal: return .orderedSame
                case .greater: return .orderedDescending
                case .unordered:
                    return nil
                case .typeError:
                    throw typeError("RDF literals are not order-comparable")
                }
            } catch let failure as XSDValidationFailure {
                throw mapXSDValidationFailure(failure)
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
            return .orderedAscending
        case .equal:
            return .orderedSame
        case .greaterThan:
            return .orderedDescending
        }
    }

    static func equalFieldValues(
        _ left: FieldValue,
        _ right: FieldValue
    ) throws -> Bool {
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
            return comparison == .orderedSame
        }
        return left == right
    }

    private static func equalRDFTerms(
        _ left: RDFTerm,
        _ right: RDFTerm
    ) throws -> Bool {
        switch (left, right) {
        case (.iri(let lhs), .iri(let rhs)):
            return lhs == rhs
        case (.blankNode(let lhs), .blankNode(let rhs)):
            return lhs == rhs
        case (.literal(let lhs), .literal(let rhs)):
            if lhs.languageTag != nil || rhs.languageTag != nil
                || lhs.baseDirection != nil || rhs.baseDirection != nil {
                return lhs == rhs
            }
            do {
                switch try SPARQLValueComparator().compare(lhs, rhs) {
                case .equal: return true
                case .less, .greater, .unordered: return false
                case .typeError:
                    throw typeError("RDF literals are not value-comparable")
                }
            } catch let failure as XSDValidationFailure {
                throw mapXSDValidationFailure(failure)
            }
        case (
            .tripleTerm(let leftSubject, let leftPredicate, let leftObject),
            .tripleTerm(let rightSubject, let rightPredicate, let rightObject)
        ):
            return try equalRDFTerms(leftSubject.term, rightSubject.term)
                && equalRDFTerms(leftPredicate.term, rightPredicate.term)
                && equalRDFTerms(leftObject, rightObject)
        default:
            return false
        }
    }

    private static func numericBinary(
        _ lhs: DatabaseKit.Expression,
        _ rhs: DatabaseKit.Expression,
        binding: VariableBinding,
        operation: SPARQLNumericValue.ArithmeticOperation
    ) throws -> FieldValue {
        let lhsValue = try evaluate(lhs, binding: binding)
        let rhsValue = try evaluate(rhs, binding: binding)
        guard let left = SPARQLNumericValue(lhsValue),
              let right = SPARQLNumericValue(rhsValue) else {
            throw typeError("arithmetic requires numeric operands")
        }
        do {
            return try left.applying(operation, to: right).fieldValue()
        } catch let error {
            throw mapNumericError(error)
        }
    }

    private static func requireArgumentCount(
        _ arguments: [DatabaseKit.Expression],
        count: Int,
        function: String
    ) throws {
        guard arguments.count == count else {
            throw SPARQLExpressionEvaluationError.invalidFunctionArguments(function)
        }
    }

    private static func booleanValue(_ value: Bool) throws -> FieldValue {
        try rdfLiteralValue(
            value ? "true" : "false",
            datatype: xsdNamespace + "boolean"
        )
    }

    private static func integerValue(_ value: Int64) throws -> FieldValue {
        try rdfLiteralValue(
            String(value),
            datatype: xsdNamespace + "integer"
        )
    }

    private static func stringValue(_ value: String) throws -> FieldValue {
        let count = UInt64(value.utf8.count)
        guard count <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
            throw resourceLimit(stage: "string result", required: count)
        }
        return try rdfLiteralValue(value, datatype: xsdNamespace + "string")
    }

    private static func stringValue(
        _ value: SPARQLStringValue
    ) throws -> FieldValue {
        let count = UInt64(value.lexicalForm.utf8.count)
        guard count <= SPARQLExecutionLimits.maximumLiteralUTF8Count else {
            throw resourceLimit(stage: "string result", required: count)
        }
        return try value.fieldValue()
    }

    private static func rdfLiteralValue(
        _ lexicalForm: String,
        datatype: String
    ) throws -> FieldValue {
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
            return .typeError(String(describing: error))
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
            return .typeError(String(describing: error))
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
    ) throws -> Bool {
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
    ) throws -> String {
        try SPARQLRegularExpression.evaluateReplacement(
            string,
            pattern: pattern,
            replacement: replacement,
            flags: flags
        )
    }

    private static func likeToRegex(_ pattern: String) throws -> String {
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

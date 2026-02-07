// ExpressionEvaluator.swift
// GraphIndex - Evaluates QueryIR.Expression against VariableBinding

import Foundation
import QueryIR
import Core
import DatabaseEngine

/// Evaluates QueryIR.Expression against a VariableBinding (SPARQL solution row).
///
/// This bridges QueryIR's unified expression representation to GraphIndex's
/// VariableBinding-based evaluation. Used for FILTER clause evaluation
/// when expressions are represented as QueryIR types rather than FilterExpression.
///
/// Follows SPARQL §17.2 Effective Boolean Value semantics:
/// - Errors in filter expressions evaluate to `false` (not thrown)
/// - Null in comparisons yields `false`
/// - Type promotion for numeric comparisons
public struct ExpressionEvaluator: Sendable {

    private init() {}

    /// Normalize a QueryIR variable name to VariableBinding key format ("?"-prefixed).
    ///
    /// QueryIR.Variable strips the "?" prefix during init, but VariableBinding
    /// keys use "?"-prefixed names (set by GraphPatternConverter.convertTerm).
    private static func bindingKey(_ v: QueryIR.Variable) -> String {
        v.name.hasPrefix("?") ? v.name : "?\(v.name)"
    }

    // MARK: - Boolean Evaluation (FILTER)

    /// Evaluate an expression as a boolean for FILTER.
    ///
    /// Per SPARQL §17.2, evaluation errors yield `false`.
    /// This is the primary entry point for FILTER clause evaluation.
    public static func evaluateAsBoolean(
        _ expr: QueryIR.Expression,
        binding: VariableBinding
    ) -> Bool {
        guard let value = evaluate(expr, binding: binding) else {
            return false
        }
        return effectiveBooleanValue(value)
    }

    // MARK: - General Evaluation

    /// Evaluate an expression to a FieldValue.
    ///
    /// Returns `nil` on evaluation error (type mismatch, unbound variable, etc.).
    public static func evaluate(
        _ expr: QueryIR.Expression,
        binding: VariableBinding
    ) -> FieldValue? {
        switch expr {
        // Identifiers
        case .variable(let v):
            return binding[bindingKey(v)]
        case .column(let col):
            // Treat column references as variable lookups in SPARQL context
            return binding[col.column]
        case .literal(let lit):
            return lit.toSPARQLFieldValue()

        // Arithmetic
        case .add(let lhs, let rhs):
            return numericBinary(lhs, rhs, binding: binding, op: +)
        case .subtract(let lhs, let rhs):
            return numericBinary(lhs, rhs, binding: binding, op: -)
        case .multiply(let lhs, let rhs):
            return numericBinary(lhs, rhs, binding: binding, op: *)
        case .divide(let lhs, let rhs):
            guard let r = evaluateAsDouble(rhs, binding: binding), r != 0 else { return nil }
            return numericBinary(lhs, rhs, binding: binding, op: /)
        case .modulo(let lhs, let rhs):
            guard let l = evaluateAsInt64(lhs, binding: binding),
                  let r = evaluateAsInt64(rhs, binding: binding), r != 0 else { return nil }
            return .int64(l % r)
        case .negate(let inner):
            guard let val = evaluate(inner, binding: binding) else { return nil }
            switch val {
            case .int64(let v): return .int64(-v)
            case .double(let v): return .double(-v)
            default: return nil
            }

        // Comparisons → Bool
        case .equal(let lhs, let rhs):
            return boolResult(compareValues(lhs, rhs, binding: binding) == .orderedSame)
        case .notEqual(let lhs, let rhs):
            return boolResult(compareValues(lhs, rhs, binding: binding) != .orderedSame)
        case .lessThan(let lhs, let rhs):
            return boolResult(compareValues(lhs, rhs, binding: binding) == .orderedAscending)
        case .lessThanOrEqual(let lhs, let rhs):
            guard let cmp = compareValues(lhs, rhs, binding: binding) else { return nil }
            return boolResult(cmp == .orderedAscending || cmp == .orderedSame)
        case .greaterThan(let lhs, let rhs):
            return boolResult(compareValues(lhs, rhs, binding: binding) == .orderedDescending)
        case .greaterThanOrEqual(let lhs, let rhs):
            guard let cmp = compareValues(lhs, rhs, binding: binding) else { return nil }
            return boolResult(cmp == .orderedDescending || cmp == .orderedSame)

        // Logical
        case .and(let lhs, let rhs):
            let left = evaluateAsBoolean(lhs, binding: binding)
            let right = evaluateAsBoolean(rhs, binding: binding)
            return .bool(left && right)
        case .or(let lhs, let rhs):
            let left = evaluateAsBoolean(lhs, binding: binding)
            let right = evaluateAsBoolean(rhs, binding: binding)
            return .bool(left || right)
        case .not(let inner):
            return .bool(!evaluateAsBoolean(inner, binding: binding))

        // Null / Bound checks
        case .isNull(let inner):
            return .bool(evaluate(inner, binding: binding) == nil || evaluate(inner, binding: binding) == .null)
        case .isNotNull(let inner):
            guard let val = evaluate(inner, binding: binding) else { return .bool(false) }
            return .bool(val != .null)
        case .bound(let v):
            return .bool(binding.isBound(bindingKey(v)))

        // Pattern matching
        case .regex(let inner, let pattern, let flags):
            guard let str = evaluateAsString(inner, binding: binding) else { return nil }
            return .bool(matchRegex(str, pattern: pattern, flags: flags))
        case .like(let inner, let pattern):
            guard let str = evaluateAsString(inner, binding: binding) else { return nil }
            let regex = likeToRegex(pattern)
            return .bool(matchRegex(str, pattern: regex, flags: nil))

        // IN list
        case .inList(let inner, let values):
            guard let val = evaluate(inner, binding: binding) else { return nil }
            for v in values {
                if let candidate = evaluate(v, binding: binding),
                   val.isEqual(to: candidate) {
                    return .bool(true)
                }
            }
            return .bool(false)

        // Functions
        case .function(let call):
            return evaluateFunction(call, binding: binding)

        // Conditional
        case .caseWhen(let cases, let elseResult):
            for pair in cases {
                if evaluateAsBoolean(pair.condition, binding: binding) {
                    return evaluate(pair.result, binding: binding)
                }
            }
            if let elseExpr = elseResult {
                return evaluate(elseExpr, binding: binding)
            }
            return .null

        case .coalesce(let exprs):
            for expr in exprs {
                if let val = evaluate(expr, binding: binding), val != .null {
                    return val
                }
            }
            return .null

        case .nullIf(let lhs, let rhs):
            guard let l = evaluate(lhs, binding: binding),
                  let r = evaluate(rhs, binding: binding) else { return nil }
            return l.isEqual(to: r) ? .null : l

        // RDF-star operations (W3C RDF-star / SPARQL-star)
        case .triple(let s, let p, let o):
            guard let sv = evaluate(s, binding: binding),
                  let pv = evaluate(p, binding: binding),
                  let ov = evaluate(o, binding: binding) else { return nil }
            return .string(QuotedTripleEncoding.encode(subject: sv, predicate: pv, object: ov))

        case .isTriple(let e):
            guard let val = evaluate(e, binding: binding) else { return nil }
            if case .string(let s) = val {
                return .bool(QuotedTripleEncoding.isQuotedTriple(s))
            }
            return .bool(false)

        case .subject(let e):
            guard let val = evaluate(e, binding: binding),
                  case .string(let s) = val,
                  let components = QuotedTripleEncoding.decode(s) else { return nil }
            return components.subject

        case .predicate(let e):
            guard let val = evaluate(e, binding: binding),
                  case .string(let s) = val,
                  let components = QuotedTripleEncoding.decode(s) else { return nil }
            return components.predicate

        case .object(let e):
            guard let val = evaluate(e, binding: binding),
                  case .string(let s) = val,
                  let components = QuotedTripleEncoding.decode(s) else { return nil }
            return components.object

        // Unsupported (subqueries, aggregates, cast, etc.)
        default:
            return nil
        }
    }

    // MARK: - Built-in Functions

    private static func evaluateFunction(
        _ call: QueryIR.FunctionCall,
        binding: VariableBinding
    ) -> FieldValue? {
        let name = call.name.uppercased()
        let args = call.arguments

        switch name {
        // String functions
        case "STR":
            guard args.count == 1,
                  let val = evaluate(args[0], binding: binding) else { return nil }
            return .string(stringRepresentation(val))
        case "STRLEN":
            guard args.count == 1,
                  let str = evaluateAsString(args[0], binding: binding) else { return nil }
            return .int64(Int64(str.count))
        case "UCASE":
            guard args.count == 1,
                  let str = evaluateAsString(args[0], binding: binding) else { return nil }
            return .string(str.uppercased())
        case "LCASE":
            guard args.count == 1,
                  let str = evaluateAsString(args[0], binding: binding) else { return nil }
            return .string(str.lowercased())
        case "CONTAINS":
            guard args.count == 2,
                  let str = evaluateAsString(args[0], binding: binding),
                  let substr = evaluateAsString(args[1], binding: binding) else { return nil }
            return .bool(str.contains(substr))
        case "STRSTARTS":
            guard args.count == 2,
                  let str = evaluateAsString(args[0], binding: binding),
                  let prefix = evaluateAsString(args[1], binding: binding) else { return nil }
            return .bool(str.hasPrefix(prefix))
        case "STRENDS":
            guard args.count == 2,
                  let str = evaluateAsString(args[0], binding: binding),
                  let suffix = evaluateAsString(args[1], binding: binding) else { return nil }
            return .bool(str.hasSuffix(suffix))
        case "SUBSTR":
            guard args.count >= 2,
                  let str = evaluateAsString(args[0], binding: binding),
                  let start = evaluateAsInt64(args[1], binding: binding) else { return nil }
            let startIndex = max(Int(start) - 1, 0)  // SPARQL is 1-based
            guard startIndex <= str.count else { return .string("") }
            let from = str.index(str.startIndex, offsetBy: startIndex)
            if args.count >= 3, let length = evaluateAsInt64(args[2], binding: binding) {
                let endOffset = min(startIndex + Int(length), str.count)
                let to = str.index(str.startIndex, offsetBy: endOffset)
                return .string(String(str[from..<to]))
            }
            return .string(String(str[from...]))
        case "CONCAT":
            var result = ""
            for arg in args {
                guard let str = evaluateAsString(arg, binding: binding) else { return nil }
                result += str
            }
            return .string(result)
        case "REPLACE":
            guard args.count >= 3,
                  let str = evaluateAsString(args[0], binding: binding),
                  let pattern = evaluateAsString(args[1], binding: binding),
                  let replacement = evaluateAsString(args[2], binding: binding) else { return nil }
            let flags = args.count >= 4 ? evaluateAsString(args[3], binding: binding) : nil
            return .string(replaceRegex(str, pattern: pattern, replacement: replacement, flags: flags))

        // Numeric functions
        case "ABS":
            guard args.count == 1, let val = evaluate(args[0], binding: binding) else { return nil }
            switch val {
            case .int64(let v): return .int64(abs(v))
            case .double(let v): return .double(abs(v))
            default: return nil
            }
        case "ROUND":
            guard args.count == 1, let v = evaluateAsDouble(args[0], binding: binding) else { return nil }
            return .double(v.rounded())
        case "CEIL":
            guard args.count == 1, let v = evaluateAsDouble(args[0], binding: binding) else { return nil }
            return .double(ceil(v))
        case "FLOOR":
            guard args.count == 1, let v = evaluateAsDouble(args[0], binding: binding) else { return nil }
            return .double(floor(v))

        // Type checking
        case "ISIRI", "ISURI":
            guard args.count == 1, let val = evaluate(args[0], binding: binding) else { return nil }
            if case .string(let s) = val {
                return .bool(s.hasPrefix("http://") || s.hasPrefix("https://") || s.hasPrefix("urn:"))
            }
            return .bool(false)
        case "ISBLANK":
            guard args.count == 1, let val = evaluate(args[0], binding: binding) else { return nil }
            if case .string(let s) = val {
                return .bool(s.hasPrefix("_:"))
            }
            return .bool(false)
        case "ISLITERAL":
            guard args.count == 1 else { return nil }
            let val = evaluate(args[0], binding: binding)
            return .bool(val != nil)
        case "ISNUMERIC":
            guard args.count == 1, let val = evaluate(args[0], binding: binding) else { return nil }
            switch val {
            case .int64, .double: return .bool(true)
            default: return .bool(false)
            }

        // Hash functions
        case "MD5", "SHA1", "SHA256", "SHA384", "SHA512":
            guard args.count == 1,
                  let str = evaluateAsString(args[0], binding: binding) else { return nil }
            return .string(hashFunction(name, str))

        // Type conversion
        case "DATATYPE":
            guard args.count == 1, let val = evaluate(args[0], binding: binding) else { return nil }
            return .string(xsdDatatype(val))

        // IF
        case "IF":
            guard args.count == 3 else { return nil }
            if evaluateAsBoolean(args[0], binding: binding) {
                return evaluate(args[1], binding: binding)
            } else {
                return evaluate(args[2], binding: binding)
            }

        // BOUND (function form)
        case "BOUND":
            guard args.count == 1, case .variable(let v) = args[0] else { return nil }
            return .bool(binding.isBound(bindingKey(v)))

        // COALESCE (function form)
        case "COALESCE":
            for arg in args {
                if let val = evaluate(arg, binding: binding), val != .null {
                    return val
                }
            }
            return .null

        default:
            return nil
        }
    }

    // MARK: - Helpers

    private static func evaluateAsString(
        _ expr: QueryIR.Expression,
        binding: VariableBinding
    ) -> String? {
        guard let val = evaluate(expr, binding: binding) else { return nil }
        return stringRepresentation(val)
    }

    private static func evaluateAsDouble(
        _ expr: QueryIR.Expression,
        binding: VariableBinding
    ) -> Double? {
        guard let val = evaluate(expr, binding: binding) else { return nil }
        switch val {
        case .double(let v): return v
        case .int64(let v): return Double(v)
        case .string(let s): return Double(s)
        default: return nil
        }
    }

    private static func evaluateAsInt64(
        _ expr: QueryIR.Expression,
        binding: VariableBinding
    ) -> Int64? {
        guard let val = evaluate(expr, binding: binding) else { return nil }
        switch val {
        case .int64(let v): return v
        case .double(let v): return Int64(exactly: v)
        case .string(let s): return Int64(s)
        default: return nil
        }
    }

    private static func stringRepresentation(_ value: FieldValue) -> String {
        switch value {
        case .string(let s): return s
        case .int64(let v): return String(v)
        case .double(let v): return String(v)
        case .bool(let v): return v ? "true" : "false"
        case .null: return ""
        default: return String(describing: value)
        }
    }

    private static func boolResult(_ value: Bool?) -> FieldValue? {
        guard let v = value else { return nil }
        return .bool(v)
    }

    private static func effectiveBooleanValue(_ value: FieldValue) -> Bool {
        switch value {
        case .bool(let v): return v
        case .int64(let v): return v != 0
        case .double(let v): return v != 0 && !v.isNaN
        case .string(let s): return !s.isEmpty
        case .null: return false
        default: return false
        }
    }

    private static func compareValues(
        _ lhs: QueryIR.Expression,
        _ rhs: QueryIR.Expression,
        binding: VariableBinding
    ) -> ComparisonResult? {
        guard let l = evaluate(lhs, binding: binding),
              let r = evaluate(rhs, binding: binding) else { return nil }
        if l == .null || r == .null { return nil }
        return l.compare(to: r)
    }

    private static func numericBinary(
        _ lhs: QueryIR.Expression,
        _ rhs: QueryIR.Expression,
        binding: VariableBinding,
        op: (Double, Double) -> Double
    ) -> FieldValue? {
        guard let l = evaluate(lhs, binding: binding),
              let r = evaluate(rhs, binding: binding) else { return nil }
        // If both are integers, try integer arithmetic
        if case .int64(let lv) = l, case .int64(let rv) = r {
            let result = op(Double(lv), Double(rv))
            if let intResult = Int64(exactly: result) {
                return .int64(intResult)
            }
            return .double(result)
        }
        // Otherwise promote to double
        guard let ld = asDouble(l), let rd = asDouble(r) else { return nil }
        return .double(op(ld, rd))
    }

    private static func asDouble(_ value: FieldValue) -> Double? {
        switch value {
        case .double(let v): return v
        case .int64(let v): return Double(v)
        case .string(let s): return Double(s)
        default: return nil
        }
    }

    // MARK: - Regex

    private static func matchRegex(_ string: String, pattern: String, flags: String?) -> Bool {
        var options: NSRegularExpression.Options = []
        if let flags = flags {
            if flags.contains("i") { options.insert(.caseInsensitive) }
            if flags.contains("m") { options.insert(.anchorsMatchLines) }
            if flags.contains("s") { options.insert(.dotMatchesLineSeparators) }
            if flags.contains("x") { options.insert(.allowCommentsAndWhitespace) }
        }
        guard let regex = try? NSRegularExpression(pattern: pattern, options: options) else {
            return false
        }
        let range = NSRange(string.startIndex..., in: string)
        return regex.firstMatch(in: string, range: range) != nil
    }

    private static func replaceRegex(
        _ string: String,
        pattern: String,
        replacement: String,
        flags: String?
    ) -> String {
        var options: NSRegularExpression.Options = []
        if let flags = flags {
            if flags.contains("i") { options.insert(.caseInsensitive) }
            if flags.contains("m") { options.insert(.anchorsMatchLines) }
            if flags.contains("s") { options.insert(.dotMatchesLineSeparators) }
        }
        guard let regex = try? NSRegularExpression(pattern: pattern, options: options) else {
            return string
        }
        let range = NSRange(string.startIndex..., in: string)
        return regex.stringByReplacingMatches(in: string, range: range, withTemplate: replacement)
    }

    private static func likeToRegex(_ pattern: String) -> String {
        var result = "^"
        for char in pattern {
            switch char {
            case "%": result += ".*"
            case "_": result += "."
            case ".": result += "\\."
            case "\\": result += "\\\\"
            case "[": result += "\\["
            case "]": result += "\\]"
            case "(": result += "\\("
            case ")": result += "\\)"
            case "{": result += "\\{"
            case "}": result += "\\}"
            case "^": result += "\\^"
            case "$": result += "\\$"
            case "+": result += "\\+"
            case "?": result += "\\?"
            case "|": result += "\\|"
            case "*": result += "\\*"
            default: result += String(char)
            }
        }
        result += "$"
        return result
    }

    // MARK: - Type Helpers

    private static func xsdDatatype(_ value: FieldValue) -> String {
        switch value {
        case .bool: return "http://www.w3.org/2001/XMLSchema#boolean"
        case .int64: return "http://www.w3.org/2001/XMLSchema#integer"
        case .double: return "http://www.w3.org/2001/XMLSchema#double"
        case .string: return "http://www.w3.org/2001/XMLSchema#string"
        case .data: return "http://www.w3.org/2001/XMLSchema#base64Binary"
        default: return ""
        }
    }

    private static func hashFunction(_ name: String, _ input: String) -> String {
        // Placeholder — actual crypto hashing requires import Crypto
        // Returns empty string for unsupported hash functions
        return ""
    }
}

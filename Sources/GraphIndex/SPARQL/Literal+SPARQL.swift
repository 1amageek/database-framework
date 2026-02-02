// Literal+SPARQL.swift
// GraphIndex - SPARQL-aware Literal to FieldValue conversion
//
// Unlike LiteralBridge.toFieldValue() (DatabaseEngine) which returns nil for
// RDF-specific types (IRI, blankNode, typedLiteral, langLiteral), this extension
// converts ALL Literal cases to FieldValue for SPARQL execution contexts.
//
// Conversion rules align with GraphPatternConverter.convertTerm() for consistency:
//   .iri(v)           → .string(v)            (same as convertTerm(.iri))
//   .blankNode(v)     → .string("_:v")        (same as convertTerm(.blankNode))
//   .typedLiteral     → XSD datatype-aware conversion (SPARQL §17.1)
//   .langLiteral(v,_) → .string(v)            (string value preserved)
//   .date(d)          → .string(ISO8601)
//   .timestamp(d)     → .string(ISO8601)

import Foundation
import QueryIR
import Core
import DatabaseEngine

extension QueryIR.Literal {

    /// Convert a Literal to FieldValue in SPARQL execution context.
    ///
    /// Delegates to `toFieldValue()` for common types (null, bool, int, double, string, binary, array).
    /// Handles RDF-specific types that `toFieldValue()` returns nil for.
    /// This method never returns nil.
    public func toSPARQLFieldValue() -> FieldValue {
        if let fv = self.toFieldValue() { return fv }
        switch self {
        case .iri(let value):
            return .string(value)
        case .blankNode(let value):
            return .string("_:\(value)")
        case .typedLiteral(let value, let datatype):
            return Self.convertTypedLiteral(value: value, datatype: datatype)
        case .langLiteral(let value, _):
            return .string(value)
        case .date(let date):
            let formatter = ISO8601DateFormatter()
            formatter.formatOptions = [.withFullDate]
            return .string(formatter.string(from: date))
        case .timestamp(let date):
            return .string(ISO8601DateFormatter().string(from: date))
        default:
            // Unreachable: toFieldValue() covers null, bool, int, double, string, binary, array
            return .null
        }
    }

    private static func convertTypedLiteral(value: String, datatype: String) -> FieldValue {
        switch XSDDatatype(rawValue: datatype) {
        case .integer:
            if let v = Int64(value) { return .int64(v) }
            return .string(value)
        case .decimal, .double, .float:
            if let v = Double(value) { return .double(v) }
            return .string(value)
        case .boolean:
            return .bool(value == "true" || value == "1")
        case .base64Binary:
            if let data = Data(base64Encoded: value) { return .data(data) }
            return .string(value)
        case .string, .anyURI, .date, .dateTime, .time, .duration, .hexBinary, nil:
            return .string(value)
        }
    }
}

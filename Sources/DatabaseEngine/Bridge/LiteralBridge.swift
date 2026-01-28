// LiteralBridge.swift
// DatabaseEngine - Bridge between Core.FieldValue and QueryIR.Literal

import Foundation
import Core
import QueryIR

// MARK: - FieldValue → Literal

extension FieldValue {
    /// Convert a FieldValue to a QueryIR Literal.
    ///
    /// All FieldValue cases have direct QueryIR.Literal equivalents.
    /// This conversion always succeeds.
    public func toLiteral() -> QueryIR.Literal {
        switch self {
        case .null:
            return .null
        case .bool(let v):
            return .bool(v)
        case .int64(let v):
            return .int(v)
        case .double(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .data(let v):
            return .binary(v)
        case .array(let values):
            return .array(values.map { $0.toLiteral() })
        }
    }
}

// MARK: - Literal → FieldValue

extension QueryIR.Literal {
    /// Convert a QueryIR Literal to a FieldValue.
    ///
    /// Returns `nil` for RDF-specific literal types that have no FieldValue equivalent:
    /// `.iri`, `.blankNode`, `.typedLiteral`, `.langLiteral`, `.date`, `.timestamp`.
    ///
    /// - Note: `.date` and `.timestamp` are not supported because FieldValue has no Date case.
    ///   If needed in the future, consider encoding dates as `.string` with ISO 8601.
    public func toFieldValue() -> FieldValue? {
        switch self {
        case .null:
            return .null
        case .bool(let v):
            return .bool(v)
        case .int(let v):
            return .int64(v)
        case .double(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .binary(let v):
            return .data(v)
        case .array(let literals):
            var values: [FieldValue] = []
            values.reserveCapacity(literals.count)
            for lit in literals {
                guard let fv = lit.toFieldValue() else { return nil }
                values.append(fv)
            }
            return .array(values)
        case .date, .timestamp:
            return nil
        case .iri, .blankNode, .typedLiteral, .langLiteral:
            return nil
        }
    }
}

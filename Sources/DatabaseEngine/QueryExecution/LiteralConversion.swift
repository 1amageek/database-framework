// LiteralConversion.swift
// DatabaseEngine - Conversion between Core.FieldValue and QueryIR.Literal

import Core
import DatabaseValue
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
        case .uint64(let value):
            return .uint(value)
        case .double(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .data(let v):
            return .binary(v)
        case .rdfTerm(let term):
            return .rdfTerm(term)
        case .array(let values):
            return .array(values.map { $0.toLiteral() })
        }
    }
}

// MARK: - Literal → FieldValue

extension QueryIR.Literal {
    /// Convert a query literal into the canonical database value model.
    public func toDatabaseValue() throws(LiteralConversionError) -> DatabaseValue {
        switch self {
        case .null:
            return .null
        case .bool(let value):
            return .bool(value)
        case .int(let value):
            return .int64(value)
        case .uint(let value):
            return .uint64(value)
        case .decimal(let coefficient, let scale):
            return .decimal(coefficient: coefficient, scale: scale)
        case .double(let value):
            return .double(value)
        case .string(let value):
            return .string(value)
        case .date(let value):
            return .date(value)
        case .timestamp(let value):
            return .timestamp(value)
        case .binary(let value):
            return .bytes(value)
        case .uuid(let value):
            return .uuid(value)
        case .array(let values):
            var converted: [DatabaseValue] = []
            converted.reserveCapacity(values.count)
            for value in values {
                converted.append(try value.toDatabaseValue())
            }
            return .array(converted)
        case .iri(let value):
            return .rdfTerm(.iri(value))
        case .blankNode(let value):
            return .rdfTerm(.blankNode(value))
        case .typedLiteral(let value, let datatype):
            do {
                return .rdfTerm(
                    .literal(
                        try DatabaseRDFLiteral(
                            lexicalForm: value,
                            datatype: datatype
                        )
                    )
                )
            } catch {
                throw .invalidRDFLiteral(datatype: datatype)
            }
        case .langLiteral(let value, let language):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw .invalidLanguageTag(language)
            }
            return .rdfTerm(
                .literal(DatabaseRDFLiteral(lexicalForm: value, language: tag))
            )
        case .dirLangLiteral(let value, let language, let direction):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw .invalidLanguageTag(language)
            }
            guard let baseDirection = DatabaseRDFDirection(rawValue: direction) else {
                throw .invalidBaseDirection(direction)
            }
            return .rdfTerm(
                .literal(
                    DatabaseRDFLiteral(
                        lexicalForm: value,
                        language: tag,
                        direction: baseDirection
                    )
                )
            )
        case .rdfTerm(let value):
            return .rdfTerm(value)
        }
    }

    /// Project a query literal into the narrower `FieldValue` model.
    ///
    /// Canonical values that `FieldValue` cannot preserve exactly fail with a
    /// typed error. Call `toDatabaseValue()` when decimal, date, timestamp, or
    /// UUID semantics must be retained.
    public func toFieldValue() throws(LiteralConversionError) -> FieldValue {
        switch self {
        case .null:
            return .null
        case .bool(let v):
            return .bool(v)
        case .int(let v):
            return .int64(v)
        case .uint(let value):
            return .uint64(value)
        case .decimal:
            throw .fieldValueUnsupported(kind: .decimal)
        case .double(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .binary(let v):
            return .data(v)
        case .uuid:
            throw .fieldValueUnsupported(kind: .uuid)
        case .array(let literals):
            var values: [FieldValue] = []
            values.reserveCapacity(literals.count)
            for literal in literals {
                values.append(try literal.toFieldValue())
            }
            return .array(values)
        case .date:
            throw .fieldValueUnsupported(kind: .date)
        case .timestamp:
            throw .fieldValueUnsupported(kind: .timestamp)
        case .iri, .blankNode, .typedLiteral, .langLiteral,
             .dirLangLiteral, .rdfTerm:
            let canonical = try toDatabaseValue()
            guard case .rdfTerm(let term) = canonical else {
                throw .fieldValueConversionInvariantViolation
            }
            return .rdfTerm(term)
        }
    }
}

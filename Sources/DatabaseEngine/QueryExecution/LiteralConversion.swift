// LiteralConversion.swift
// DatabaseEngine - Conversion between FieldValue and Literal

import DatabaseKit
import DatabaseTypes

// MARK: - FieldValue → Literal

extension FieldValue {
    /// Convert a canonical field value to a query literal.
    ///
    /// Object and reference values have no QueryIR literal representation and
    /// fail explicitly instead of being projected into a lossy substitute.
    public func toLiteral() throws(LiteralConversionError) -> Literal {
        switch self {
        case .null:
            return .null
        case .bool(let v):
            return .bool(v)
        case .int8(let v):
            return .int(Int64(v))
        case .int16(let v):
            return .int(Int64(v))
        case .int32(let v):
            return .int(Int64(v))
        case .int64(let v):
            return .int(v)
        case .uint8(let value):
            return .uint(UInt64(value))
        case .uint16(let value):
            return .uint(UInt64(value))
        case .uint32(let value):
            return .uint(UInt64(value))
        case .uint64(let value):
            return .uint(value)
        case .decimal(let value):
            return .decimal(value)
        case .float32(let v):
            return .double(Double(v))
        case .float64(let v):
            return .double(v)
        case .string(let v):
            return .string(v)
        case .bytes(let v):
            return .binary(v)
        case .date(let value):
            return .date(value)
        case .timestamp(let value):
            return .timestamp(value)
        case .uuid(let value):
            return .uuid(value)
        case .rdfTerm(let term):
            return .rdfTerm(term)
        case .array(let values):
            var literals: [Literal] = []
            literals.reserveCapacity(values.count)
            for value in values {
                literals.append(try value.toLiteral())
            }
            return .array(literals)
        case .object:
            throw .fieldValueUnsupported(kind: .object)
        case .reference:
            throw .fieldValueUnsupported(kind: .reference)
        case .time:
            throw .fieldValueUnsupported(kind: .time)
        case .dateTime:
            throw .fieldValueUnsupported(kind: .dateTime)
        case .timeSpan:
            throw .fieldValueUnsupported(kind: .timeSpan)
        case .calendarPeriod:
            throw .fieldValueUnsupported(kind: .calendarPeriod)
        case .geographicPoint:
            throw .fieldValueUnsupported(kind: .geographicPoint)
        case .geographicPosition:
            throw .fieldValueUnsupported(kind: .geographicPosition)
        case .vector:
            throw .fieldValueUnsupported(kind: .vector)
        }
    }
}

// MARK: - Literal → FieldValue

extension Literal {
    /// Convert a query literal into the canonical database value model.
    public func toFieldValue() throws(LiteralConversionError) -> FieldValue {
        switch self {
        case .null:
            return .null
        case .bool(let value):
            return .bool(value)
        case .int(let value):
            return .int64(value)
        case .uint(let value):
            return .uint64(value)
        case .decimal(let value):
            return .decimal(value)
        case .double(let value):
            return .float64(value)
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
            var converted: [FieldValue] = []
            converted.reserveCapacity(values.count)
            for value in values {
                converted.append(try value.toFieldValue())
            }
            return .array(converted)
        case .iri(let value):
            do {
                return .rdfTerm(.iri(try RDFIRI(value)))
            } catch {
                throw .invalidRDFLiteral(datatype: value)
            }
        case .blankNode(let value):
            do {
                return .rdfTerm(
                    .blankNode(try RDFBlankNodeIdentifier(value))
                )
            } catch {
                throw .invalidRDFLiteral(datatype: value)
            }
        case .typedLiteral(let value, let datatype):
            do {
                return .rdfTerm(
                    .literal(
                        try RDFLiteral(
                            lexicalForm: value,
                            datatype: datatype
                        )
                    )
                )
            } catch {
                throw .invalidRDFLiteral(datatype: datatype)
            }
        case .langLiteral(let value, let language):
            let tag: RDFLanguageTag
            do {
                tag = try RDFLanguageTag(language)
            } catch {
                throw .invalidLanguageTag(language)
            }
            return .rdfTerm(
                .literal(RDFLiteral(lexicalForm: value, language: tag))
            )
        case .dirLangLiteral(let value, let language, let direction):
            let tag: RDFLanguageTag
            do {
                tag = try RDFLanguageTag(language)
            } catch {
                throw .invalidLanguageTag(language)
            }
            guard let baseDirection = RDFDirection(rawValue: direction) else {
                throw .invalidBaseDirection(direction)
            }
            return .rdfTerm(
                .literal(
                    RDFLiteral(
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

}

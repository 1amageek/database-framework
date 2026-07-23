#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseValue
import QueryIR

extension QueryIR.Literal {
    /// Converts a SPARQL term literal without erasing its RDF identity or datatype.
    public func toSPARQLFieldValue() throws -> FieldValue {
        switch self {
        case .null:
            throw SPARQLLiteralConversionError.nullTermUnsupported
        case .array:
            throw SPARQLLiteralConversionError.arrayTermUnsupported
        case .bool(let value):
            return try Self.rdfLiteral(
                value ? "true" : "false",
                datatype: "http://www.w3.org/2001/XMLSchema#boolean"
            )
        case .int(let value):
            return try Self.rdfLiteral(
                String(value),
                datatype: "http://www.w3.org/2001/XMLSchema#integer"
            )
        case .uint(let value):
            return try Self.rdfLiteral(
                String(value),
                datatype: "http://www.w3.org/2001/XMLSchema#unsignedLong"
            )
        case .decimal(let coefficient, let scale):
            let lexicalForm: String
            do {
                lexicalForm = try DatabaseExactDecimal(
                    coefficient: coefficient,
                    scale: scale
                ).decimalLexicalForm(
                    maximumUTF8Count: SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            } catch let error {
                switch error {
                case .invalidMaximumUTF8Count:
                    throw SPARQLLiteralConversionError.invalidLexicalForm(
                        value: String(coefficient),
                        datatype: "http://www.w3.org/2001/XMLSchema#decimal"
                    )
                case .representationTooLarge(let required, let maximum):
                    throw SPARQLLiteralConversionError.literalTooLarge(
                        requiredUTF8Count: required,
                        maximumUTF8Count: maximum
                    )
                }
            }
            return try Self.rdfLiteral(
                lexicalForm,
                datatype: "http://www.w3.org/2001/XMLSchema#decimal"
            )
        case .double(let value):
            return try Self.rdfLiteral(
                Self.xsdDoubleLexicalForm(value),
                datatype: "http://www.w3.org/2001/XMLSchema#double"
            )
        case .string(let value):
            return try Self.rdfLiteral(
                value,
                datatype: "http://www.w3.org/2001/XMLSchema#string"
            )
        case .date(let value):
            return try Self.rdfLiteral(
                DatabaseLiteralEncoding.iso8601(value),
                datatype: "http://www.w3.org/2001/XMLSchema#date"
            )
        case .timestamp(let value):
            return try Self.rdfLiteral(
                DatabaseLiteralEncoding.iso8601(value),
                datatype: "http://www.w3.org/2001/XMLSchema#dateTime"
            )
        case .binary(let bytes):
            return try Self.rdfLiteral(
                DatabaseLiteralEncoding.base64(bytes),
                datatype: "http://www.w3.org/2001/XMLSchema#base64Binary"
            )
        case .uuid(let value):
            return try Self.rdfLiteral(
                value.description,
                datatype: "urn:uuid"
            )
        case .iri(let value):
            return .rdfTerm(.iri(value))
        case .blankNode(let identifier):
            return .rdfTerm(.blankNode(identifier))
        case .typedLiteral(let value, let datatype):
            return try Self.rdfLiteral(value, datatype: datatype)
        case .langLiteral(let value, let language):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw SPARQLLiteralConversionError.invalidLexicalForm(
                    value: value,
                    datatype: DatabaseRDFIRI.rdfLanguageString.rawValue
                )
            }
            return .rdfTerm(
                .literal(
                    DatabaseRDFLiteral(
                        lexicalForm: value,
                        language: tag
                    )
                )
            )
        case .dirLangLiteral(let value, let language, let direction):
            let tag: DatabaseRDFLanguageTag
            do {
                tag = try DatabaseRDFLanguageTag(language)
            } catch {
                throw SPARQLLiteralConversionError.invalidLexicalForm(
                    value: value,
                    datatype: DatabaseRDFIRI.rdfDirectionalLanguageString
                        .rawValue
                )
            }
            guard let baseDirection = DatabaseRDFDirection(
                rawValue: direction
            ) else {
                throw SPARQLLiteralConversionError.invalidLexicalForm(
                    value: value,
                    datatype: DatabaseRDFIRI.rdfDirectionalLanguageString
                        .rawValue
                )
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
        case .rdfTerm(let term):
            return .rdfTerm(term)
        }
    }

    private static func rdfLiteral(
        _ lexicalForm: String,
        datatype: String
    ) throws -> FieldValue {
        do {
            let literal = try DatabaseRDFLiteral(
                lexicalForm: lexicalForm,
                datatype: datatype
            )
            guard isValidKnownLexicalForm(literal) else {
                throw SPARQLLiteralConversionError.invalidLexicalForm(
                    value: lexicalForm,
                    datatype: datatype
                )
            }
            return .rdfTerm(.literal(literal))
        } catch let error as SPARQLLiteralConversionError {
            throw error
        } catch {
            throw SPARQLLiteralConversionError.invalidLexicalForm(
                value: lexicalForm,
                datatype: datatype
            )
        }
    }

    private static func isValidKnownLexicalForm(
        _ literal: DatabaseRDFLiteral
    ) -> Bool {
        let namespace = "http://www.w3.org/2001/XMLSchema#"
        switch literal.datatype {
        case namespace + "boolean":
            return literal.lexicalForm == "true"
                || literal.lexicalForm == "false"
                || literal.lexicalForm == "1"
                || literal.lexicalForm == "0"
        case namespace + "integer",
             namespace + "nonPositiveInteger",
             namespace + "negativeInteger",
             namespace + "long",
             namespace + "int",
             namespace + "short",
             namespace + "byte",
             namespace + "nonNegativeInteger",
             namespace + "positiveInteger",
             namespace + "unsignedLong",
             namespace + "unsignedInt",
             namespace + "unsignedShort",
             namespace + "unsignedByte",
             namespace + "decimal",
             namespace + "float",
             namespace + "double":
            return SPARQLNumericValue(.rdfTerm(.literal(literal))) != nil
        case namespace + "date":
            return DatabaseXSDDateTimeCodec.parseDate(literal.lexicalForm) != nil
        case namespace + "dateTime":
            return DatabaseXSDDateTimeCodec.parseTimestamp(literal.lexicalForm) != nil
        default:
            return true
        }
    }

    private static func xsdDoubleLexicalForm(_ value: Double) -> String {
        if value.isNaN { return "NaN" }
        if value == .infinity { return "INF" }
        if value == -.infinity { return "-INF" }
        return String(value)
    }
}

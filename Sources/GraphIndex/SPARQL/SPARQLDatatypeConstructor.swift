import Core
import DatabaseValue
import Graph
import OntologyIndex

/// Evaluates the constructor functions imported by SPARQL from XPath.
enum SPARQLDatatypeConstructor {
    static func evaluate(
        identifier: DatabaseRDFIRI,
        argument: FieldValue
    ) throws -> FieldValue {
        let localName = identifier.rawValue.dropFirst(
            SPARQLFunctionIdentifier.xsdNamespace.count
        )
        switch localName {
        case "string":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try stringLexicalForm(argument)
            )
        case "boolean":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try booleanLexicalForm(argument)
            )
        case "integer":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try integerLexicalForm(argument)
            )
        case "decimal":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try decimalLexicalForm(argument)
            )
        case "float":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try floatingLexicalForm(argument, asFloat: true)
            )
        case "double":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try floatingLexicalForm(argument, asFloat: false)
            )
        case "dateTime":
            return try construct(
                datatype: identifier.rawValue,
                lexicalForm: try dateTimeLexicalForm(argument)
            )
        default:
            throw SPARQLExpressionEvaluationError.unsupportedExpression(
                "datatype constructor \(identifier.rawValue)"
            )
        }
    }

    private static func stringLexicalForm(
        _ argument: FieldValue
    ) throws -> String {
        switch argument {
        case .rdfTerm(.iri(let iri)):
            return iri
        case .rdfTerm(.literal(let literal)):
            guard literal.language == nil, literal.direction == nil,
                  isSPARQLOperandDatatype(literal.datatype) else {
                throw typeError("xsd:string cannot cast this RDF literal")
            }
            return literal.lexicalForm
        case .string(let value):
            return value
        case .bool(let value):
            return value ? "true" : "false"
        case .int64(let value):
            return String(value)
        case .uint64(let value):
            return String(value)
        case .double(let value):
            return floatingLexicalForm(value)
        case .data, .rdfTerm, .null, .array:
            throw typeError("xsd:string requires an IRI or SPARQL operand literal")
        }
    }

    private static func booleanLexicalForm(
        _ argument: FieldValue
    ) throws -> String {
        if let numeric = SPARQLNumericValue(argument) {
            return numeric.isZero || numeric.isNaN ? "false" : "true"
        }
        switch argument {
        case .bool(let value):
            return value ? "true" : "false"
        case .rdfTerm(.literal(let literal))
            where literal.datatype == xsd("boolean"):
            switch literal.lexicalForm {
            case "true", "1": return "true"
            case "false", "0": return "false"
            default: throw typeError("invalid xsd:boolean lexical form")
            }
        default:
            return try stringInput(argument, target: "xsd:boolean")
        }
    }

    private static func integerLexicalForm(
        _ argument: FieldValue
    ) throws -> String {
        if let numeric = SPARQLNumericValue(argument) {
            guard let value = numeric.integerConstructorValue() else {
                throw typeError("numeric value cannot be cast to xsd:integer")
            }
            return String(value)
        }
        if let value = booleanValue(argument) {
            return value ? "1" : "0"
        }
        return try stringInput(argument, target: "xsd:integer")
    }

    private static func decimalLexicalForm(
        _ argument: FieldValue
    ) throws -> String {
        if let numeric = SPARQLNumericValue(argument) {
            do {
                guard let lexicalForm = try numeric
                    .decimalConstructorLexicalForm() else {
                    throw typeError("numeric value cannot be cast to xsd:decimal")
                }
                return lexicalForm
            } catch let error as SPARQLExpressionEvaluationError {
                throw error
            } catch let error as SPARQLNumericError {
                switch error {
                case .resultLiteralTooLarge(let required, let maximum):
                    throw SPARQLExpressionEvaluationError.resourceLimitExceeded(
                        stage: "xsd:decimal",
                        required: required,
                        maximum: maximum
                    )
                default:
                    throw typeError("numeric value cannot be represented as xsd:decimal")
                }
            }
        }
        if let value = booleanValue(argument) {
            return value ? "1" : "0"
        }
        return try stringInput(argument, target: "xsd:decimal")
    }

    private static func floatingLexicalForm(
        _ argument: FieldValue,
        asFloat: Bool
    ) throws -> String {
        if let numeric = SPARQLNumericValue(argument) {
            return numeric.floatingConstructorLexicalForm(asFloat: asFloat)
        }
        if let value = booleanValue(argument) {
            return value ? "1" : "0"
        }
        return try stringInput(
            argument,
            target: asFloat ? "xsd:float" : "xsd:double"
        )
    }

    private static func dateTimeLexicalForm(
        _ argument: FieldValue
    ) throws -> String {
        switch argument {
        case .rdfTerm(.literal(let literal))
            where literal.datatype == xsd("dateTime"):
            return literal.lexicalForm
        default:
            return try stringInput(argument, target: "xsd:dateTime")
        }
    }

    private static func stringInput(
        _ argument: FieldValue,
        target: String
    ) throws -> String {
        switch argument {
        case .string(let value):
            return value
        case .rdfTerm(.literal(let literal))
            where literal.language == nil
                && literal.direction == nil
                && literal.datatype == xsd("string"):
            return literal.lexicalForm
        default:
            throw typeError("\(target) requires a compatible numeric, boolean, or string literal")
        }
    }

    private static func booleanValue(_ argument: FieldValue) -> Bool? {
        switch argument {
        case .bool(let value):
            return value
        case .rdfTerm(.literal(let literal))
            where literal.datatype == xsd("boolean"):
            switch literal.lexicalForm {
            case "true", "1": return true
            case "false", "0": return false
            default: return nil
            }
        default:
            return nil
        }
    }

    private static func construct(
        datatype: String,
        lexicalForm: String
    ) throws -> FieldValue {
        let literal: DatabaseRDFLiteral
        do {
            literal = try DatabaseRDFLiteral(
                lexicalForm: lexicalForm,
                datatype: datatype
            )
        } catch {
            throw typeError("invalid datatype constructor result")
        }
        do {
            _ = try XSDValueParser(
                profile: .extendedXSD11,
                limits: .default
            ).parse(literal)
        } catch let failure as XSDValidationFailure {
            switch failure {
            case .resourceLimitExceeded(let resource, let limit, let actual):
                throw SPARQLExpressionEvaluationError.resourceLimitExceeded(
                    stage: "datatype constructor \(resource)",
                    required: UInt64(max(0, actual)),
                    maximum: UInt64(max(0, limit))
                )
            case .invalidLexicalForm, .unsupportedDatatype,
                 .invalidRestriction:
                throw typeError("invalid lexical form for \(datatype)")
            }
        }
        return .rdfTerm(.literal(literal))
    }

    private static func isSPARQLOperandDatatype(_ datatype: String) -> Bool {
        if datatype == xsd("string") || datatype == xsd("boolean")
            || datatype == xsd("dateTime") {
            return true
        }
        guard let kind = XSDDatatypeKind(iri: datatype) else { return false }
        return kind.isNumeric
    }

    private static func floatingLexicalForm(_ value: Double) -> String {
        if value.isNaN { return "NaN" }
        if value == .infinity { return "INF" }
        if value == -.infinity { return "-INF" }
        return String(value)
    }

    private static func xsd(_ localName: String) -> String {
        SPARQLFunctionIdentifier.xsdNamespace + localName
    }

    private static func typeError(
        _ detail: String
    ) -> SPARQLExpressionEvaluationError {
        .typeError(detail)
    }
}

import DatabaseKit
import DatabaseTypes

struct SPARQLStringValue: Sendable, Equatable {
    enum Kind: Sendable, Equatable {
        case string
        case language(RDFLanguageTag)
        case directionalLanguage(RDFLanguageTag, RDFDirection)
    }

    let lexicalForm: String
    let kind: Kind

    init(lexicalForm: String, kind: Kind) {
        self.lexicalForm = lexicalForm
        self.kind = kind
    }

    init?(_ value: FieldValue) {
        switch value {
        case .string(let string):
            lexicalForm = string
            kind = .string
        case .rdfTerm(.literal(let literal)):
            lexicalForm = literal.lexicalForm
            switch literal.annotation {
            case .typed(let datatype):
                guard datatype.iri == .xsdString else { return nil }
                kind = .string
            case .languageTagged(let language):
                kind = .language(language)
            case .directionalLanguageTagged(let language, let direction):
                kind = .directionalLanguage(language, direction)
            }
        default:
            return nil
        }
    }

    func acceptsArgument(_ other: Self) -> Bool {
        switch (kind, other.kind) {
        case (.string, .string):
            return true
        case (.language(let left), .language(let right)):
            return left == right
        case (
            .directionalLanguage(let leftLanguage, let leftDirection),
            .directionalLanguage(let rightLanguage, let rightDirection)
        ):
            return leftLanguage == rightLanguage && leftDirection == rightDirection
        case (.language, .string), (.directionalLanguage, .string):
            return true
        default:
            return false
        }
    }

    func replacingLexicalForm(_ lexicalForm: String) -> Self {
        Self(lexicalForm: lexicalForm, kind: kind)
    }

    func fieldValue() throws -> FieldValue {
        switch kind {
        case .string:
            return .rdfTerm(
                .literal(
                    RDFLiteral(
                        lexicalForm: lexicalForm,
                        datatype: .xsdString
                    )
                )
            )
        case .language(let language):
            return .rdfTerm(
                .literal(
                    RDFLiteral(
                        lexicalForm: lexicalForm,
                        language: language
                    )
                )
            )
        case .directionalLanguage(let language, let direction):
            return .rdfTerm(
                .literal(
                    RDFLiteral(
                        lexicalForm: lexicalForm,
                        language: language,
                        direction: direction
                    )
                )
            )
        }
    }

    static func concatenationKind(_ values: [Self]) -> Kind {
        guard let first = values.first else { return .string }
        guard values.dropFirst().allSatisfy({ $0.kind == first.kind }) else {
            return .string
        }
        return first.kind
    }
}

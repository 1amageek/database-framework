import DatabaseKit
import DatabaseTypes

/// Immutable full-text input for a canonical Fusion plan.
public struct Search<Item: Persistable>: FusionQueryInput, Sendable {
    private let field: FieldIdentity
    private var indexName: String?
    private var searchTerms: [String] = []
    private var matchMode: TextMatchMode = .all
    private var k1: Float = 1.2
    private var b: Float = 0.75
    private var resultLimit: UInt64?

    public init(_ field: Field<Item, String>) {
        self.field = field.identity
    }

    public init(_ field: Field<Item, String?>) {
        self.field = field.identity
    }

    public func index(named name: String) -> Self {
        var copy = self
        copy.indexName = name
        return copy
    }

    public func terms(_ terms: [String]) -> Self {
        var copy = self
        copy.searchTerms = terms
        return copy
    }

    public func terms(_ terms: [String], mode: TextMatchMode) -> Self {
        var copy = self
        copy.searchTerms = terms
        copy.matchMode = mode
        return copy
    }

    public func mode(_ mode: TextMatchMode) -> Self {
        var copy = self
        copy.matchMode = mode
        return copy
    }

    public func bm25(k1: Float = 1.2, b: Float = 0.75) -> Self {
        var copy = self
        copy.k1 = k1
        copy.b = b
        return copy
    }

    public func limit(_ count: UInt64) -> Self {
        var copy = self
        copy.resultLimit = count
        return copy
    }

    public var fusionInput: FusionInput {
        let parameters: [String: FieldValue] = [
            FullTextReadParameter.fieldName: .string(field.name),
            FullTextReadParameter.terms: .array(searchTerms.map(FieldValue.string)),
            FullTextReadParameter.matchMode: .string(matchModeIdentifier),
            FullTextReadParameter.bm25K1: .float64(Double(k1)),
            FullTextReadParameter.bm25B: .float64(Double(b)),
        ]
        let selection: FusionIndexSelection = if let indexName {
            .named(name: indexName, type: .text(.fullText))
        } else {
            .matching(
                type: .text(.fullText),
                fields: [field],
                fieldMatch: .exact
            )
        }
        return FusionInput(
            operation: .index(
                FusionIndexSource(
                    selection: selection,
                    referencedFields: [field],
                    parameters: parameters
                )
            ),
            scoring: .annotation(
                name: "score",
                order: .higherIsBetter
            ),
            limit: resultLimit
        )
    }

    private var matchModeIdentifier: String {
        switch matchMode {
        case .all: "all"
        case .any: "any"
        case .phrase: "phrase"
        }
    }
}

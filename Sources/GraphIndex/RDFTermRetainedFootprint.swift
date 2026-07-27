import DatabaseEngine
import DatabaseTypes

/// Deterministic retained-memory accounting for one RDF term.
///
/// RDF-star recursion exists only through a triple term's object. Subject and
/// predicate are leaves, so traversal uses one moving cursor and requires no
/// depth-proportional worklist allocation.
enum RDFTermRetainedFootprint {
    private static let termNodeByteCount: UInt64 = 32
    private static let literalByteCount: UInt64 = 24
    private static let stringStorageByteCount: UInt64 = 16

    static func measure(
        _ term: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        var footprint = DatabaseIntermediateFootprint()
        var current = copy term

        while true {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(bytes: termNodeByteCount)
            )
            switch current {
            case .iri(let iri):
                return try footprint.adding(stringFootprint(iri.rawValue))

            case .blankNode(let identifier):
                return try footprint.adding(
                    stringFootprint(identifier.rawValue)
                )

            case .literal(let literal):
                footprint = try footprint.adding(
                    DatabaseIntermediateFootprint(bytes: literalByteCount)
                ).adding(
                    stringFootprint(literal.lexicalForm)
                )
                switch literal.annotation {
                case .typed(let datatype):
                    return try footprint.adding(
                        stringFootprint(datatype.rawValue)
                    )
                case .languageTagged(let language),
                     .directionalLanguageTagged(let language, _):
                    return try footprint.adding(
                        stringFootprint(language.rawValue)
                    )
                }

            case .tripleTerm(let subject, let predicate, let object):
                footprint = try footprint.adding(
                    leafFootprint(subject)
                ).adding(
                    iriFootprint(predicate.iri)
                )
                current = object
            }
        }
    }

    private static func leafFootprint(
        _ subject: RDFSubject
    ) throws -> DatabaseIntermediateFootprint {
        switch subject {
        case .iri(let iri):
            return try iriFootprint(iri)
        case .blankNode(let identifier):
            return try DatabaseIntermediateFootprint(
                bytes: termNodeByteCount
            ).adding(
                stringFootprint(identifier.rawValue)
            )
        }
    }

    private static func iriFootprint(
        _ iri: RDFIRI
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: termNodeByteCount
        ).adding(
            stringFootprint(iri.rawValue)
        )
    }

    private static func stringFootprint(
        _ value: String
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: stringStorageByteCount
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(value.utf8.count))
        )
    }
}

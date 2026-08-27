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
        _ term: borrowing RDFTerm,
        replacingBlankNodeByteCount replacement: UInt64? = nil
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
                    replacement.map(stringFootprint(byteCount:))
                        ?? stringFootprint(identifier.rawValue)
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
                    leafFootprint(
                        subject,
                        replacingBlankNodeByteCount: replacement
                    )
                ).adding(
                    iriFootprint(predicate.iri)
                )
                current = object
            }
        }
    }

    /// Computes the same retained semantic footprint from validated canonical
    /// bytes, before any RDF value or String has been materialized.
    static func measure(
        _ validation: RDFTermStorageValidation,
        replacingBlankNodeByteCount replacement: UInt64? = nil
    ) throws -> DatabaseIntermediateFootprint {
        let termNodes = try checkedMultiply(
            UInt64(validation.objectCount),
            termNodeByteCount
        )
        let literals = try checkedMultiply(
            UInt64(validation.literalCount),
            literalByteCount
        )
        let strings = try checkedMultiply(
            UInt64(validation.stringCount),
            stringStorageByteCount
        )
        var stringBytes = UInt64(validation.decodedStringByteCount)
        if let replacement {
            let originalBlankNodeBytes = UInt64(
                validation.blankNodeStringByteCount
            )
            let replacementBytes = try checkedMultiply(
                UInt64(validation.blankNodeCount),
                replacement
            )
            guard stringBytes >= originalBlankNodeBytes else {
                throw RDFDatasetScannerError.retainedByteCountOverflow(
                    operation: .addition,
                    left: stringBytes,
                    right: originalBlankNodeBytes
                )
            }
            stringBytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
                stringBytes - originalBlankNodeBytes,
                replacementBytes
            )
        }
        var bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(
            termNodes,
            literals
        )
        bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(bytes, strings)
        bytes = try RDFDatasetScanRetainedMetrics.checkedAdd(bytes, stringBytes)
        return DatabaseIntermediateFootprint(bytes: bytes)
    }

    private static func checkedMultiply(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        try RDFDatasetScanRetainedMetrics.checkedMultiply(left, right)
    }

    private static func leafFootprint(
        _ subject: RDFSubject,
        replacingBlankNodeByteCount replacement: UInt64? = nil
    ) throws -> DatabaseIntermediateFootprint {
        switch subject {
        case .iri(let iri):
            return try iriFootprint(iri)
        case .blankNode(let identifier):
            return try DatabaseIntermediateFootprint(
                bytes: termNodeByteCount
            ).adding(
                replacement.map(stringFootprint(byteCount:))
                    ?? stringFootprint(identifier.rawValue)
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
        try stringFootprint(byteCount: UInt64(value.utf8.count))
    }

    private static func stringFootprint(
        byteCount: UInt64
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            bytes: stringStorageByteCount
        ).adding(
            DatabaseIntermediateFootprint(bytes: byteCount)
        )
    }
}

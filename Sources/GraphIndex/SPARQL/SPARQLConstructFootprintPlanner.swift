import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Computes a safe retained-footprint maximum from a CONSTRUCT template and
/// one borrowed solution without materializing any output RDF value.
enum SPARQLConstructFootprintPlanner {
    private static let quadPayloadByteCount: UInt64 = 64
    private static let termNodeByteCount: UInt64 = 32
    private static let literalPayloadByteCount: UInt64 = 24
    private static let stringStorageByteCount: UInt64 = 16
    private static let generatedBlankNodeUTF8Count: UInt64 = 65

    static func maximumQuadFootprint(
        _ pattern: TriplePattern,
        binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint? {
        try maximumQuadFootprint(
            subject: pattern.subject,
            predicate: pattern.predicate,
            object: pattern.object,
            binding: binding
        )
    }

    static func maximumReificationQuadFootprint(
        subject: SPARQLTerm,
        predicate: SPARQLTerm,
        object: SPARQLTerm,
        reifier: SPARQLTerm,
        binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint? {
        guard let reifierFootprint = try maximumTermFootprint(
            reifier,
            binding: binding
        ), let objectSubjectFootprint = try maximumTermFootprint(
            subject,
            binding: binding
        ), let objectPredicateFootprint = try maximumTermFootprint(
            predicate,
            binding: binding
        ), let objectObjectFootprint = try maximumTermFootprint(
            object,
            binding: binding
        ) else {
            return nil
        }
        let reifiesPredicate = iriFootprint(
            utf8Count: UInt64(
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#reifies"
                    .utf8.count
            )
        )
        let tripleObject = try DatabaseIntermediateFootprint(
            bytes: termNodeByteCount
        ).adding(objectSubjectFootprint)
            .adding(objectPredicateFootprint)
            .adding(objectObjectFootprint)
        return try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: quadPayloadByteCount
        ).adding(reifierFootprint)
            .adding(reifiesPredicate)
            .adding(tripleObject)
    }

    static func maximumTermFootprint(
        _ term: SPARQLTerm,
        binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint? {
        switch term {
        case .variable(let name):
            guard let value = binding["?\(name)"],
                  case .rdfTerm(let rdfTerm) = value else {
                return nil
            }
            return try RDFTermRetainedFootprint.measure(rdfTerm)

        case .iri(let value):
            return iriFootprint(utf8Count: UInt64(value.utf8.count))

        case .literal(let literal):
            return try maximumLiteralFootprint(literal)

        case .blankNode:
            return blankNodeFootprint(
                utf8Count: generatedBlankNodeUTF8Count
            )

        case .tripleTerm(let subject, let predicate, let object):
            guard let subject = try maximumTermFootprint(
                subject,
                binding: binding
            ), let predicate = try maximumTermFootprint(
                predicate,
                binding: binding
            ), let object = try maximumTermFootprint(
                object,
                binding: binding
            ) else {
                return nil
            }
            return try DatabaseIntermediateFootprint(
                bytes: termNodeByteCount
            ).adding(subject)
                .adding(predicate)
                .adding(object)

        case .reifiedTriple(_, _, _, let reifier):
            return try maximumTermFootprint(
                reifier,
                binding: binding
            )
        }
    }

    private static func maximumQuadFootprint(
        subject: SPARQLTerm,
        predicate: SPARQLTerm,
        object: SPARQLTerm,
        binding: borrowing VariableBinding
    ) throws -> DatabaseIntermediateFootprint? {
        guard let subject = try maximumTermFootprint(
            subject,
            binding: binding
        ), let predicate = try maximumTermFootprint(
            predicate,
            binding: binding
        ), let object = try maximumTermFootprint(
            object,
            binding: binding
        ) else {
            return nil
        }
        return try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: quadPayloadByteCount
        ).adding(subject)
            .adding(predicate)
            .adding(object)
    }

    private static func maximumLiteralFootprint(
        _ literal: Literal
    ) throws -> DatabaseIntermediateFootprint {
        switch literal {
        case .null:
            throw SPARQLLiteralConversionError.nullTermUnsupported
        case .array:
            throw SPARQLLiteralConversionError.arrayTermUnsupported
        case .bool:
            return literalFootprint(
                maximumLexicalUTF8Count: 5,
                annotation: "http://www.w3.org/2001/XMLSchema#boolean"
            )
        case .int:
            return literalFootprint(
                maximumLexicalUTF8Count: 20,
                annotation: "http://www.w3.org/2001/XMLSchema#integer"
            )
        case .uint:
            return literalFootprint(
                maximumLexicalUTF8Count: 20,
                annotation: "http://www.w3.org/2001/XMLSchema#unsignedLong"
            )
        case .decimal(let decimal):
            let count = try decimalLexicalUTF8Count(decimal)
            return literalFootprint(
                maximumLexicalUTF8Count: count,
                annotation: "http://www.w3.org/2001/XMLSchema#decimal"
            )
        case .double:
            return literalFootprint(
                maximumLexicalUTF8Count: 32,
                annotation: "http://www.w3.org/2001/XMLSchema#double"
            )
        case .string(let value):
            return literalFootprint(
                maximumLexicalUTF8Count: UInt64(value.utf8.count),
                annotation: "http://www.w3.org/2001/XMLSchema#string"
            )
        case .date:
            return literalFootprint(
                maximumLexicalUTF8Count: 17,
                annotation: "http://www.w3.org/2001/XMLSchema#date"
            )
        case .timestamp:
            return literalFootprint(
                maximumLexicalUTF8Count: 48,
                annotation: "http://www.w3.org/2001/XMLSchema#dateTime"
            )
        case .binary(let bytes):
            return literalFootprint(
                maximumLexicalUTF8Count: try base64UTF8Count(bytes.count),
                annotation: "http://www.w3.org/2001/XMLSchema#base64Binary"
            )
        case .uuid:
            return literalFootprint(
                maximumLexicalUTF8Count: 36,
                annotation: "urn:uuid"
            )
        case .iri(let value):
            return iriFootprint(utf8Count: UInt64(value.utf8.count))
        case .blankNode(let identifier):
            return blankNodeFootprint(
                utf8Count: UInt64(identifier.utf8.count)
            )
        case .typedLiteral(let value, let datatype):
            return literalFootprint(
                maximumLexicalUTF8Count: UInt64(value.utf8.count),
                annotation: datatype
            )
        case .langLiteral(let value, let language),
             .dirLangLiteral(let value, let language, _):
            return literalFootprint(
                maximumLexicalUTF8Count: UInt64(value.utf8.count),
                annotation: language
            )
        case .rdfTerm(let term):
            return try RDFTermRetainedFootprint.measure(term)
        }
    }

    private static func literalFootprint(
        maximumLexicalUTF8Count: UInt64,
        annotation: String
    ) -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: termNodeByteCount
                + literalPayloadByteCount
                + stringStorageByteCount
                + maximumLexicalUTF8Count
                + stringStorageByteCount
                + UInt64(annotation.utf8.count)
        )
    }

    private static func iriFootprint(
        utf8Count: UInt64
    ) -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: termNodeByteCount + stringStorageByteCount + utf8Count
        )
    }

    private static func blankNodeFootprint(
        utf8Count: UInt64
    ) -> DatabaseIntermediateFootprint {
        DatabaseIntermediateFootprint(
            bytes: termNodeByteCount + stringStorageByteCount + utf8Count
        )
    }

    private static func decimalLexicalUTF8Count(
        _ decimal: ExactDecimal
    ) throws -> UInt64 {
        let digitCount = decimalDigitCount(decimal.coefficient.magnitude)
        let signCount: UInt64 = decimal.coefficient < 0 ? 1 : 0
        let required: UInt64
        if decimal.scale <= 0 {
            required = try checkedAdd(
                try checkedAdd(signCount, digitCount),
                UInt64(-Int64(decimal.scale))
            )
        } else if digitCount > UInt64(decimal.scale) {
            required = try checkedAdd(
                try checkedAdd(signCount, digitCount),
                1
            )
        } else {
            required = try checkedAdd(
                try checkedAdd(signCount, 2),
                UInt64(decimal.scale)
            )
        }
        let maximum = UInt64(SPARQLExecutionLimits.maximumLiteralUTF8Count)
        guard required <= maximum else {
            throw SPARQLLiteralConversionError.literalTooLarge(
                requiredUTF8Count: required,
                maximumUTF8Count: maximum
            )
        }
        return required
    }

    private static func decimalDigitCount(_ value: UInt128) -> UInt64 {
        var remaining = value
        var count: UInt64 = 1
        while remaining >= 10 {
            remaining /= 10
            count += 1
        }
        return count
    }

    private static func base64UTF8Count(_ byteCount: Int) throws -> UInt64 {
        let count = UInt64(byteCount)
        let rounded = try checkedAdd(count, 2)
        let groups = rounded / 3
        let (encoded, overflow) = groups.multipliedReportingOverflow(by: 4)
        guard !overflow else {
            throw SPARQLLiteralConversionError.literalTooLarge(
                requiredUTF8Count: UInt64.max,
                maximumUTF8Count: UInt64(
                    SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            )
        }
        return encoded
    }

    private static func checkedAdd(
        _ left: UInt64,
        _ right: UInt64
    ) throws -> UInt64 {
        let (sum, overflow) = left.addingReportingOverflow(right)
        guard !overflow else {
            throw SPARQLLiteralConversionError.literalTooLarge(
                requiredUTF8Count: UInt64.max,
                maximumUTF8Count: UInt64(
                    SPARQLExecutionLimits.maximumLiteralUTF8Count
                )
            )
        }
        return sum
    }
}

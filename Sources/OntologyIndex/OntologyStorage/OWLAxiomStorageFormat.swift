import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// The bounded binary representation owned by persistent ontology storage.
///
/// Recursive expressions are length-delimited so `StorageFrameLimits` applies
/// the same depth, collection, string, and frame budgets during writes and
/// reads. The format is independent of DatabaseWire.
enum OWLAxiomStorageFormat {
    private static let magic: UInt32 = 0x3141_574F

    static func encode(
        _ axiom: OWLAxiom,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            writer.writeUInt32(magic)
            try write(axiom, into: &writer)
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> OWLAxiom {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == magic else {
            throw .invalidMagic
        }
        let axiom = try readAxiom(from: &reader)
        try reader.ensureFullyRead()
        return axiom
    }

    private static func write(
        _ axiom: OWLAxiom,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch axiom {
        case .subClassOf(let sub, let sup):
            writer.writeUInt8(0)
            try write(sub, into: &writer)
            try write(sup, into: &writer)
        case .equivalentClasses(let values):
            writer.writeUInt8(1)
            try write(values, into: &writer)
        case .disjointClasses(let values):
            writer.writeUInt8(2)
            try write(values, into: &writer)
        case .disjointUnion(let classIRI, let disjuncts):
            writer.writeUInt8(3)
            try writer.writeString(classIRI)
            try write(disjuncts, into: &writer)
        case .subObjectPropertyOf(let sub, let sup):
            writer.writeUInt8(4)
            try writer.writeString(sub)
            try writer.writeString(sup)
        case .subPropertyChainOf(let chain, let sup):
            writer.writeUInt8(5)
            try write(chain, into: &writer)
            try writer.writeString(sup)
        case .equivalentObjectProperties(let values):
            writer.writeUInt8(6)
            try write(values, into: &writer)
        case .disjointObjectProperties(let values):
            writer.writeUInt8(7)
            try write(values, into: &writer)
        case .inverseObjectProperties(let first, let second):
            writer.writeUInt8(8)
            try writer.writeString(first)
            try writer.writeString(second)
        case .objectPropertyDomain(let property, let domain):
            writer.writeUInt8(9)
            try writer.writeString(property)
            try write(domain, into: &writer)
        case .objectPropertyRange(let property, let range):
            writer.writeUInt8(10)
            try writer.writeString(property)
            try write(range, into: &writer)
        case .functionalObjectProperty(let value):
            try writeUnaryString(tag: 11, value: value, into: &writer)
        case .inverseFunctionalObjectProperty(let value):
            try writeUnaryString(tag: 12, value: value, into: &writer)
        case .transitiveObjectProperty(let value):
            try writeUnaryString(tag: 13, value: value, into: &writer)
        case .symmetricObjectProperty(let value):
            try writeUnaryString(tag: 14, value: value, into: &writer)
        case .asymmetricObjectProperty(let value):
            try writeUnaryString(tag: 15, value: value, into: &writer)
        case .reflexiveObjectProperty(let value):
            try writeUnaryString(tag: 16, value: value, into: &writer)
        case .irreflexiveObjectProperty(let value):
            try writeUnaryString(tag: 17, value: value, into: &writer)
        case .subDataPropertyOf(let sub, let sup):
            writer.writeUInt8(18)
            try writer.writeString(sub)
            try writer.writeString(sup)
        case .equivalentDataProperties(let values):
            writer.writeUInt8(19)
            try write(values, into: &writer)
        case .disjointDataProperties(let values):
            writer.writeUInt8(20)
            try write(values, into: &writer)
        case .dataPropertyDomain(let property, let domain):
            writer.writeUInt8(21)
            try writer.writeString(property)
            try write(domain, into: &writer)
        case .dataPropertyRange(let property, let range):
            writer.writeUInt8(22)
            try writer.writeString(property)
            try write(range, into: &writer)
        case .functionalDataProperty(let value):
            try writeUnaryString(tag: 23, value: value, into: &writer)
        case .classAssertion(let individual, let classExpression):
            writer.writeUInt8(24)
            try writer.writeString(individual)
            try write(classExpression, into: &writer)
        case .objectPropertyAssertion(let subject, let property, let object):
            writer.writeUInt8(25)
            try writer.writeString(subject)
            try writer.writeString(property)
            try writer.writeString(object)
        case .negativeObjectPropertyAssertion(
            let subject,
            let property,
            let object
        ):
            writer.writeUInt8(26)
            try writer.writeString(subject)
            try writer.writeString(property)
            try writer.writeString(object)
        case .dataPropertyAssertion(let subject, let property, let value):
            writer.writeUInt8(27)
            try writer.writeString(subject)
            try writer.writeString(property)
            try write(value, into: &writer)
        case .negativeDataPropertyAssertion(
            let subject,
            let property,
            let value
        ):
            writer.writeUInt8(28)
            try writer.writeString(subject)
            try writer.writeString(property)
            try write(value, into: &writer)
        case .sameIndividual(let values):
            writer.writeUInt8(29)
            try write(values, into: &writer)
        case .differentIndividuals(let values):
            writer.writeUInt8(30)
            try write(values, into: &writer)
        case .declareClass(let value):
            try writeUnaryString(tag: 31, value: value, into: &writer)
        case .declareObjectProperty(let value):
            try writeUnaryString(tag: 32, value: value, into: &writer)
        case .declareDataProperty(let value):
            try writeUnaryString(tag: 33, value: value, into: &writer)
        case .declareNamedIndividual(let value):
            try writeUnaryString(tag: 34, value: value, into: &writer)
        case .declareDatatype(let value):
            try writeUnaryString(tag: 35, value: value, into: &writer)
        case .declareAnnotationProperty(let value):
            try writeUnaryString(tag: 36, value: value, into: &writer)
        }
    }

    private static func readAxiom(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OWLAxiom {
        switch try reader.readUInt8() {
        case 0:
            return .subClassOf(
                sub: try readClassExpression(from: &reader),
                sup: try readClassExpression(from: &reader)
            )
        case 1:
            return .equivalentClasses(
                try readClassExpressions(from: &reader)
            )
        case 2:
            return .disjointClasses(
                try readClassExpressions(from: &reader)
            )
        case 3:
            return .disjointUnion(
                class_: try reader.readString(),
                disjuncts: try readClassExpressions(from: &reader)
            )
        case 4:
            return .subObjectPropertyOf(
                sub: try reader.readString(),
                sup: try reader.readString()
            )
        case 5:
            return .subPropertyChainOf(
                chain: try readStrings(from: &reader),
                sup: try reader.readString()
            )
        case 6:
            return .equivalentObjectProperties(
                try readStrings(from: &reader)
            )
        case 7:
            return .disjointObjectProperties(
                try readStrings(from: &reader)
            )
        case 8:
            return .inverseObjectProperties(
                first: try reader.readString(),
                second: try reader.readString()
            )
        case 9:
            return .objectPropertyDomain(
                property: try reader.readString(),
                domain: try readClassExpression(from: &reader)
            )
        case 10:
            return .objectPropertyRange(
                property: try reader.readString(),
                range: try readClassExpression(from: &reader)
            )
        case 11:
            return .functionalObjectProperty(try reader.readString())
        case 12:
            return .inverseFunctionalObjectProperty(try reader.readString())
        case 13:
            return .transitiveObjectProperty(try reader.readString())
        case 14:
            return .symmetricObjectProperty(try reader.readString())
        case 15:
            return .asymmetricObjectProperty(try reader.readString())
        case 16:
            return .reflexiveObjectProperty(try reader.readString())
        case 17:
            return .irreflexiveObjectProperty(try reader.readString())
        case 18:
            return .subDataPropertyOf(
                sub: try reader.readString(),
                sup: try reader.readString()
            )
        case 19:
            return .equivalentDataProperties(try readStrings(from: &reader))
        case 20:
            return .disjointDataProperties(try readStrings(from: &reader))
        case 21:
            return .dataPropertyDomain(
                property: try reader.readString(),
                domain: try readClassExpression(from: &reader)
            )
        case 22:
            return .dataPropertyRange(
                property: try reader.readString(),
                range: try readDataRange(from: &reader)
            )
        case 23:
            return .functionalDataProperty(try reader.readString())
        case 24:
            return .classAssertion(
                individual: try reader.readString(),
                class_: try readClassExpression(from: &reader)
            )
        case 25:
            return .objectPropertyAssertion(
                subject: try reader.readString(),
                property: try reader.readString(),
                object: try reader.readString()
            )
        case 26:
            return .negativeObjectPropertyAssertion(
                subject: try reader.readString(),
                property: try reader.readString(),
                object: try reader.readString()
            )
        case 27:
            return .dataPropertyAssertion(
                subject: try reader.readString(),
                property: try reader.readString(),
                value: try readLiteral(from: &reader)
            )
        case 28:
            return .negativeDataPropertyAssertion(
                subject: try reader.readString(),
                property: try reader.readString(),
                value: try readLiteral(from: &reader)
            )
        case 29:
            return .sameIndividual(try readStrings(from: &reader))
        case 30:
            return .differentIndividuals(try readStrings(from: &reader))
        case 31:
            return .declareClass(try reader.readString())
        case 32:
            return .declareObjectProperty(try reader.readString())
        case 33:
            return .declareDataProperty(try reader.readString())
        case 34:
            return .declareNamedIndividual(try reader.readString())
        case 35:
            return .declareDatatype(try reader.readString())
        case 36:
            return .declareAnnotationProperty(try reader.readString())
        default:
            throw .invalidValue
        }
    }

    private static func write(
        _ expression: OWLClassExpression,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeLengthPrefixed {
            (child: inout StorageFrameEncoder) throws(StorageFrameError) in
            switch expression {
            case .named(let iri):
                child.writeUInt8(0)
                try child.writeString(iri)
            case .thing:
                child.writeUInt8(1)
            case .nothing:
                child.writeUInt8(2)
            case .intersection(let values):
                child.writeUInt8(3)
                try write(values, into: &child)
            case .union(let values):
                child.writeUInt8(4)
                try write(values, into: &child)
            case .complement(let value):
                child.writeUInt8(5)
                try write(value, into: &child)
            case .oneOf(let values):
                child.writeUInt8(6)
                try write(values, into: &child)
            case .someValuesFrom(let property, let filler):
                child.writeUInt8(7)
                try child.writeString(property)
                try write(filler, into: &child)
            case .allValuesFrom(let property, let filler):
                child.writeUInt8(8)
                try child.writeString(property)
                try write(filler, into: &child)
            case .hasValue(let property, let individual):
                child.writeUInt8(9)
                try child.writeString(property)
                try child.writeString(individual)
            case .hasSelf(let property):
                child.writeUInt8(10)
                try child.writeString(property)
            case .minCardinality(let property, let count, let filler):
                child.writeUInt8(11)
                try writeCardinality(
                    property: property,
                    count: count,
                    filler: filler,
                    into: &child
                )
            case .maxCardinality(let property, let count, let filler):
                child.writeUInt8(12)
                try writeCardinality(
                    property: property,
                    count: count,
                    filler: filler,
                    into: &child
                )
            case .exactCardinality(let property, let count, let filler):
                child.writeUInt8(13)
                try writeCardinality(
                    property: property,
                    count: count,
                    filler: filler,
                    into: &child
                )
            case .dataSomeValuesFrom(let property, let range):
                child.writeUInt8(14)
                try child.writeString(property)
                try write(range, into: &child)
            case .dataAllValuesFrom(let property, let range):
                child.writeUInt8(15)
                try child.writeString(property)
                try write(range, into: &child)
            case .dataHasValue(let property, let literal):
                child.writeUInt8(16)
                try child.writeString(property)
                try write(literal, into: &child)
            case .dataMinCardinality(let property, let count, let range):
                child.writeUInt8(17)
                try writeCardinality(
                    property: property,
                    count: count,
                    range: range,
                    into: &child
                )
            case .dataMaxCardinality(let property, let count, let range):
                child.writeUInt8(18)
                try writeCardinality(
                    property: property,
                    count: count,
                    range: range,
                    into: &child
                )
            case .dataExactCardinality(let property, let count, let range):
                child.writeUInt8(19)
                try writeCardinality(
                    property: property,
                    count: count,
                    range: range,
                    into: &child
                )
            }
        }
    }

    private static func readClassExpression(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OWLClassExpression {
        try reader.readLengthPrefixed {
            (child: inout StorageFrameDecoder) throws(StorageFrameError) in
            switch try child.readUInt8() {
            case 0:
                return .named(try child.readString())
            case 1:
                return .thing
            case 2:
                return .nothing
            case 3:
                return .intersection(
                    try readClassExpressions(from: &child)
                )
            case 4:
                return .union(try readClassExpressions(from: &child))
            case 5:
                return .complement(
                    try readClassExpression(from: &child)
                )
            case 6:
                return .oneOf(try readStrings(from: &child))
            case 7:
                return .someValuesFrom(
                    property: try child.readString(),
                    filler: try readClassExpression(from: &child)
                )
            case 8:
                return .allValuesFrom(
                    property: try child.readString(),
                    filler: try readClassExpression(from: &child)
                )
            case 9:
                return .hasValue(
                    property: try child.readString(),
                    individual: try child.readString()
                )
            case 10:
                return .hasSelf(property: try child.readString())
            case 11:
                return .minCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    filler: try readOptionalClassExpression(from: &child)
                )
            case 12:
                return .maxCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    filler: try readOptionalClassExpression(from: &child)
                )
            case 13:
                return .exactCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    filler: try readOptionalClassExpression(from: &child)
                )
            case 14:
                return .dataSomeValuesFrom(
                    property: try child.readString(),
                    range: try readDataRange(from: &child)
                )
            case 15:
                return .dataAllValuesFrom(
                    property: try child.readString(),
                    range: try readDataRange(from: &child)
                )
            case 16:
                return .dataHasValue(
                    property: try child.readString(),
                    literal: try readLiteral(from: &child)
                )
            case 17:
                return .dataMinCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    range: try readOptionalDataRange(from: &child)
                )
            case 18:
                return .dataMaxCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    range: try readOptionalDataRange(from: &child)
                )
            case 19:
                return .dataExactCardinality(
                    property: try child.readString(),
                    n: try readInt(from: &child),
                    range: try readOptionalDataRange(from: &child)
                )
            default:
                throw StorageFrameError.invalidValue
            }
        }
    }

    private static func write(
        _ range: OWLDataRange,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeLengthPrefixed {
            (child: inout StorageFrameEncoder) throws(StorageFrameError) in
            switch range {
            case .datatype(let iri):
                child.writeUInt8(0)
                try child.writeString(iri)
            case .dataIntersectionOf(let values):
                child.writeUInt8(1)
                try writeDataRanges(values, into: &child)
            case .dataUnionOf(let values):
                child.writeUInt8(2)
                try writeDataRanges(values, into: &child)
            case .dataComplementOf(let value):
                child.writeUInt8(3)
                try write(value, into: &child)
            case .dataOneOf(let values):
                child.writeUInt8(4)
                try child.writeCount(values.count)
                for value in values {
                    try write(value, into: &child)
                }
            case .datatypeRestriction(let datatype, let facets):
                child.writeUInt8(5)
                try child.writeString(datatype)
                try child.writeCount(facets.count)
                for facet in facets {
                    try child.writeString(facet.facet.rawValue)
                    try write(facet.value, into: &child)
                }
            }
        }
    }

    private static func readDataRange(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OWLDataRange {
        try reader.readLengthPrefixed {
            (child: inout StorageFrameDecoder) throws(StorageFrameError) in
            switch try child.readUInt8() {
            case 0:
                return .datatype(try child.readString())
            case 1:
                return .dataIntersectionOf(
                    try readDataRanges(from: &child)
                )
            case 2:
                return .dataUnionOf(try readDataRanges(from: &child))
            case 3:
                return .dataComplementOf(
                    try readDataRange(from: &child)
                )
            case 4:
                let count = try child.readCount()
                var values: [RDFLiteral] = []
                values.reserveCapacity(count)
                for _ in 0..<count {
                    values.append(try readLiteral(from: &child))
                }
                return .dataOneOf(values)
            case 5:
                let datatype = try child.readString()
                let count = try child.readCount()
                var facets: [FacetRestriction] = []
                facets.reserveCapacity(count)
                for _ in 0..<count {
                    let rawFacet = try child.readString()
                    guard let facet = XSDFacet.allCases.first(
                        where: { $0.rawValue == rawFacet }
                    ) else {
                        throw StorageFrameError.invalidValue
                    }
                    facets.append(FacetRestriction(
                        facet: facet,
                        value: try readLiteral(from: &child)
                    ))
                }
                return .datatypeRestriction(
                    datatype: datatype,
                    facets: facets
                )
            default:
                throw StorageFrameError.invalidValue
            }
        }
    }

    private static func write(
        _ literal: RDFLiteral,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try StorageValueEncoder.write(
            .rdfTerm(.literal(literal)),
            into: &writer
        )
    }

    private static func readLiteral(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> RDFLiteral {
        guard case .rdfTerm(.literal(let literal)) =
                try StorageValueDecoder.read(from: &reader) else {
            throw .invalidValue
        }
        return literal
    }

    private static func writeUnaryString(
        tag: UInt8,
        value: String,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        writer.writeUInt8(tag)
        try writer.writeString(value)
    }

    private static func write(
        _ values: [String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(values.count)
        for value in values {
            try writer.writeString(value)
        }
    }

    private static func readStrings(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String] {
        let count = try reader.readCount()
        var values: [String] = []
        values.reserveCapacity(count)
        for _ in 0..<count {
            values.append(try reader.readString())
        }
        return values
    }

    private static func write(
        _ values: [OWLClassExpression],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(values.count)
        for value in values {
            try write(value, into: &writer)
        }
    }

    private static func readClassExpressions(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [OWLClassExpression] {
        let count = try reader.readCount()
        var values: [OWLClassExpression] = []
        values.reserveCapacity(count)
        for _ in 0..<count {
            values.append(try readClassExpression(from: &reader))
        }
        return values
    }

    private static func writeDataRanges(
        _ values: [OWLDataRange],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(values.count)
        for value in values {
            try write(value, into: &writer)
        }
    }

    private static func readDataRanges(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [OWLDataRange] {
        let count = try reader.readCount()
        var values: [OWLDataRange] = []
        values.reserveCapacity(count)
        for _ in 0..<count {
            values.append(try readDataRange(from: &reader))
        }
        return values
    }

    private static func writeCardinality(
        property: String,
        count: Int,
        filler: OWLClassExpression?,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(property)
        try write(count, into: &writer)
        writer.writeBool(filler != nil)
        if let filler {
            try write(filler, into: &writer)
        }
    }

    private static func writeCardinality(
        property: String,
        count: Int,
        range: OWLDataRange?,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(property)
        try write(count, into: &writer)
        writer.writeBool(range != nil)
        if let range {
            try write(range, into: &writer)
        }
    }

    private static func readOptionalClassExpression(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OWLClassExpression? {
        try reader.readBool()
            ? try readClassExpression(from: &reader)
            : nil
    }

    private static func readOptionalDataRange(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> OWLDataRange? {
        try reader.readBool() ? try readDataRange(from: &reader) : nil
    }

    private static func write(
        _ value: Int,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let stored = Int64(exactly: value) else {
            throw .integerOutOfRange
        }
        writer.writeInt64(stored)
    }

    private static func readInt(
        from reader: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Int {
        guard let value = Int(exactly: try reader.readInt64()) else {
            throw .integerOutOfRange
        }
        return value
    }
}

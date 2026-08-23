import DatabaseEngine
import DatabaseKit
import DatabaseTypes

enum SHACLShapesGraphStorageFormat {
    private static let magic: UInt32 = 0x4C43_4853
    private static let version: UInt16 = 1

    static func encode(
        _ graph: SHACLShapesGraph,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) {
            (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
            encoder.writeUInt32(magic)
            encoder.writeUInt16(version)
            try encoder.writeString(graph.iri)
            let mappings = graph.prefixes.mappings.sorted {
                $0.key.utf8.lexicographicallyPrecedes($1.key.utf8)
            }
            try encoder.writeCount(mappings.count)
            for mapping in mappings {
                try encoder.writeString(mapping.key)
                try encoder.writeString(mapping.value)
            }
            switch graph.entailment {
            case .none: encoder.writeUInt8(1)
            case .rdfs: encoder.writeUInt8(2)
            case .owl: encoder.writeUInt8(3)
            }
            try writeShapes(graph.shapes, to: &encoder)
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> SHACLShapesGraph {
        var decoder = try StorageFrameDecoder(bytes, limits: limits)
        guard try decoder.readUInt32() == magic else {
            throw .invalidMagic
        }
        let decodedVersion = try decoder.readUInt16()
        guard decodedVersion == version else {
            throw .unsupportedVersion(decodedVersion)
        }
        let iri = try decoder.readString()
        let prefixCount = try decoder.readCount()
        var mappings: [String: String] = [:]
        mappings.reserveCapacity(prefixCount)
        for _ in 0..<prefixCount {
            let prefix = try decoder.readString()
            guard mappings.updateValue(
                try decoder.readString(),
                forKey: prefix
            ) == nil else {
                throw .invalidValue
            }
        }
        let entailment: SHACLEntailment
        switch try decoder.readUInt8() {
        case 1: entailment = .none
        case 2: entailment = .rdfs
        case 3: entailment = .owl
        case let tag: throw .invalidValueTag(tag)
        }
        let shapes = try readShapes(from: &decoder)
        try decoder.ensureFullyRead()
        return SHACLShapesGraph(
            iri: iri,
            shapes: shapes,
            prefixes: PrefixMap(mappings),
            entailment: entailment
        )
    }

    private static func writeShapes(
        _ shapes: [SHACLShape],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(shapes.count)
        for shape in shapes {
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try writeShape(shape, to: &child)
            }
        }
    }

    private static func readShapes(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [SHACLShape] {
        let count = try decoder.readCount()
        var shapes: [SHACLShape] = []
        shapes.reserveCapacity(count)
        for _ in 0..<count {
            shapes.append(try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readShape(from: &child)
            })
        }
        return shapes
    }

    private static func writeShape(
        _ shape: SHACLShape,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch shape {
        case .node(let value):
            encoder.writeUInt8(1)
            try writeNodeShape(value, to: &encoder)
        case .property(let value):
            encoder.writeUInt8(2)
            try writePropertyShape(value, to: &encoder)
        }
    }

    private static func readShape(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLShape {
        switch try decoder.readUInt8() {
        case 1: return .node(try readNodeShape(from: &decoder))
        case 2: return .property(try readPropertyShape(from: &decoder))
        case let tag: throw .invalidValueTag(tag)
        }
    }

    private static func writeNodeShape(
        _ shape: NodeShape,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writeOptionalTerm(shape.identifier, to: &encoder)
        try writeTargets(shape.targets, to: &encoder)
        try writeConstraints(shape.constraints, to: &encoder)
        try writePropertyShapes(shape.propertyShapes, to: &encoder)
        writeSeverity(shape.severity, to: &encoder)
        try writeStrings(shape.messages, to: &encoder)
        encoder.writeBool(shape.deactivated)
    }

    private static func readNodeShape(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> NodeShape {
        NodeShape(
            identifier: try readOptionalTerm(from: &decoder),
            targets: try readTargets(from: &decoder),
            constraints: try readConstraints(from: &decoder),
            propertyShapes: try readPropertyShapes(from: &decoder),
            severity: try readSeverity(from: &decoder),
            messages: try readStrings(from: &decoder),
            deactivated: try decoder.readBool()
        )
    }

    private static func writePropertyShape(
        _ shape: PropertyShape,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writeOptionalTerm(shape.identifier, to: &encoder)
        try encoder.writeLengthPrefixed {
            (child: inout StorageFrameEncoder) throws(StorageFrameError) in
            try writePath(shape.path, to: &child)
        }
        try writeTargets(shape.targets, to: &encoder)
        try writeConstraints(shape.constraints, to: &encoder)
        try writePropertyShapes(shape.propertyShapes, to: &encoder)
        writeSeverity(shape.severity, to: &encoder)
        try writeStrings(shape.messages, to: &encoder)
        encoder.writeBool(shape.deactivated)
        try encoder.writeOptionalString(shape.name)
        try encoder.writeOptionalString(shape.shapeDescription)
        encoder.writeBool(shape.order != nil)
        if let order = shape.order { encoder.writeDouble(order) }
        try encoder.writeOptionalString(shape.group)
        try writeOptionalTerm(shape.defaultValue, to: &encoder)
    }

    private static func readPropertyShape(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> PropertyShape {
        let identifier = try readOptionalTerm(from: &decoder)
        let path = try decoder.readLengthPrefixed {
            (child: inout StorageFrameDecoder) throws(StorageFrameError) in
            try readPath(from: &child)
        }
        let targets = try readTargets(from: &decoder)
        let constraints = try readConstraints(from: &decoder)
        let propertyShapes = try readPropertyShapes(from: &decoder)
        let severity = try readSeverity(from: &decoder)
        let messages = try readStrings(from: &decoder)
        let deactivated = try decoder.readBool()
        let name = try decoder.readOptionalString()
        let shapeDescription = try decoder.readOptionalString()
        let order = try decoder.readBool() ? try decoder.readDouble() : nil
        let group = try decoder.readOptionalString()
        let defaultValue = try readOptionalTerm(from: &decoder)
        return PropertyShape(
            identifier: identifier,
            path: path,
            targets: targets,
            constraints: constraints,
            propertyShapes: propertyShapes,
            severity: severity,
            messages: messages,
            deactivated: deactivated,
            name: name,
            shapeDescription: shapeDescription,
            order: order,
            group: group,
            defaultValue: defaultValue
        )
    }

    private static func writePropertyShapes(
        _ shapes: [PropertyShape],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(shapes.count)
        for shape in shapes {
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try writePropertyShape(shape, to: &child)
            }
        }
    }

    private static func readPropertyShapes(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [PropertyShape] {
        let count = try decoder.readCount()
        var shapes: [PropertyShape] = []
        shapes.reserveCapacity(count)
        for _ in 0..<count {
            shapes.append(try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readPropertyShape(from: &child)
            })
        }
        return shapes
    }

    private static func writeTargets(
        _ targets: [SHACLTarget],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(targets.count)
        for target in targets {
            switch target {
            case .node(let term):
                encoder.writeUInt8(1)
                try encoder.writeRDFTerm(term)
            case .class_(let iri):
                encoder.writeUInt8(2)
                try encoder.writeString(iri)
            case .subjectsOf(let iri):
                encoder.writeUInt8(3)
                try encoder.writeString(iri)
            case .objectsOf(let iri):
                encoder.writeUInt8(4)
                try encoder.writeString(iri)
            case .implicitClass:
                encoder.writeUInt8(5)
            }
        }
    }

    private static func readTargets(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [SHACLTarget] {
        let count = try decoder.readCount()
        var targets: [SHACLTarget] = []
        targets.reserveCapacity(count)
        for _ in 0..<count {
            switch try decoder.readUInt8() {
            case 1: targets.append(.node(try decoder.readRDFTerm()))
            case 2: targets.append(.class_(try decoder.readString()))
            case 3: targets.append(.subjectsOf(try decoder.readString()))
            case 4: targets.append(.objectsOf(try decoder.readString()))
            case 5: targets.append(.implicitClass)
            case let tag: throw .invalidValueTag(tag)
            }
        }
        return targets
    }

    private static func writePath(
        _ path: SHACLPath,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch path {
        case .predicate(let predicate):
            encoder.writeUInt8(1)
            try encoder.writeString(predicate.rawValue)
        case .inverse(let inner):
            encoder.writeUInt8(2)
            try writeNestedPath(inner, to: &encoder)
        case .sequence(let paths):
            encoder.writeUInt8(3)
            try writePaths(paths.elements, to: &encoder)
        case .alternative(let paths):
            encoder.writeUInt8(4)
            try writePaths(paths.elements, to: &encoder)
        case .zeroOrMore(let inner):
            encoder.writeUInt8(5)
            try writeNestedPath(inner, to: &encoder)
        case .oneOrMore(let inner):
            encoder.writeUInt8(6)
            try writeNestedPath(inner, to: &encoder)
        case .zeroOrOne(let inner):
            encoder.writeUInt8(7)
            try writeNestedPath(inner, to: &encoder)
        }
    }

    private static func readPath(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLPath {
        switch try decoder.readUInt8() {
        case 1:
            do {
                return .predicate(
                    try RDFPredicateIRI(decoder.readString())
                )
            } catch {
                throw .invalidValue
            }
        case 2: return .inverse(try readNestedPath(from: &decoder))
        case 3:
            return .sequence(try readPathList(from: &decoder))
        case 4:
            return .alternative(try readPathList(from: &decoder))
        case 5: return .zeroOrMore(try readNestedPath(from: &decoder))
        case 6: return .oneOrMore(try readNestedPath(from: &decoder))
        case 7: return .zeroOrOne(try readNestedPath(from: &decoder))
        case let tag: throw .invalidValueTag(tag)
        }
    }

    private static func writeNestedPath(
        _ path: SHACLPath,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeLengthPrefixed {
            (child: inout StorageFrameEncoder) throws(StorageFrameError) in
            try writePath(path, to: &child)
        }
    }

    private static func readNestedPath(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLPath {
        try decoder.readLengthPrefixed {
            (child: inout StorageFrameDecoder) throws(StorageFrameError) in
            try readPath(from: &child)
        }
    }

    private static func writePaths(
        _ paths: [SHACLPath],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(paths.count)
        for path in paths { try writeNestedPath(path, to: &encoder) }
    }

    private static func readPathList(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLPathList {
        let count = try decoder.readCount()
        var paths: [SHACLPath] = []
        paths.reserveCapacity(count)
        for _ in 0..<count {
            paths.append(try readNestedPath(from: &decoder))
        }
        do {
            return try SHACLPathList(consume paths)
        } catch {
            throw .invalidValue
        }
    }

    private static func writeConstraints(
        _ constraints: [SHACLConstraint],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(constraints.count)
        for constraint in constraints {
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try writeConstraint(constraint, to: &child)
            }
        }
    }

    private static func readConstraints(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [SHACLConstraint] {
        let count = try decoder.readCount()
        var constraints: [SHACLConstraint] = []
        constraints.reserveCapacity(count)
        for _ in 0..<count {
            constraints.append(try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readConstraint(from: &child)
            })
        }
        return constraints
    }

    private static func writeConstraint(
        _ value: SHACLConstraint,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch value {
        case .class_(let v): encoder.writeUInt8(1); try encoder.writeString(v)
        case .datatype(let v): encoder.writeUInt8(2); try encoder.writeString(v)
        case .nodeKind(let v):
            encoder.writeUInt8(3); writeNodeKind(v, to: &encoder)
        case .minCount(let v): encoder.writeUInt8(4); try writeInt(v, to: &encoder)
        case .maxCount(let v): encoder.writeUInt8(5); try writeInt(v, to: &encoder)
        case .minExclusive(let v): encoder.writeUInt8(6); try encoder.writeRDFTerm(v)
        case .maxExclusive(let v): encoder.writeUInt8(7); try encoder.writeRDFTerm(v)
        case .minInclusive(let v): encoder.writeUInt8(8); try encoder.writeRDFTerm(v)
        case .maxInclusive(let v): encoder.writeUInt8(9); try encoder.writeRDFTerm(v)
        case .minLength(let v): encoder.writeUInt8(10); try writeInt(v, to: &encoder)
        case .maxLength(let v): encoder.writeUInt8(11); try writeInt(v, to: &encoder)
        case .pattern(let pattern, let flags):
            encoder.writeUInt8(12); try encoder.writeString(pattern)
            try encoder.writeOptionalString(flags)
        case .languageIn(let v): encoder.writeUInt8(13); try writeStrings(v, to: &encoder)
        case .uniqueLang: encoder.writeUInt8(14)
        case .equals(let v): encoder.writeUInt8(15); try writeNestedPath(v, to: &encoder)
        case .disjoint(let v): encoder.writeUInt8(16); try writeNestedPath(v, to: &encoder)
        case .lessThan(let v): encoder.writeUInt8(17); try writeNestedPath(v, to: &encoder)
        case .lessThanOrEquals(let v): encoder.writeUInt8(18); try writeNestedPath(v, to: &encoder)
        case .not(let v): encoder.writeUInt8(19); try writeNestedShape(v, to: &encoder)
        case .and(let v): encoder.writeUInt8(20); try writeShapes(v, to: &encoder)
        case .or(let v): encoder.writeUInt8(21); try writeShapes(v, to: &encoder)
        case .xone(let v): encoder.writeUInt8(22); try writeShapes(v, to: &encoder)
        case .node(let v):
            encoder.writeUInt8(23)
            try encoder.writeLengthPrefixed {
                (child: inout StorageFrameEncoder) throws(StorageFrameError) in
                try writeNodeShape(v, to: &child)
            }
        case .qualifiedValueShape(let shape, let min, let max):
            encoder.writeUInt8(24); try writeNestedShape(shape, to: &encoder)
            try writeOptionalInt(min, to: &encoder)
            try writeOptionalInt(max, to: &encoder)
        case .closed(let v): encoder.writeUInt8(25); try writeStrings(v, to: &encoder)
        case .hasValue(let v): encoder.writeUInt8(26); try encoder.writeRDFTerm(v)
        case .in_(let values):
            encoder.writeUInt8(27); try encoder.writeCount(values.count)
            for value in values { try encoder.writeRDFTerm(value) }
        }
    }

    private static func readConstraint(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLConstraint {
        switch try decoder.readUInt8() {
        case 1: return .class_(try decoder.readString())
        case 2: return .datatype(try decoder.readString())
        case 3: return .nodeKind(try readNodeKind(from: &decoder))
        case 4: return .minCount(try readInt(from: &decoder))
        case 5: return .maxCount(try readInt(from: &decoder))
        case 6: return .minExclusive(try decoder.readRDFTerm())
        case 7: return .maxExclusive(try decoder.readRDFTerm())
        case 8: return .minInclusive(try decoder.readRDFTerm())
        case 9: return .maxInclusive(try decoder.readRDFTerm())
        case 10: return .minLength(try readInt(from: &decoder))
        case 11: return .maxLength(try readInt(from: &decoder))
        case 12:
            return .pattern(
                try decoder.readString(),
                flags: try decoder.readOptionalString()
            )
        case 13: return .languageIn(try readStrings(from: &decoder))
        case 14: return .uniqueLang
        case 15: return .equals(try readNestedPath(from: &decoder))
        case 16: return .disjoint(try readNestedPath(from: &decoder))
        case 17: return .lessThan(try readNestedPath(from: &decoder))
        case 18: return .lessThanOrEquals(try readNestedPath(from: &decoder))
        case 19: return .not(try readNestedShape(from: &decoder))
        case 20: return .and(try readShapes(from: &decoder))
        case 21: return .or(try readShapes(from: &decoder))
        case 22: return .xone(try readShapes(from: &decoder))
        case 23:
            return .node(try decoder.readLengthPrefixed {
                (child: inout StorageFrameDecoder) throws(StorageFrameError) in
                try readNodeShape(from: &child)
            })
        case 24:
            return .qualifiedValueShape(
                shape: try readNestedShape(from: &decoder),
                min: try readOptionalInt(from: &decoder),
                max: try readOptionalInt(from: &decoder)
            )
        case 25: return .closed(ignoredProperties: try readStrings(from: &decoder))
        case 26: return .hasValue(try decoder.readRDFTerm())
        case 27:
            let count = try decoder.readCount()
            var values: [RDFTerm] = []
            values.reserveCapacity(count)
            for _ in 0..<count { values.append(try decoder.readRDFTerm()) }
            return .in_(values)
        case let tag: throw .invalidValueTag(tag)
        }
    }

    private static func writeNestedShape(
        _ shape: SHACLShape,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeLengthPrefixed {
            (child: inout StorageFrameEncoder) throws(StorageFrameError) in
            try writeShape(shape, to: &child)
        }
    }

    private static func readNestedShape(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLShape {
        try decoder.readLengthPrefixed {
            (child: inout StorageFrameDecoder) throws(StorageFrameError) in
            try readShape(from: &child)
        }
    }

    private static func writeOptionalTerm(
        _ value: RDFTerm?,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeBool(value != nil)
        if let value { try encoder.writeRDFTerm(value) }
    }

    private static func readOptionalTerm(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> RDFTerm? {
        try decoder.readBool() ? try decoder.readRDFTerm() : nil
    }

    private static func writeStrings(
        _ values: [String],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(values.count)
        for value in values { try encoder.writeString(value) }
    }

    private static func readStrings(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String] {
        let count = try decoder.readCount()
        var values: [String] = []
        values.reserveCapacity(count)
        for _ in 0..<count { values.append(try decoder.readString()) }
        return values
    }

    private static func writeInt(
        _ value: Int,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        guard let value = Int64(exactly: value) else {
            throw .integerOutOfRange
        }
        encoder.writeInt64(value)
    }

    private static func readInt(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Int {
        guard let value = Int(exactly: try decoder.readInt64()) else {
            throw .integerOutOfRange
        }
        return value
    }

    private static func writeOptionalInt(
        _ value: Int?,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeBool(value != nil)
        if let value { try writeInt(value, to: &encoder) }
    }

    private static func readOptionalInt(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> Int? {
        try decoder.readBool() ? try readInt(from: &decoder) : nil
    }

    private static func writeSeverity(
        _ value: SHACLSeverity,
        to encoder: inout StorageFrameEncoder
    ) {
        switch value {
        case .violation: encoder.writeUInt8(1)
        case .warning: encoder.writeUInt8(2)
        case .info: encoder.writeUInt8(3)
        }
    }

    private static func readSeverity(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLSeverity {
        switch try decoder.readUInt8() {
        case 1: return .violation
        case 2: return .warning
        case 3: return .info
        case let tag: throw .invalidValueTag(tag)
        }
    }

    private static func writeNodeKind(
        _ value: SHACLNodeKind,
        to encoder: inout StorageFrameEncoder
    ) {
        switch value {
        case .blankNode: encoder.writeUInt8(1)
        case .iri: encoder.writeUInt8(2)
        case .literal: encoder.writeUInt8(3)
        case .blankNodeOrIRI: encoder.writeUInt8(4)
        case .blankNodeOrLiteral: encoder.writeUInt8(5)
        case .iriOrLiteral: encoder.writeUInt8(6)
        }
    }

    private static func readNodeKind(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> SHACLNodeKind {
        switch try decoder.readUInt8() {
        case 1: return .blankNode
        case 2: return .iri
        case 3: return .literal
        case 4: return .blankNodeOrIRI
        case 5: return .blankNodeOrLiteral
        case 6: return .iriOrLiteral
        case let tag: throw .invalidValueTag(tag)
        }
    }
}

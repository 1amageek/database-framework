import DatabaseEngine
import DatabaseKit
import DatabaseTypes

/// Persistent representation of one SQL/PGQ property graph definition.
///
/// This format is owned by the execution layer and is deliberately independent
/// of the client/server wire protocol.
package enum PropertyGraphDefinitionStorageFormat {
    private static let magic: UInt32 = 0x5047_4446
    private static let version: UInt16 = 1

    private enum LabelEncodingStep {
        case expression(LabelExpression, depth: Int)
        case collection(
            [LabelExpression],
            nextIndex: Int,
            depth: Int
        )
    }

    private struct LabelDecodingFrame {
        let tag: UInt8
        let elementCount: Int
        let depth: Int
        var elements: [LabelExpression]

        init(tag: UInt8, elementCount: Int, depth: Int) {
            self.tag = tag
            self.elementCount = elementCount
            self.depth = depth
            self.elements = []
            self.elements.reserveCapacity(elementCount)
        }

        consuming func expression() throws(
            StorageFrameError
        ) -> LabelExpression {
            switch tag {
            case 2:
                return .or(elements)
            case 3:
                return .and(elements)
            default:
                throw .invalidValueTag(tag)
            }
        }
    }

    package static func encode(
        _ definition: CreateGraphStatement,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) {
            (encoder: inout StorageFrameEncoder)
                throws(StorageFrameError) in
            encoder.writeUInt32(magic)
            encoder.writeUInt16(version)
            try encoder.writeString(definition.graphName)
            encoder.writeBool(definition.ifNotExists)

            try encoder.writeCount(definition.vertexTables.count)
            for table in definition.vertexTables {
                try encode(table, to: &encoder)
            }

            try encoder.writeCount(definition.edgeTables.count)
            for table in definition.edgeTables {
                try encode(table, to: &encoder)
            }
        }
    }

    package static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> CreateGraphStatement {
        var decoder = try StorageFrameDecoder(bytes, limits: limits)
        guard try decoder.readUInt32() == magic else {
            throw .invalidMagic
        }
        let decodedVersion = try decoder.readUInt16()
        guard decodedVersion == version else {
            throw .unsupportedVersion(decodedVersion)
        }

        let graphName = try decoder.readString()
        let ifNotExists = try decoder.readBool()

        let vertexTableCount = try decoder.readCount()
        var vertexTables: [VertexTableDefinition] = []
        vertexTables.reserveCapacity(vertexTableCount)
        for _ in 0..<vertexTableCount {
            vertexTables.append(try decodeVertexTable(from: &decoder))
        }

        let edgeTableCount = try decoder.readCount()
        var edgeTables: [EdgeTableDefinition] = []
        edgeTables.reserveCapacity(edgeTableCount)
        for _ in 0..<edgeTableCount {
            edgeTables.append(try decodeEdgeTable(from: &decoder))
        }

        try decoder.ensureFullyRead()
        return CreateGraphStatement(
            graphName: graphName,
            ifNotExists: ifNotExists,
            vertexTables: vertexTables,
            edgeTables: edgeTables
        )
    }

    private static func encode(
        _ table: VertexTableDefinition,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(table.tableName)
        try encoder.writeOptionalString(table.alias)
        try encode(table.keyColumns, to: &encoder)
        try encode(table.labelExpression, to: &encoder)
        try encode(table.propertiesSpec, to: &encoder)
    }

    private static func encode(
        _ table: EdgeTableDefinition,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(table.tableName)
        try encoder.writeOptionalString(table.alias)
        try encode(table.keyColumns, to: &encoder)
        try encode(table.sourceVertex, to: &encoder)
        try encode(table.destinationVertex, to: &encoder)
        try encode(table.labelExpression, to: &encoder)
        try encode(table.propertiesSpec, to: &encoder)
    }

    private static func encode(
        _ reference: VertexReference,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeString(reference.tableName)
        try encoder.writeCount(reference.keyColumns.count)
        for mapping in reference.keyColumns {
            try encoder.writeString(mapping.source)
            try encoder.writeString(mapping.target)
        }
    }

    private static func encode(
        _ strings: [String],
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try encoder.writeCount(strings.count)
        for string in strings {
            try encoder.writeString(string)
        }
    }

    private static func encode(
        _ expression: LabelExpression?,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeBool(expression != nil)
        guard let expression else { return }

        var steps: [LabelEncodingStep] = [
            .expression(expression, depth: 0)
        ]
        var encodedNodeCount = 0
        while let step = steps.popLast() {
            switch step {
            case .expression(let expression, let depth):
                try admitLabelNode(
                    depth: depth,
                    nodeCount: &encodedNodeCount,
                    limits: encoder.limits
                )
                switch expression {
                case .single(let value):
                    encoder.writeUInt8(0)
                    try encoder.writeString(value)
                case .column(let value):
                    encoder.writeUInt8(1)
                    try encoder.writeString(value)
                case .or(let values):
                    encoder.writeUInt8(2)
                    try encoder.writeCount(values.count)
                    if !values.isEmpty {
                        let childDepth = try nextLabelDepth(after: depth)
                        steps.append(
                            .collection(
                                values,
                                nextIndex: 0,
                                depth: childDepth
                            )
                        )
                    }
                case .and(let values):
                    encoder.writeUInt8(3)
                    try encoder.writeCount(values.count)
                    if !values.isEmpty {
                        let childDepth = try nextLabelDepth(after: depth)
                        steps.append(
                            .collection(
                                values,
                                nextIndex: 0,
                                depth: childDepth
                            )
                        )
                    }
                }

            case .collection(let values, let nextIndex, let depth):
                let followingIndex = nextIndex + 1
                if followingIndex < values.count {
                    steps.append(
                        .collection(
                            values,
                            nextIndex: followingIndex,
                            depth: depth
                        )
                    )
                }
                steps.append(.expression(values[nextIndex], depth: depth))
            }
        }
    }

    private static func encode(
        _ specification: PropertiesSpec?,
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeBool(specification != nil)
        guard let specification else { return }
        switch specification {
        case .all:
            encoder.writeUInt8(0)
        case .none:
            encoder.writeUInt8(1)
        case .columns(let columns):
            encoder.writeUInt8(2)
            try encode(columns, to: &encoder)
        case .allExcept(let columns):
            encoder.writeUInt8(3)
            try encode(columns, to: &encoder)
        }
    }

    private static func decodeVertexTable(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> VertexTableDefinition {
        VertexTableDefinition(
            tableName: try decoder.readString(),
            alias: try decoder.readOptionalString(),
            keyColumns: try decodeStrings(from: &decoder),
            labelExpression: try decodeLabelExpression(from: &decoder),
            propertiesSpec: try decodePropertiesSpec(from: &decoder)
        )
    }

    private static func decodeEdgeTable(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> EdgeTableDefinition {
        EdgeTableDefinition(
            tableName: try decoder.readString(),
            alias: try decoder.readOptionalString(),
            keyColumns: try decodeStrings(from: &decoder),
            sourceVertex: try decodeVertexReference(from: &decoder),
            destinationVertex: try decodeVertexReference(from: &decoder),
            labelExpression: try decodeLabelExpression(from: &decoder),
            propertiesSpec: try decodePropertiesSpec(from: &decoder)
        )
    }

    private static func decodeVertexReference(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> VertexReference {
        let tableName = try decoder.readString()
        let mappingCount = try decoder.readCount()
        var mappings: [KeyColumnMapping] = []
        mappings.reserveCapacity(mappingCount)
        for _ in 0..<mappingCount {
            mappings.append(
                KeyColumnMapping(
                    source: try decoder.readString(),
                    target: try decoder.readString()
                )
            )
        }
        return VertexReference(
            tableName: tableName,
            keyColumns: mappings
        )
    }

    private static func decodeStrings(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> [String] {
        let count = try decoder.readCount()
        var strings: [String] = []
        strings.reserveCapacity(count)
        for _ in 0..<count {
            strings.append(try decoder.readString())
        }
        return strings
    }

    private static func decodeLabelExpression(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> LabelExpression? {
        guard try decoder.readBool() else { return nil }

        var frames: [LabelDecodingFrame] = []
        var decodedNodeCount = 0
        var nextDepth = 0
        while true {
            try admitLabelNode(
                depth: nextDepth,
                nodeCount: &decodedNodeCount,
                limits: decoder.limits
            )

            let tag = try decoder.readUInt8()
            var completed: LabelExpression
            switch tag {
            case 0:
                completed = .single(try decoder.readString())
            case 1:
                completed = .column(try decoder.readString())
            case 2, 3:
                let elementCount = try decoder.readCount()
                if elementCount > 0 {
                    frames.append(
                        LabelDecodingFrame(
                            tag: tag,
                            elementCount: elementCount,
                            depth: nextDepth
                        )
                    )
                    nextDepth = try nextLabelDepth(after: nextDepth)
                    continue
                }
                completed = tag == 2 ? .or([]) : .and([])
            default:
                throw .invalidValueTag(tag)
            }

            while true {
                guard !frames.isEmpty else { return completed }
                let frameIndex = frames.index(before: frames.endIndex)
                frames[frameIndex].elements.append(completed)
                if frames[frameIndex].elements.count
                    < frames[frameIndex].elementCount {
                    nextDepth = try nextLabelDepth(
                        after: frames[frameIndex].depth
                    )
                    break
                }
                completed = try frames.removeLast().expression()
            }
        }
    }

    private static func decodePropertiesSpec(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) -> PropertiesSpec? {
        guard try decoder.readBool() else { return nil }
        switch try decoder.readUInt8() {
        case 0:
            return .all
        case 1:
            return PropertiesSpec.none
        case 2:
            return .columns(try decodeStrings(from: &decoder))
        case 3:
            return .allExcept(try decodeStrings(from: &decoder))
        case let tag:
            throw .invalidValueTag(tag)
        }
    }

    private static func admitLabelNode(
        depth: Int,
        nodeCount: inout Int,
        limits: StorageFrameLimits
    ) throws(StorageFrameError) {
        guard depth <= limits.maximumNestingDepth else {
            throw .nestingTooDeep(
                actual: depth,
                maximum: limits.maximumNestingDepth
            )
        }
        let nextCount = nodeCount.addingReportingOverflow(1)
        guard !nextCount.overflow else {
            throw .byteCountOverflow
        }
        guard nextCount.partialValue <= limits.maximumCollectionCount else {
            throw .collectionTooLarge(
                actual: nextCount.partialValue,
                maximum: limits.maximumCollectionCount
            )
        }
        nodeCount = nextCount.partialValue
    }

    private static func nextLabelDepth(
        after depth: Int
    ) throws(StorageFrameError) -> Int {
        let nextDepth = depth.addingReportingOverflow(1)
        guard !nextDepth.overflow else {
            throw .byteCountOverflow
        }
        return nextDepth.partialValue
    }
}

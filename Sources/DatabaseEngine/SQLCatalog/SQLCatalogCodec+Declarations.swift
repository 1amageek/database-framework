import DatabaseKit
import DatabaseTypes
import DatabaseWire

extension SQLCatalogCodec {
    static func writeBacking(_ backing: SQLColumnBacking,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch backing {
        case .field(let number):
            writer.writeUInt8(1)
            guard let value = Int64(exactly: number) else { throw .integerOutOfRange }
            writer.writeInt64(value)
        case .virtual(let type):
            writer.writeUInt8(2)
            do {
                try writer.writeBytes(QueryIRWireFormat.encodeColumnType(type,
                    limits: queryLimits(writer.limits)))
            } catch let error as StorageFrameError { throw error }
            catch { throw .invalidValue }
        }
    }

    static func readBacking(from reader: inout StorageFrameDecoder) throws -> SQLColumnBacking {
        let tag = try reader.readUInt8()
        switch tag {
        case 1:
            guard let number = Int(exactly: try reader.readInt64()) else {
                throw StorageFrameError.integerOutOfRange
            }
            return .field(number: number)
        case 2:
            return try .virtual(type: QueryIRWireFormat.decodeColumnType(reader.readBytes(),
                limits: queryLimits(reader.limits)))
        default: throw StorageFrameError.invalidValueTag(tag)
        }
    }

    static func writePolicy(_ policy: SQLColumnPolicy,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        switch policy {
        case .input(let expression):
            writer.writeUInt8(1)
            writer.writeBool(expression != nil)
            if let expression { try writeExpression(expression, into: &writer) }
        case .generated(let expression):
            writer.writeUInt8(2)
            try writeExpression(expression, into: &writer)
        case .identity(let identity):
            writer.writeUInt8(3)
            try writer.writeString(identity.sequenceID)
            switch identity.generation {
            case .always: writer.writeUInt8(1)
            case .byDefault: writer.writeUInt8(2)
            }
        }
    }

    static func readPolicy(from reader: inout StorageFrameDecoder) throws -> SQLColumnPolicy {
        let tag = try reader.readUInt8()
        switch tag {
        case 1:
            return try .input(defaultExpression: reader.readBool() ? readExpression(from: &reader) : nil)
        case 2: return try .generated(readExpression(from: &reader))
        case 3:
            let sequenceID = try reader.readString()
            let generationTag = try reader.readUInt8()
            let generation: SQLIdentityGeneration
            switch generationTag {
            case 1: generation = .always
            case 2: generation = .byDefault
            default: throw StorageFrameError.invalidValueTag(generationTag)
            }
            return try .identity(SQLIdentityColumn(sequenceID: sequenceID, generation: generation))
        default: throw StorageFrameError.invalidValueTag(tag)
        }
    }

    static func writeAttributes(_ attributes: SQLConstraintAttributes,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(attributes.id)
        try writer.writeString(attributes.name)
        switch attributes.timing {
        case .immediate: writer.writeUInt8(1)
        case .deferrable(let initiallyDeferred):
            writer.writeUInt8(2)
            writer.writeBool(initiallyDeferred)
        }
    }

    static func readAttributes(from reader: inout StorageFrameDecoder) throws -> SQLConstraintAttributes {
        let id = try reader.readString(), name = try reader.readString()
        let tag = try reader.readUInt8()
        let timing: SQLConstraintTiming
        switch tag {
        case 1: timing = .immediate
        case 2: timing = try .deferrable(initiallyDeferred: reader.readBool())
        default: throw StorageFrameError.invalidValueTag(tag)
        }
        return try SQLConstraintAttributes(id: id, name: name, timing: timing)
    }

    static func writeConstraint(_ constraint: SQLConstraint,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writeAttributes(constraint.attributes, into: &writer)
        switch constraint.kind {
        case .unique(let columns, let nulls):
            writer.writeUInt8(1)
            try writeIDs(columns, into: &writer)
            switch nulls {
            case .distinct: writer.writeUInt8(1)
            case .notDistinct: writer.writeUInt8(2)
            }
        case .notNull(let column):
            writer.writeUInt8(2)
            try writer.writeString(column)
        case .check(let expression):
            writer.writeUInt8(3)
            try writeExpression(expression, into: &writer)
        case .foreignKey(let key):
            writer.writeUInt8(4)
            try writeIDs(key.columnIDs, into: &writer)
            try writer.writeString(key.referencedRelationID)
            writer.writeBool(key.referencedColumnIDs != nil)
            if let columns = key.referencedColumnIDs { try writeIDs(columns, into: &writer) }
            switch key.match {
            case .simple: writer.writeUInt8(1)
            case .full: writer.writeUInt8(2)
            }
            writeAction(key.onUpdate, into: &writer)
            writeAction(key.onDelete, into: &writer)
            writer.writeBool(key.onDeleteColumnIDs != nil)
            if let columns = key.onDeleteColumnIDs { try writeIDs(columns, into: &writer) }
            writer.writeBool(key.referencedUniqueIndexID != nil)
            if let indexID = key.referencedUniqueIndexID { try writer.writeString(indexID) }
        }
    }

    static func readConstraint(from reader: inout StorageFrameDecoder) throws -> SQLConstraint {
        let attributes = try readAttributes(from: &reader)
        let tag = try reader.readUInt8()
        let kind: SQLConstraintKind
        switch tag {
        case 1:
            let columns = try readIDs(from: &reader), nullTag = try reader.readUInt8()
            let nulls: SQLNullUniqueness
            switch nullTag {
            case 1: nulls = .distinct
            case 2: nulls = .notDistinct
            default: throw StorageFrameError.invalidValueTag(nullTag)
            }
            kind = .unique(columnIDs: columns, nulls: nulls)
        case 2: kind = try .notNull(columnID: reader.readString())
        case 3: kind = try .check(readExpression(from: &reader))
        case 4:
            let columns = try readIDs(from: &reader)
            let referenced = try reader.readString()
            let target = try reader.readBool() ? readIDs(from: &reader) : nil
            let matchTag = try reader.readUInt8()
            let match: SQLForeignKeyMatch
            switch matchTag {
            case 1: match = .simple
            case 2: match = .full
            default: throw StorageFrameError.invalidValueTag(matchTag)
            }
            let onUpdate = try readAction(from: &reader), onDelete = try readAction(from: &reader)
            let subset = try reader.readBool() ? readIDs(from: &reader) : nil
            let uniqueIndexID = try reader.readBool() ? reader.readString() : nil
            kind = try .foreignKey(SQLForeignKey(columnIDs: columns,
                referencedRelationID: referenced, referencedColumnIDs: target, match: match,
                onUpdate: onUpdate, onDelete: onDelete, onDeleteColumnIDs: subset,
                referencedUniqueIndexID: uniqueIndexID))
        default: throw StorageFrameError.invalidValueTag(tag)
        }
        return try SQLConstraint(attributes: attributes, kind: kind)
    }

    static func writeSequence(_ sequence: SQLSequence,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeString(sequence.id)
        try writer.writeString(sequence.namespaceID)
        try writer.writeString(sequence.name)
        switch sequence.declaredType {
        case .smallint: writer.writeUInt8(1)
        case .integer: writer.writeUInt8(2)
        case .bigint: writer.writeUInt8(3)
        default: throw .invalidValue
        }
        writer.writeInt64(sequence.start)
        writer.writeInt64(sequence.increment)
        writer.writeInt64(sequence.minimum)
        writer.writeInt64(sequence.maximum)
        writer.writeBool(sequence.cycle)
        writer.writeBool(sequence.ownedBy != nil)
        if let owner = sequence.ownedBy {
            try writer.writeString(owner.relationID)
            try writer.writeString(owner.columnID)
        }
    }

    static func readSequence(from reader: inout StorageFrameDecoder) throws -> SQLSequence {
        let id = try reader.readString(), namespaceID = try reader.readString(), name = try reader.readString()
        let typeTag = try reader.readUInt8()
        let declaredType: DataType
        switch typeTag {
        case 1: declaredType = .smallint
        case 2: declaredType = .integer
        case 3: declaredType = .bigint
        default: throw StorageFrameError.invalidValueTag(typeTag)
        }
        let start = try reader.readInt64(), increment = try reader.readInt64()
        let minimum = try reader.readInt64(), maximum = try reader.readInt64()
        let cycle = try reader.readBool()
        let owner = try reader.readBool() ? SQLSequenceOwner(relationID: reader.readString(),
            columnID: reader.readString()) : nil
        return try SQLSequence(id: id, namespaceID: namespaceID, name: name,
            start: start, increment: increment, minimum: minimum, maximum: maximum,
            cycle: cycle, ownedBy: owner, declaredType: declaredType)
    }

    static func writeExpression(_ expression: Expression,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        do {
            // The shared QueryIR codec requires an owned value at this persistence boundary.
            try writer.writeBytes(QueryIRWireFormat.encodeExpression(expression,
                limits: queryLimits(writer.limits)))
        } catch let error as StorageFrameError { throw error }
        catch { throw .invalidValue }
    }

    private static func readExpression(from reader: inout StorageFrameDecoder) throws -> Expression {
        try QueryIRWireFormat.decodeExpression(reader.readBytes(), limits: queryLimits(reader.limits))
    }

    static func queryLimits(_ limits: StorageFrameLimits) throws -> DatabaseWireLimits {
        try DatabaseWireLimits(maximumFrameBytes: limits.maximumFrameBytes,
            maximumStringBytes: limits.maximumStringBytes,
            maximumByteStringBytes: limits.maximumByteStringBytes,
            maximumCollectionCount: limits.maximumCollectionCount,
            maximumNestingDepth: min(limits.maximumNestingDepth, DatabaseWireLimits.maximumSupportedNestingDepth),
            maximumObjectCount: limits.maximumCollectionCount)
    }

    private static func writeIDs(_ ids: [String],
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writer.writeCount(ids.count)
        for id in ids { try writer.writeString(id) }
    }

    private static func readIDs(from reader: inout StorageFrameDecoder) throws -> [String] {
        let count = try count(from: &reader, minimumBytes: 4)
        var ids: [String] = []
        ids.reserveCapacity(count)
        for _ in 0..<count { ids.append(try reader.readString()) }
        return ids
    }

    private static func writeAction(_ action: SQLReferentialAction,
        into writer: inout StorageFrameEncoder
    ) {
        switch action {
        case .noAction: writer.writeUInt8(1)
        case .restrict: writer.writeUInt8(2)
        case .cascade: writer.writeUInt8(3)
        case .setNull: writer.writeUInt8(4)
        case .setDefault: writer.writeUInt8(5)
        }
    }

    private static func readAction(from reader: inout StorageFrameDecoder) throws -> SQLReferentialAction {
        let tag = try reader.readUInt8()
        switch tag {
        case 1: return .noAction
        case 2: return .restrict
        case 3: return .cascade
        case 4: return .setNull
        case 5: return .setDefault
        default: throw StorageFrameError.invalidValueTag(tag)
        }
    }
}

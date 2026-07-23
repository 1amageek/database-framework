import DatabaseValue
import DatabaseWire
import StorageKit

internal enum ContinuationStateCodec {
    private static let magic: UInt32 = 0x4b4f_5443

    static func encode(_ state: ContinuationState) throws -> Bytes {
        let encoded = try DatabaseWireWriter.encodeThrowing { writer in
            writer.writeUInt32(magic)
            writer.writeUInt8(state.version)
            writer.writeUInt8(state.scanType.rawValue)
            try writer.writeBytes(DatabaseBytes(retaining: state.lastKey))
            writer.writeBool(state.reverse)
            try writeOptionalInt(state.remainingLimit, into: &writer)
            try writeOptionalInt(state.originalLimit, into: &writer)
            try writer.writeBytes(
                DatabaseBytes(retaining: state.planFingerprint)
            )
            writer.writeBool(state.operatorState != nil)
            if let operatorState = state.operatorState {
                try encode(operatorState, into: &writer)
            }
        }
        return Bytes(retaining: encoded)
    }

    static func decode(_ bytes: Bytes) throws -> ContinuationState {
        var reader = DatabaseWireReader(DatabaseBytes(retaining: bytes))
        guard try reader.readUInt32() == magic else {
            throw ContinuationError.corruptedToken
        }
        let version = try reader.readUInt8()
        guard version == ContinuationToken.currentVersion else {
            throw ContinuationError.versionMismatch(
                expected: ContinuationToken.currentVersion,
                actual: version
            )
        }
        let scanRawValue = try reader.readUInt8()
        guard let scanType = ContinuationState.ScanType(
            rawValue: scanRawValue
        ) else {
            throw ContinuationError.corruptedToken
        }
        let lastKey = Bytes(retaining: try reader.readBytes())
        let reverse = try reader.readBool()
        let remainingLimit = try readOptionalInt(from: &reader)
        let originalLimit = try readOptionalInt(from: &reader)
        let planFingerprint = Bytes(retaining: try reader.readBytes())
        let operatorState = try reader.readBool()
            ? try decodeOperatorState(from: &reader)
            : nil
        try reader.ensureFullyRead()
        return ContinuationState(
            version: version,
            scanType: scanType,
            lastKey: lastKey,
            reverse: reverse,
            remainingLimit: remainingLimit,
            originalLimit: originalLimit,
            planFingerprint: planFingerprint,
            operatorState: operatorState
        )
    }

    private static func encode(
        _ state: OperatorContinuationState,
        into writer: inout DatabaseWireWriter
    ) throws {
        try writeOptionalInt(state.unionChildIndex, into: &writer)
        writer.writeBool(state.childContinuation != nil)
        if let continuation = state.childContinuation {
            try writer.writeBytes(DatabaseBytes(retaining: continuation))
        }
        writer.writeBool(state.exhaustedChildren != nil)
        if let exhaustedChildren = state.exhaustedChildren {
            try writer.writeCount(exhaustedChildren.count)
            for index in exhaustedChildren {
                writer.writeInt64(try int64(index))
            }
        }
        writer.writeBool(state.intersectionIds != nil)
        if let intersectionIds = state.intersectionIds {
            try writer.writeCount(intersectionIds.count)
            for identifier in intersectionIds {
                try writer.writeBytes(DatabaseBytes(retaining: identifier))
            }
        }
    }

    private static func decodeOperatorState(
        from reader: inout DatabaseWireReader
    ) throws -> OperatorContinuationState {
        let unionChildIndex = try readOptionalInt(from: &reader)
        let childContinuation = try reader.readBool()
            ? Bytes(retaining: try reader.readBytes())
            : nil
        let exhaustedChildren: [Int]?
        if try reader.readBool() {
            let count = try reader.readCount()
            var values: [Int] = []
            values.reserveCapacity(count)
            for _ in 0..<count {
                values.append(try int(try reader.readInt64()))
            }
            exhaustedChildren = values
        } else {
            exhaustedChildren = nil
        }
        let intersectionIds: [Bytes]?
        if try reader.readBool() {
            let count = try reader.readCount()
            var values: [Bytes] = []
            values.reserveCapacity(count)
            for _ in 0..<count {
                values.append(Bytes(retaining: try reader.readBytes()))
            }
            intersectionIds = values
        } else {
            intersectionIds = nil
        }
        return OperatorContinuationState(
            unionChildIndex: unionChildIndex,
            childContinuation: childContinuation,
            exhaustedChildren: exhaustedChildren,
            intersectionIds: intersectionIds
        )
    }

    private static func writeOptionalInt(
        _ value: Int?,
        into writer: inout DatabaseWireWriter
    ) throws {
        writer.writeBool(value != nil)
        if let value {
            writer.writeInt64(try int64(value))
        }
    }

    private static func readOptionalInt(
        from reader: inout DatabaseWireReader
    ) throws -> Int? {
        guard try reader.readBool() else {
            return nil
        }
        return try int(try reader.readInt64())
    }

    private static func int64(_ value: Int) throws -> Int64 {
        guard value >= 0, let result = Int64(exactly: value) else {
            throw ContinuationError.corruptedToken
        }
        return result
    }

    private static func int(_ value: Int64) throws -> Int {
        guard value >= 0, let result = Int(exactly: value) else {
            throw ContinuationError.corruptedToken
        }
        return result
    }
}

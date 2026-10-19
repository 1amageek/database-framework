import DatabaseKit
import DatabaseTypes

/// Flat selected equality bytes, with a distinct retained typed ordering source.
struct CanonicalSQLCollectionIdentity: Sendable, Hashable {
    let tokens: ByteString
    let digest: UInt64
    let source: QueryValue
    let type: QueryColumnType
    private let reservation: DatabaseIntermediateReservation

    static func prepare(_ value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter, forcedDigest: UInt64? = nil) throws -> Self {
        try CanonicalSQLValueSemantics.requireComparable(type, operation: "identity", workMeter: workMeter)
        let sourceFootprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
            workMeter: workMeter, stage: .expressionEvaluation)
        let reservation = try workMeter.reserveIntermediate(bytes: sourceFootprint.bytes,
            at: .expressionEvaluation)
        var success = false
        defer { if !success { reservation.release() } }
        var measure = Writer(destination: nil, workMeter: workMeter)
        try emit(value, type: type, environment: environment, writer: &measure)
        try reservation.reserveAdditional(bytes: UInt64(measure.count) + UInt64(MemoryLayout<Self>.stride),
            at: .expressionEvaluation)
        var digest: UInt64 = 0
        let tokens = try ByteString.copying(count: measure.count) { destination in
            var writer = Writer(destination: destination, workMeter: workMeter)
            try emit(value, type: type, environment: environment, writer: &writer)
            precondition(writer.count == destination.count)
            digest = writer.digest
        }
        success = true
        return Self(tokens: tokens, digest: forcedDigest ?? digest, source: value, type: type, reservation: reservation)
    }

    // Standard Hashable is retained for the enclosing enum's scalar ABI only.
    // Production rich-key equality is invoked through the throwing metered path.
    static func == (left: Self, right: Self) -> Bool {
        left.digest == right.digest && left.tokens == right.tokens
    }
    func hash(into hasher: inout Hasher) { hasher.combine(digest) }

    func equal(to other: Self, workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage) throws -> Bool {
        try workMeter.consume(at: stage)
        guard digest == other.digest, tokens.count == other.tokens.count else { return false }
        for index in tokens.indices {
            try workMeter.consume(at: stage)
            if tokens[index] != other.tokens[index] { return false }
        }
        return true
    }

    private struct Cell {
        let value: QueryValue
        let type: QueryColumnType
    }

    private static func emit(_ value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment?, writer: inout Writer) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Cell.self)
        let reservation = try writer.workMeter.reserveIntermediate(bytes: layout.containerByteCount,
            at: .expressionEvaluation)
        defer { reservation.release() }
        var pending: [Cell] = []
        var admittedPending = 0
        func append(_ value: QueryValue, _ type: QueryColumnType) throws {
            let growth = try layout.growth(from: admittedPending, toFit: pending.count + 1)
            if growth.capacity > admittedPending {
                try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
                pending.reserveCapacity(growth.capacity)
                admittedPending = growth.capacity
            }
            pending.append(Cell(value: value, type: type))
        }
        try append(value, type)
        while let cell = pending.popLast() {
            try writer.workMeter.consume(at: .expressionEvaluation)
            if cell.value.isNull { try writer.byte(0); continue }
            switch (cell.type, cell.value) {
            case (.sql(.array(let element)), .array(let array)):
                try writer.byte(1)
                try writer.integer(UInt64(array.dimensions.count))
                for dimension in array.dimensions {
                    try writer.integer(dimension.length); try writer.integer(dimension.lowerBound)
                }
                try writer.integer(UInt64(array.elements.count))
                for index in array.elements.indices.reversed() { try append(array.elements[index], .sql(element)) }
            case (.sql(.array(let element)), .primitive(.array(let array))):
                try writer.byte(1); try writer.integer(UInt64(array.isEmpty ? 0 : 1))
                if !array.isEmpty {
                    guard let length = Int32(exactly: array.count) else { throw DatabaseExpressionEvaluationError.collection(.invalidArrayShape) }
                    try writer.integer(length); try writer.integer(Int32(1))
                }
                try writer.integer(UInt64(array.count))
                for index in array.indices.reversed() { try append(.primitive(array[index]), .sql(element)) }
            case (.sql(.composite(let type)), .composite(let record)):
                guard record.fields.count == type.columns.count else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "composite identity arity")
                }
                try writer.byte(10); try writer.integer(UInt64(record.fields.count))
                for index in record.fields.indices.reversed() { try append(record.fields[index], type.columns[index].type) }
            case (.sql(.jsonb), .jsonb(let json)):
                try writer.byte(2); try jsonTokens(json.root, writer: &writer)
            default: try scalar(cell.value, type: cell.type, environment: environment, writer: &writer)
            }
        }
    }

    private static func scalar(_ value: QueryValue, type: QueryColumnType,
        environment: SQLTemporalEnvironment?, writer: inout Writer) throws {
        try writer.workMeter.consume(at: .expressionEvaluation)
        if value.isNull { try writer.byte(0); return }
        if case .jsonb(let json) = value {
            try writer.byte(2)
            try jsonTokens(json.root, writer: &writer)
            return
        }
        if let temporal = try CanonicalSQLValueSemantics.temporalIdentity(value, type: type,
            environment: environment, workMeter: writer.workMeter) {
            try writer.byte(3)
            try writer.byte(temporal.family.rawValue)
            try writer.integer(temporal.infinityRank)
            try writer.integer(temporal.scalar)
            try writer.integer(temporal.offsetTie)
            return
        }
        let primitive: FieldValue
        switch value {
        case .numeric(let number): primitive = .decimal(number.value)
        case .primitive(let value): primitive = value
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected array identity cell")
        }
        // Character trimming is borrowed here; no temporary String is materialized.
        if case .string(let text) = primitive {
            var view = text.utf8[...]
            if case .sql(.char) = type {
                while view.last == 32 { try writer.workMeter.consume(at: .expressionEvaluation); view = view.dropLast() }
            }
            try writer.byte(4)
            try writer.sequence(view)
            return
        }
        let normalized = try RelationalValueIdentity.canonicalize(primitive).value
        switch normalized {
        case .int64(let value): try decimal(ExactDecimal(coefficient: Int128(value), scale: 0), writer: &writer)
        case .uint64(let value): try decimal(ExactDecimal(coefficient: Int128(value), scale: 0), writer: &writer)
        case .decimal(let value): try decimal(value, writer: &writer)
        case .float64(let value): try writer.byte(6); try writer.integer(value.bitPattern)
        case .bool(let value): try writer.byte(7); try writer.byte(value ? 1 : 0)
        case .bytes(let bytes): try writer.byte(8); try writer.sequence(bytes)
        case .uuid(let value): try writer.byte(9); try writer.sequence(value)
        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected array identity cell")
        }
    }

    private static func decimal(_ value: ExactDecimal, writer: inout Writer) throws {
        try writer.byte(5)
        try writer.integer(value.coefficient)
        try writer.integer(value.scale)
    }

    private enum Visit: Sendable {
        case node(QueryJSONBNode)
        case array(QueryJSONBElements, Int)
        case object(QueryJSONBMembers, Int)
    }

    private static func jsonTokens(_ root: QueryJSONBNode, writer: inout Writer) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Visit.self)
        let reservation = try writer.workMeter.reserveIntermediate(bytes: layout.containerByteCount,
            at: .expressionEvaluation)
        defer { reservation.release() }
        var stack: [Visit] = []
        var capacity = 0
        func append(_ visit: Visit) throws {
            let growth = try layout.growth(from: capacity, toFit: stack.count + 1)
            if growth.capacity > capacity {
                try reservation.reserveAdditional(bytes: growth.additionalByteCount, at: .expressionEvaluation)
                stack.reserveCapacity(growth.capacity)
                capacity = growth.capacity
            }
            stack.append(visit)
        }
        try append(.node(root))
        while let visit = stack.popLast() {
            try writer.workMeter.consume(at: .expressionEvaluation)
            switch visit {
            case .node(let node):
                switch node {
                case .null: try writer.byte(0)
                case .bool(let value): try writer.byte(1); try writer.byte(value ? 1 : 0)
                case .string(let value): try writer.byte(2); try writer.sequence(value.utf8)
                case .number(let value): try writer.byte(3); try decimal(value.value, writer: &writer)
                case .array(let values):
                    try writer.byte(4); try writer.integer(UInt64(values.count))
                    if !values.isEmpty { try append(.array(values, 0)) }
                case .object(let values):
                    try writer.byte(5); try writer.integer(UInt64(values.count))
                    if !values.isEmpty { try append(.object(values, 0)) }
                }
            case .array(let values, let index):
                if index + 1 < values.count { try append(.array(values, index + 1)) }
                try append(.node(values[index]))
            case .object(let values, let index):
                let member = values[index]
                try writer.sequence(member.key.utf8)
                if index + 1 < values.count { try append(.object(values, index + 1)) }
                try append(.node(member.value))
            }
        }
    }

    /// The optional destination is borrowed synchronously by the producer only.
    private struct Writer {
        let destination: UnsafeMutableRawBufferPointer?
        let workMeter: DatabaseWorkMeter
        var count = 0
        var digest: UInt64 = 14_695_981_039_346_656_037

        mutating func byte(_ value: UInt8) throws {
            try workMeter.consume(at: .expressionEvaluation)
            let next = count.addingReportingOverflow(1)
            guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            if let destination {
                precondition(count < destination.count)
                destination[count] = value
                digest = (digest ^ UInt64(value)) &* 1_099_511_628_211
            }
            count = next.partialValue
        }
        mutating func integer<Value: FixedWidthInteger>(_ value: Value) throws {
            for index in (0..<MemoryLayout<Value>.size).reversed() {
                try byte(UInt8(truncatingIfNeeded: value >> (index * 8)))
            }
        }
        mutating func sequence<Bytes: Collection>(_ bytes: Bytes) throws where Bytes.Element == UInt8 {
            try workMeter.consume(UInt64(bytes.count), at: .expressionEvaluation)
            try integer(UInt64(bytes.count))
            for byte in bytes { try self.byte(byte) }
        }
    }
}

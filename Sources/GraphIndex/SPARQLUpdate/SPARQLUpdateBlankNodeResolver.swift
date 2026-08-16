import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseTypes

@_spi(DatabaseExecution)
public struct SPARQLUpdateBlankNodeResolver: Sendable {
    public let idempotencyKey: String
    public let operationOrdinal: UInt64
    public let solutionOrdinal: UInt64

    public init(
        idempotencyKey: String,
        operationOrdinal: UInt64,
        solutionOrdinal: UInt64
    ) {
        self.idempotencyKey = idempotencyKey
        self.operationOrdinal = operationOrdinal
        self.solutionOrdinal = solutionOrdinal
    }

    public func identifier(for label: String) -> String {
        var accumulator = SHA256Accumulator()
        accumulator.update([0x53, 0x50, 0x41, 0x52, 0x51, 0x4c, 0x55, 0x01])
        Self.updateFramed(idempotencyKey, accumulator: &accumulator)
        Self.update(operationOrdinal, accumulator: &accumulator)
        Self.update(solutionOrdinal, accumulator: &accumulator)
        Self.updateFramed(label, accumulator: &accumulator)
        return "u" + DatabaseTextFormatting.lowercaseHex(
            accumulator.finalize()
        )
    }

    private static func updateFramed(
        _ value: String,
        accumulator: inout SHA256Accumulator
    ) {
        update(UInt64(value.utf8.count), accumulator: &accumulator)
        accumulator.update(utf8: value)
    }

    private static func update(
        _ value: UInt64,
        accumulator: inout SHA256Accumulator
    ) {
        var encoded = value.bigEndian
        withUnsafeBytes(of: &encoded) {
            accumulator.update($0)
        }
    }
}

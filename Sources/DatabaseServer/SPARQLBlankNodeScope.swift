import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct SPARQLBlankNodeScope: Sendable {
    let idempotencyKey: String
    let operationOrdinal: UInt64
    let solutionOrdinal: UInt64

    func identifier(for label: String) -> String {
        var accumulator = DatabaseRequestDigestAccumulator(
            operation: .mutationExecute
        )
        accumulator.update([0x42, 0x4e, 0x02])
        accumulator.update(utf8: idempotencyKey)
        accumulator.update(bigEndian: operationOrdinal)
        accumulator.update(bigEndian: solutionOrdinal)
        accumulator.update(utf8: label)
        return "u" + DatabaseTextFormatting.lowercaseHex(
            accumulator.finalize()
        )
    }
}

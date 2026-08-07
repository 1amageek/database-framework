#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct SPARQLUpdateBlankNodeResolver: Sendable {
    let idempotencyKey: String
    let operationOrdinal: UInt64
    let solutionOrdinal: UInt64

    func identifier(for label: String) -> String {
        var accumulator = DatabaseRequestDigestAccumulator(
            operation: .mutationExecute
        )
        accumulator.update([0x42, 0x4e, 0x02])
        Self.updateFramed(idempotencyKey, accumulator: &accumulator)
        accumulator.update(bigEndian: operationOrdinal)
        accumulator.update(bigEndian: solutionOrdinal)
        Self.updateFramed(label, accumulator: &accumulator)
        return "u" + DatabaseTextFormatting.lowercaseHex(
            accumulator.finalize()
        )
    }

    private static func updateFramed(
        _ value: String,
        accumulator: inout DatabaseRequestDigestAccumulator
    ) {
        accumulator.update(bigEndian: UInt64(value.utf8.count))
        accumulator.update(utf8: value)
    }
}

#endif

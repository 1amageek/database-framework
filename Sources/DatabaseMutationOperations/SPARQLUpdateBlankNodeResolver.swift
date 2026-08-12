import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_MUTATION_OPERATIONS_GRAPH_INDEXES
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct SPARQLUpdateBlankNodeResolver: Sendable {
    package let idempotencyKey: String
    package let operationOrdinal: UInt64
    package let solutionOrdinal: UInt64

    package init(
        idempotencyKey: String,
        operationOrdinal: UInt64,
        solutionOrdinal: UInt64
    ) {
        self.idempotencyKey = idempotencyKey
        self.operationOrdinal = operationOrdinal
        self.solutionOrdinal = solutionOrdinal
    }

    package func identifier(for label: String) -> String {
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

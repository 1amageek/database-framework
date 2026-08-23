import DatabaseKit
import DatabaseTypes
import StorageKit

/// Raw storage reads bound to one caller-owned transaction.
package struct TransactionStorageReader: StorageReader {
    private let transaction: any TransactionReadAccess

    package init(transaction: any TransactionReadAccess) {
        self.transaction = transaction
    }

    package func rangeCursor(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) throws -> KeyValueCursor {
        let beginKey: ByteString
        if let start {
            let packed = subspace.pack(start)
            beginKey = startInclusive
                ? packed
                : try strinc(packed)
        } else {
            beginKey = subspace.prefix
        }

        let endKey: ByteString
        if let end {
            let packed = subspace.pack(end)
            endKey = endInclusive
                ? try strinc(packed)
                : packed
        } else {
            endKey = subspace.range().end
        }

        return transaction.rangeCursor(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: reverse,
            snapshot: true,
            streamingMode: .iterator
        )
    }

    package func getValue(key: ByteString) async throws -> ByteString? {
        try await transaction.getValue(for: key, snapshot: true)
    }
}

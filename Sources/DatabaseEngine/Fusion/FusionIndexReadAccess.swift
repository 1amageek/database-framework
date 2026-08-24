import DatabaseTypes
import StorageKit

/// Revocable read capability for one schema-admitted index subspace.
///
/// This deliberately does not refine `TransactionReadAccess`: feature modules
/// cannot select arbitrary keys, obtain transaction lifecycle authority, or
/// escape the admitted index range.
package protocol FusionIndexReadAccess: Sendable {
    var index: ReadableIndex { get }

    func getValue(key: ByteString) async throws -> FusionIndexReadValue?

    /// Opens one cursor over exact packed bounds inside the admitted index.
    ///
    /// Feature-owned physical planners may already have canonical packed
    /// bounds. The session still validates both bounds against the admitted
    /// index root before opening the backend cursor.
    func rangeCursor(
        from beginKey: ByteString,
        to endKey: ByteString,
        reverse: Bool
    ) throws -> FusionIndexReadCursor
}

extension FusionIndexReadAccess {
    package func rangeCursor(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) throws -> FusionIndexReadCursor {
        guard index.subspace.contains(subspace.prefix) else {
            throw FusionExecutionContractError
                .indexReadOutsideAdmittedSubspace(
                    index: index.descriptor.name
                )
        }
        let beginKey: ByteString
        if let start {
            let packed = subspace.pack(start)
            beginKey = startInclusive ? packed : try strinc(packed)
        } else {
            beginKey = subspace.prefix
        }
        let endKey: ByteString
        if let end {
            let packed = subspace.pack(end)
            endKey = endInclusive ? try strinc(packed) : packed
        } else {
            endKey = try subspace.prefixRange().end
        }
        return try rangeCursor(
            from: beginKey,
            to: endKey,
            reverse: reverse
        )
    }

    package func subspaceCursor(
        _ subspace: Subspace,
        reverse: Bool
    ) throws -> FusionIndexReadCursor {
        try rangeCursor(
            subspace: subspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: reverse
        )
    }
}

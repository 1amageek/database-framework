import DatabaseKit
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

package func openFusionSubspaceCursor(
    using access: any FusionIndexReadAccess,
    in subspace: Subspace,
    reverse: Bool
) throws -> FusionIndexReadCursor {
    guard access.index.subspace.contains(subspace.prefix) else {
        throw FusionExecutionContractError
            .indexReadOutsideAdmittedSubspace(
                index: access.index.descriptor.name
            )
    }
    return try access.rangeCursor(
        from: subspace.prefix,
        to: subspace.prefixRange().end,
        reverse: reverse
    )
}

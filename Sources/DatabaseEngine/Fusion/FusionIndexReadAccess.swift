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

    func rangeCursor(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) throws -> FusionIndexReadCursor
}

extension FusionIndexReadAccess {
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

import DatabaseTypes
import DatabaseEngine
import StorageKit

/// A decoded posting identifier paired with its canonical ordering key and
/// exact intermediate-memory claim.
///
/// The packed suffix must be self-contained before this value is created. The
/// scan owner admits and detaches a cursor-backed suffix, then uses this value
/// to keep the decoded tuple, canonical comparison representation, and
/// accounting metadata together.
struct FullTextPostingCandidate: Sendable, Equatable {
    let identifier: Tuple
    let canonicalKey: ByteString
    let retainedFootprint: DatabaseIntermediateFootprint

    init(
        packedSuffix: ByteString,
        retainedFootprint: DatabaseIntermediateFootprint = .init(),
        admitting admit: @escaping (Int) throws -> Void
    ) throws {
        precondition(
            packedSuffix.isStorageSelfContained,
            "Posting candidates must own a self-contained packed suffix"
        )

        var admittedFootprint = retainedFootprint
        let identifier = try Tuple(
            packed: packedSuffix,
            admitting: { allocation in
                let bytes = UInt64(allocation)
                let nextFootprint = try admittedFootprint.adding(
                    DatabaseIntermediateFootprint(bytes: bytes)
                )
                try admit(allocation)
                admittedFootprint = nextFootprint
            }
        )

        let canonicalKey = try identifier.pack(
            admitting: { allocation in
                let bytes = UInt64(allocation)
                let nextFootprint = try admittedFootprint.adding(
                    DatabaseIntermediateFootprint(bytes: bytes)
                )
                try admit(allocation)
                admittedFootprint = nextFootprint
            }
        )

        self.identifier = identifier
        self.canonicalKey = canonicalKey
        self.retainedFootprint = admittedFootprint
    }

    static func == (
        lhs: borrowing FullTextPostingCandidate,
        rhs: borrowing FullTextPostingCandidate
    ) -> Bool {
        lhs.canonicalKey == rhs.canonicalKey
    }
}

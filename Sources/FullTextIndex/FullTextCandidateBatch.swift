import DatabaseEngine
import StorageKit

/// A posting-list result whose backing collection remains charged until the
/// next retained owner is constructed. The batch owns one reservation for its
/// candidate collection; candidates do not carry independent reservations.
struct FullTextCandidateBatch: Sendable {
    private(set) var values: [FullTextPostingCandidate]
    let reservation: DatabaseIntermediateReservation

    init(workMeter: DatabaseWorkMeter) throws {
        self.values = []
        self.reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[FullTextPostingCandidate]>.stride),
            at: .indexScan
        )
    }

    init(
        values: consuming [FullTextPostingCandidate],
        reservation: DatabaseIntermediateReservation
    ) {
        self.values = values
        self.reservation = reservation
    }

    var count: Int { values.count }

    /// Admits the candidate and its decoded tuple before preserving it.
    ///
    /// The caller supplies a packed suffix borrowed from a cursor. The suffix
    /// owner is detached only after the batch has admitted its complete base
    /// footprint; tuple decoder allocations are admitted synchronously through
    /// the callback before each allocation is created.
    mutating func append(scannedSuffix: ByteString) throws {
        let (suffixBytes, suffixOverflow) = UInt64(scannedSuffix.count)
            .addingReportingOverflow(96)
        guard !suffixOverflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: UInt64(scannedSuffix.count),
                right: 96
            )
        }
        let (baseBytes, baseOverflow) = UInt64(
            MemoryLayout<FullTextPostingCandidate>.stride
        ).addingReportingOverflow(suffixBytes)
        guard !baseOverflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: UInt64(MemoryLayout<FullTextPostingCandidate>.stride),
                right: suffixBytes
            )
        }
        try reservation.reserveAdditional(
            rows: 1,
            bytes: baseBytes,
            at: .indexScan
        )
        let batchReservation = reservation
        var claimedBytes = baseBytes
        do {
            let ownedSuffix = scannedSuffix.detached()
            let candidate = try FullTextPostingCandidate(
                packedSuffix: ownedSuffix,
                retainedFootprint: DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: baseBytes
                ),
                admitting: { allocation in
                    let bytes = UInt64(allocation)
                    let (nextClaimedBytes, overflow) = claimedBytes
                        .addingReportingOverflow(bytes)
                    guard !overflow else {
                        throw DatabaseIntermediateFootprintError
                            .byteAdditionOverflow(
                                left: claimedBytes,
                                right: bytes
                            )
                    }
                    try batchReservation.reserveAdditional(
                        bytes: bytes,
                        at: .indexScan
                    )
                    claimedBytes = nextClaimedBytes
                }
            )
            values.append(candidate)
        } catch {
            batchReservation.releaseGuaranteedPartial(
                rows: 1,
                bytes: claimedBytes
            )
            throw error
        }
    }

    func release() {
        reservation.release()
    }
}

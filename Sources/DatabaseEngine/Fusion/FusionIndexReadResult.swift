/// Validated matches retained under the request's intermediate-memory budget.
struct FusionIndexReadResult: Sendable {
    let matches: [FusionIndexMatch]
    let coverage: FusionInputCoverage

    // Copies share this exactly-once reservation owner after the sink has
    // irrevocably transferred ownership.
    private let reservation: DatabaseIntermediateReservation

    init(
        matches: [FusionIndexMatch],
        coverage: FusionInputCoverage,
        reservation: DatabaseIntermediateReservation
    ) {
        self.matches = matches
        self.coverage = coverage
        self.reservation = reservation
    }
}

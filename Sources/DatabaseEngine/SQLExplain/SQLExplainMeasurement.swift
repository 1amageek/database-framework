/// Statement-wide observations; operator-level counters are never inferred.
struct SQLExplainMeasurement: Sendable {
    let rows: UInt64
    let workUnits: UInt64
    let elapsed: Duration
}

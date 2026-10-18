import DatabaseKit

/// Retains selected argument conversion storage through ordinary execution.
struct SQLPreparedInvocation: Sendable {
    let statement: QueryStatement
    let parameters: [QueryParameter]
    let payloadLeases: [DatabaseQueryPayloadLease]
    let reservation: DatabaseIntermediateReservation
}

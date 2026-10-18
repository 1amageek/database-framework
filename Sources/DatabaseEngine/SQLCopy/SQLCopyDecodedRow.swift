/// A single decoded row and its claims, retained through the mutation await.
struct SQLCopyDecodedRow: ~Copyable {
    let fields: [String?]
    let reservation: DatabaseIntermediateReservation
}

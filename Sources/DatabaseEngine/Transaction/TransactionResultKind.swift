/// Whether a transaction execution produces a read result or a write result.
///
/// The distinction decides one thing: whether cancellation is checked again
/// after the attempt closes. A read result must not reach a cancelled caller,
/// so it is checked. A write result must never be reported as failed once its
/// commit is authoritative, so it is not.
///
/// Every caller states its kind explicitly. A default would let a new
/// execution path acquire the read contract, or lose it, without deciding.
enum TransactionResultKind: Sendable, Equatable {
    /// The execution returns data read inside the transaction.
    ///
    /// The transaction must be admitted through an access that refuses
    /// persistent mutation, so no durable outcome exists to be lost when the
    /// closed attempt reports cancellation.
    case readResult

    /// The execution may commit durable state.
    case writeResult
}

/// Resource admission for transactional IVF and PQ training.
///
/// The default budget remains below FoundationDB's transaction mutation limit
/// and leaves headroom under Cloudflare's 128 MiB isolate memory limit, which
/// includes WebAssembly allocations.
public struct VectorTrainingResourceLimits: Sendable, Hashable {
    public static let `default` = VectorTrainingResourceLimits(
        maximumVectorCount: 2_048,
        maximumVectorPayloadByteCount: 2 * 1_024 * 1_024,
        maximumWorkingByteCount: 20 * 1_024 * 1_024,
        maximumTransactionMutationByteCount: 6 * 1_024 * 1_024
    )

    public let maximumVectorCount: Int
    public let maximumVectorPayloadByteCount: Int
    public let maximumWorkingByteCount: Int
    public let maximumTransactionMutationByteCount: Int

    public init(
        maximumVectorCount: Int,
        maximumVectorPayloadByteCount: Int,
        maximumWorkingByteCount: Int,
        maximumTransactionMutationByteCount: Int
    ) {
        self.maximumVectorCount = maximumVectorCount
        self.maximumVectorPayloadByteCount = maximumVectorPayloadByteCount
        self.maximumWorkingByteCount = maximumWorkingByteCount
        self.maximumTransactionMutationByteCount = maximumTransactionMutationByteCount
    }

    func validate() throws(VectorIndexError) {
        guard maximumVectorCount > 0,
              maximumVectorPayloadByteCount > 0,
              maximumWorkingByteCount > 0,
              maximumTransactionMutationByteCount > 0 else {
            throw .invalidArgument(
                "Vector training resource limits must be positive"
            )
        }
    }
}

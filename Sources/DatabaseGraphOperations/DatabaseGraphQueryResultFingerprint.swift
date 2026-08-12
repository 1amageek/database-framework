import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseKit
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseEngine
import DatabaseTypes

package enum DatabaseGraphQueryResultFingerprint {
    private static let prefix: ByteString = [0x47, 0x51, 0x01]

    package static func compute(
        graph: borrowing DatabaseRetainedRDFGraph,
        wireLimits: DatabaseWireLimits,
        workMeter: DatabaseWorkMeter
    ) throws -> ByteString {
        var hasher = SHA256Accumulator()
        hasher.update(Self.prefix)
        for index in 0..<graph.count {
            try graph.withElement(at: index) { triple in
                try DatabaseRuntimePayloadEncoder.emit(
                    triple,
                    limits: wireLimits,
                    prepare: { byteCount in
                        try workMeter.consume(
                            UInt64(byteCount),
                            at: .resultMaterialization
                        )
                        var count = UInt64(byteCount).bigEndian
                        withUnsafeBytes(of: &count) {
                            hasher.update($0)
                        }
                    },
                    consume: { bytes in
                        hasher.update(bytes)
                    }
                )
            }
        }
        return hasher.finalize()
    }
}

#endif

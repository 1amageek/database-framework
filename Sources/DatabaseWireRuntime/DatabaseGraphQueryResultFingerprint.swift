#if DATABASE_WIRE_RUNTIME_GRAPH_INDEXES
import DatabaseKit
@_spi(DatabaseWireRuntime) import DatabaseWire
import DatabaseEngine
import DatabaseTypes

enum DatabaseGraphQueryResultFingerprint {
    private static let prefix: ByteString = [0x47, 0x51, 0x01]

    static func compute(
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

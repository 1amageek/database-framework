import DatabaseDigest
import DatabaseEngine
import DatabaseValue
import DatabaseWire

enum DatabaseGraphQueryResultFingerprint {
    private static let prefix: DatabaseBytes = [0x47, 0x51, 0x01]

    static func compute(
        graph: borrowing DatabaseRetainedRDFGraph,
        wireLimits: DatabaseWireLimits,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseBytes {
        var hasher = SHA256Accumulator()
        hasher.update(Self.prefix)
        for index in 0..<graph.count {
            try graph.withElement(at: index) { triple in
                try DatabaseWireWriter.emit(
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
                ) {
                    (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                    try triple.encode(into: &writer)
                }
            }
        }
        return hasher.finalize()
    }
}

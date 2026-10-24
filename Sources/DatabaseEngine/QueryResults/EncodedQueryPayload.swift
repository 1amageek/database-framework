import DatabaseTypes

/// Private frame allocation retained through authoritative request closure.
struct EncodedQueryPayload: Sendable {
    let bytes: ByteString
    let claim: DatabaseIntermediateReservation

    static func encode(_ output: borrowing SQLBlockOutputView,
        encoder: any SQLBlockResponseEncoder, workMeter: DatabaseWorkMeter) throws -> Self {
        try prepare(workMeter: workMeter, measure: { try encoder.measure(output) },
            encode: { destination in try encoder.encode(output, into: &destination) })
    }

    static func prepare(workMeter: DatabaseWorkMeter,
        measure: () throws -> UInt64,
        encode: (inout MutableSpan<UInt8>) throws -> Void) throws -> Self {
        try workMeter.checkpoint(at: .resultMaterialization)
        let measured = try measure()
        guard measured > 0, let count = Int(exactly: measured) else {
            throw SQLBlockProjectionError.invalidMeasuredSize(measured)
        }
        try DatabaseByteProcessingMeter.consume(byteCount: measured, workMeter: workMeter, stage: .resultMaterialization)
        let claim = try workMeter.reserveIntermediate(bytes: measured, at: .resultMaterialization)
        // ByteString owns and releases the exact allocation. Initialize its full UInt8
        // region before lending a synchronous span; no pointer escapes this borrow.
        let bytes = try ByteString.copying(count: count) { destination in
            let initialized = destination.initializeMemory(as: UInt8.self, repeating: 0)
            var span = initialized.mutableSpan
            try encode(&span)
        }
        try workMeter.checkpoint(at: .resultMaterialization)
        return Self(bytes: bytes, claim: claim)
    }
}

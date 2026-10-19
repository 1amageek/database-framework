import DatabaseTypes

/// Private frame allocation retained through authoritative request closure.
struct SQLEncodedPayload: Sendable {
    let bytes: ByteString
    let claim: DatabaseIntermediateReservation

    static func encode(_ output: borrowing SQLBlockOutputView,
        encoder: any SQLBlockResponseEncoder, workMeter: DatabaseWorkMeter) throws -> Self {
        try workMeter.checkpoint(at: .resultMaterialization)
        let measured = try encoder.measure(output)
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
            try encoder.encode(output, into: &span)
        }
        try workMeter.checkpoint(at: .resultMaterialization)
        return Self(bytes: bytes, claim: claim)
    }
}

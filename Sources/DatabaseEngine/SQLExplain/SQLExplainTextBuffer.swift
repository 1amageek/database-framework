/// Metered rendering storage retained until the result row owns its payload.
struct SQLExplainTextBuffer {
    private(set) var text = ""
    let reservation: DatabaseIntermediateReservation
    let meter: DatabaseWorkMeter

    init(meter: DatabaseWorkMeter) throws {
        self.meter = meter
        self.reservation = try meter.reserveIntermediate(bytes: 64, at: .resultMaterialization)
    }

    mutating func append(_ value: String) throws {
        try meter.checkpoint(at: .resultMaterialization)
        // Cover String growth, UTF representation, and the old/new allocation overlap.
        let footprint = try DatabaseIntermediateFootprint(bytes: UInt64(value.utf8.count)).multiplied(by: 8)
        try reservation.reserveAdditional(bytes: footprint.bytes, at: .resultMaterialization)
        try DatabaseByteProcessingMeter.consume(byteCount: UInt64(value.utf8.count), workMeter: meter,
            stage: .resultMaterialization)
        text.append(value)
    }

    mutating func quoted(_ value: String) throws {
        try append("\"")
        for scalar in value.unicodeScalars {
            switch scalar.value {
            case 0x22: try append("\\\"")
            case 0x5c: try append("\\\\")
            case 0x00...0x1f:
                let hexadecimal = String(scalar.value, radix: 16)
                try append("\\u" + String(repeating: "0", count: 4 - hexadecimal.count) + hexadecimal)
            default: try append(String(scalar))
            }
        }
        try append("\"")
    }
}

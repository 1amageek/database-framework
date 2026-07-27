import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct PersistentJobValue: ServerPayloadValue {
    let value: FieldValue

    func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try value.encode(into: &writer)
    }

    init(value: FieldValue) {
        self.value = value
    }

    init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        value = try FieldValue(from: &reader)
    }
}

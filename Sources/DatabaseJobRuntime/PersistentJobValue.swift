import DatabaseOperationCore
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct PersistentJobValue: DatabaseRuntimePayloadValue {
    let value: FieldValue

    package func encode(
        into writer: inout DatabaseWireWriter
    ) throws(DatabaseWireError) {
        try value.encode(into: &writer)
    }

    package init(value: FieldValue) {
        self.value = value
    }

    package init(
        from reader: inout DatabaseWireReader
    ) throws(DatabaseWireError) {
        value = try FieldValue(from: &reader)
    }
}

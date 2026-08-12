import DatabaseJobRuntime
import DatabaseTypes

/// Base-local durable progress for a maintenance job. The control-domain job
/// state may lag this checkpoint after a process crash, so every retry derives
/// its next operation from this value before touching the backend again.
package struct DatabaseMaintenanceJobCheckpoint:
    PersistentJobPayload,
    Sendable,
    Hashable
{
    private static let formatVersion: UInt8 = 1

    package let state: DatabaseMaintenanceJobState
    package let cumulativeWorkUnits: UInt64
    package let isComplete: Bool

    package func persistentJobValue()
        throws(PersistentJobPayloadError) -> FieldValue
    {
        do {
            return .object(try FieldObject([
                (key: "version", value: .uint8(Self.formatVersion)),
                (key: "state", value: try state.persistentJobValue()),
                (
                    key: "cumulativeWorkUnits",
                    value: .uint64(cumulativeWorkUnits)
                ),
                (key: "isComplete", value: .bool(isComplete)),
            ]))
        } catch let error as PersistentJobPayloadError {
            throw error
        } catch {
            throw .invalidValue(
                "Maintenance checkpoint fields are not unique"
            )
        }
    }

    package init(
        persistentJobValue: FieldValue
    ) throws(PersistentJobPayloadError) {
        guard let fields = persistentJobValue.objectValue,
              fields.count == 4,
              fields["version"]?.uint8Value == Self.formatVersion,
              let stateValue = fields["state"],
              let cumulativeWorkUnits =
                fields["cumulativeWorkUnits"]?.uint64Value,
              let isComplete = fields["isComplete"]?.boolValue else {
            throw .invalidValue("Invalid maintenance checkpoint")
        }
        self.state = try DatabaseMaintenanceJobState(
            persistentJobValue: stateValue
        )
        self.cumulativeWorkUnits = cumulativeWorkUnits
        self.isComplete = isComplete
    }

    package init(
        state: DatabaseMaintenanceJobState,
        cumulativeWorkUnits: UInt64,
        isComplete: Bool
    ) {
        self.state = state
        self.cumulativeWorkUnits = cumulativeWorkUnits
        self.isComplete = isComplete
    }
}

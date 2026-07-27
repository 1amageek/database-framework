import DatabaseKit
import DatabaseTypes

package struct DatabaseIndexMaintenanceStatus: Sendable {
    package let entity: String
    package let index: String
    package let partitions: FieldObject
    package let indexState: IndexState
    package let rebuildState: DatabaseIndexRebuildState?
}

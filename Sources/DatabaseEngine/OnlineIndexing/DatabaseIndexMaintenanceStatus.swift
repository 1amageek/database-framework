import Core
import DatabaseValue

package struct DatabaseIndexMaintenanceStatus: Sendable {
    package let entity: String
    package let index: String
    package let partitions: [DatabaseObjectField]
    package let indexState: IndexState
    package let rebuildRecord: DatabaseIndexRebuildRecord?
}

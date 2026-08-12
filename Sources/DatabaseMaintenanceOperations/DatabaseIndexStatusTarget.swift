import DatabaseJobRuntime
import DatabaseTypes

package struct DatabaseIndexStatusTarget: Sendable, Hashable {
    package let entity: String
    package let index: String
    package let partitions: FieldObject
}

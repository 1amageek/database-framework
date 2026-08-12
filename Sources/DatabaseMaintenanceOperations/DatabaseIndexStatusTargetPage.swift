import DatabaseJobRuntime
import DatabaseTypes

package struct DatabaseIndexStatusTargetPage: Sendable, Hashable {
    package let targets: [DatabaseIndexStatusTarget]
    package let continuation: ByteString?
}

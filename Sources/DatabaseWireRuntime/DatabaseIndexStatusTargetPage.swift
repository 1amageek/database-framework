import DatabaseTypes

struct DatabaseIndexStatusTargetPage: Sendable, Hashable {
    let targets: [DatabaseIndexStatusTarget]
    let continuation: ByteString?
}

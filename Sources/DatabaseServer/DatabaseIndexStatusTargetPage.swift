import DatabaseValue

struct DatabaseIndexStatusTargetPage: Sendable, Hashable {
    let targets: [DatabaseIndexStatusTarget]
    let continuation: DatabaseBytes?
}

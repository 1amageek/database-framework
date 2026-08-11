import DatabaseTypes

struct DatabaseIndexStatusTarget: Sendable, Hashable {
    let entity: String
    let index: String
    let partitions: FieldObject
}

import DatabaseKit

@Persistable
public struct EmbeddedRuntimePerson {
    public var id: String = ""
    public var name: String = ""
    public var age: Int32 = 0
    public var isActive: Bool = false
}

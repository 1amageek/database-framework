import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationRelationshipOwner {
    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: PersistableReference<RuntimeConfigurationRelationshipTarget>? = nil
}

import Core
import DatabaseValue
import Relationship

@Persistable
struct RuntimeConfigurationRelationshipOwner {
    var id: String = ""

    @Relationship(deleteRule: .cascade)
    var target: DatabaseReference<RuntimeConfigurationRelationshipTarget>? = nil
}

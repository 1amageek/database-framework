import DatabaseKit

@Persistable
struct EntityMutationFixture: Equatable {
    #Directory<EntityMutationFixture>("entity_mutation_fixtures")

    var id: String = ""
    var title: String = ""
}

import DatabaseKit

@Persistable
struct RuntimeConfigurationPolymorphicVectorEntity:
    RuntimeConfigurationPolymorphicVectorDocument
{
    #Directory<RuntimeConfigurationPolymorphicVectorEntity>(
        "runtime_configuration_polymorphic_vector_entities"
    )

    var id: String = ""
    var embedding: Vector
}

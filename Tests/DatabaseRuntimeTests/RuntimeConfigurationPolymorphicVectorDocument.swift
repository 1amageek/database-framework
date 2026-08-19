import DatabaseKit

@Polymorphable(identifier: "RuntimeConfigurationPolymorphicVectorDocument")
@PolymorphicDirectory("runtime_configuration_polymorphic_vectors")
@PolymorphicIndex(
    .vector(
        name: "RuntimeConfigurationPolymorphicVectorDocument_embedding",
        embedding: "embedding",
        dimensions: 3, metric: .cosine
    ))
protocol RuntimeConfigurationPolymorphicVectorDocument:
    Polymorphable<RuntimeConfigurationPolymorphicVectorDocumentPolymorphicGroup>
{
    var id: String { get }
    var embedding: Vector { get }
}

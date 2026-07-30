import DatabaseKit

@Polymorphable(identifier: "RuntimeConfigurationPolymorphicVectorDocument")
@PolymorphicDirectory("runtime_configuration_polymorphic_vectors")
@PolymorphicIndex(
    .vector(dimensions: 3, metric: .cosine),
    embedding: "embedding",
    name: "RuntimeConfigurationPolymorphicVectorDocument_embedding"
)
protocol RuntimeConfigurationPolymorphicVectorDocument:
    Polymorphable<RuntimeConfigurationPolymorphicVectorDocumentPolymorphicGroup>
{
    var id: String { get }
    var embedding: Vector { get }
}

import DatabaseKit

@Persistable
@OWLClass(
    "https://example.invalid/ontology/RuntimeConfigurationOWLEntity",
    individualIRIBase: "https://example.invalid/runtime-configuration/"
)
struct RuntimeConfigurationOWLEntity {
    var id: String = ""

    @OWLDataProperty(
        "https://example.invalid/ontology/runtimeConfigurationName"
    )
    var name: String
}

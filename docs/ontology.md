# Ontology And Graph

GraphIndex provides graph edges, RDF triples, SPARQL-oriented queries, and OWL
metadata. It is an optional index module built on the same StorageKit
execution layer.

## Responsibility Split

~~~text
database-kit
  OWL and graph metadata, terms, query contracts
        |
        v
database-framework
  GraphIndex, OntologyIndex, traversal, reasoning, persistence
        |
        v
StorageKit
  transactions and key/value storage
~~~

Persistable owns storage metadata. Graph and ontology annotations add semantic
metadata without changing the persistence contract.

## Ontology Metadata

~~~swift
import Database
import Graph

@Persistable
@OWLClass("http://example.org/onto#Employee")
struct Employee {
    #Directory<Employee>("app", "employees")

    @OWLDataProperty("http://example.org/onto#name")
    var name: String
}
~~~

Use OWLObjectProperty for a persisted edge type and GraphIndexKind for explicit
RDF triple layouts. The Graph module keeps these declarations separate from
the core persistence macro.

## Persistence

Register ontology-aware models in the schema:

~~~swift
let schema = Schema([Employee.self, Department.self, WorksFor.self, RDFTriple.self])
let container = try await DBContainer(
    for: schema,
    configuration: configuration
)
~~~

Ontology loading and graph writes use the container's selected StorageEngine.
The storage layer is not tied to FoundationDB, although some graph algorithms
may have backend-specific operational limits.

## Query Boundary

- GraphIndex owns graph pattern and traversal execution.
- QueryAST owns SQL/SPARQL parsing and serialization.
- DatabaseEngine owns transaction and result orchestration.
- StorageKit owns physical transaction and range operations.

See [Sources/GraphIndex/README.md](../Sources/GraphIndex/README.md) for supported
graph query and reasoning APIs.

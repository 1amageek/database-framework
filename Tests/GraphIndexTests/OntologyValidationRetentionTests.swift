import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import OntologyIndex
import StorageKit
import TestSupport
import Testing

@Suite("Ontology validation retention")
struct OntologyValidationRetentionTests {
    @Test("Validation reconstruction is preflighted and retained")
    func reconstructionIsPreflightedAndRetained() async throws {
        let engine = InMemoryEngine()
        let store = OntologyStore(
            subspace: OntologySubspace(
                base: Subspace(
                    prefix: Tuple("ontology-validation-retention").pack()
                )
            )
        )
        var ontology = OWLOntology(
            iri: "https://example.com/retained-ontology"
        )
        ontology.classes = [
            OWLClass(iri: "https://example.com/Person"),
            OWLClass(iri: "https://example.com/Employee"),
        ]
        ontology.axioms = [
            .subClassOf(
                sub: .named("https://example.com/Employee"),
                sup: .named("https://example.com/Person")
            )
        ]
        let storedOntology = ontology
        try await engine.withTransaction { transaction in
            try await store.loadOntology(
                storedOntology,
                at: Timestamp(secondsSinceUnixEpoch: 1),
                transaction: transaction
            )
        }
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 1_000,
                maximumIntermediateRows: 100,
                maximumIntermediateBytes: 4 * 1_024 * 1_024
            ),
            monotonicClock: TestProcessMonotonicClock()
        )

        let retained = try await engine.withTransaction { transaction in
            try await store.reconstructRetainedForValidation(
                iri: storedOntology.iri,
                transaction: transaction,
                workMeter: meter
            )
        }
        let value = try #require(retained)
        #expect(value.ontology.classes.count == 2)
        #expect(value.ontology.axioms == storedOntology.axioms)
        #expect(meter.retainedIntermediateRows == 4)
        #expect(meter.retainedIntermediateBytes > 0)
        #expect(meter.peakIntermediateBytes == meter.retainedIntermediateBytes)
        value.reservation.release()
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Validation reconstruction rejects an unadmitted ontology")
    func reconstructionRejectsBeforeMaterialization() async throws {
        let engine = InMemoryEngine()
        let store = OntologyStore(
            subspace: OntologySubspace(
                base: Subspace(
                    prefix: Tuple("ontology-validation-rejection").pack()
                )
            )
        )
        let ontology = OWLOntology(
            iri: "https://example.com/rejected-ontology"
        )
        try await engine.withTransaction { transaction in
            try await store.loadOntology(
                ontology,
                at: Timestamp(secondsSinceUnixEpoch: 1),
                transaction: transaction
            )
        }
        let meter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 100,
                maximumIntermediateRows: 10,
                maximumIntermediateBytes: 1
            ),
            monotonicClock: TestProcessMonotonicClock()
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            try await engine.withTransaction { transaction in
                try await store.reconstructRetainedForValidation(
                    iri: ontology.iri,
                    transaction: transaction,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }
}

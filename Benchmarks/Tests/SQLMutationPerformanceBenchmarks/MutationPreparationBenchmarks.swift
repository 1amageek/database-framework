import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine
import StorageKit
import Testing

@Suite("Mutation preparation benchmarks", .serialized)
struct MutationPreparationBenchmarks {
    @Test("Prepare 1,000 inserts", .timeLimit(.minutes(1)))
    func prepareOneThousandInserts() async throws {
        try await withMutationPreparationContainer { container in
            let changes = try makeChanges(count: 1_000)
            let executor = DatabaseEntityMutationExecutor(
                container: container,
                limits: try DatabaseEntityMutationLimits(
                    maximumChanges: changes.count,
                    maximumPreconditions: changes.count
                )
            )
            let budget = ExecutionBudget(
                maximumWorkUnits: 10_000_000,
                maximumIntermediateRows: 4_000,
                maximumIntermediateBytes: 32 * 1_024 * 1_024
            )

            let measurement = try await measureBenchmark(
                name: "1,000 insert plans",
                warmupIterations: 5,
                measurementIterations: 50
            ) {
                let meter = DatabaseWorkMeter(
                    budget: budget,
                    monotonicClock: container.monotonicClock
                )
                _ = try executor.prepare(
                    changes,
                    preconditions: [],
                    workMeter: meter
                )
            }

            let footprintMeter = DatabaseWorkMeter(
                budget: budget,
                monotonicClock: container.monotonicClock
            )
            _ = try executor.prepare(
                changes,
                preconditions: [],
                workMeter: footprintMeter
            )
            printBenchmarkReport(
                title: "Entity mutation preparation",
                measurement: measurement
            )
            print(
                "BENCHMARK_FOOTPRINT | peak_rows="
                    + "\(footprintMeter.peakIntermediateRows)"
                    + " | peak_bytes="
                    + "\(footprintMeter.peakIntermediateBytes)"
            )
        }
    }

    private func makeChanges(
        count: Int
    ) throws -> [EntityMutationChange] {
        try (0..<count).map { index in
            let model = MutationPreparationBenchmarkEntity(
                id: "entity-\(index)",
                title: "Benchmark entity \(index)"
            )
            let persisted = try PersistedModel(model)
            return EntityMutationChange(
                kind: .insert,
                identity: try EntityReference(
                    entity: MutationPreparationBenchmarkEntity.persistableType,
                    id: .string(model.id)
                ),
                fields: try DatabaseEntityProjection.fieldObject(
                    for: persisted
                )
            )
        }
    }
}

private func withMutationPreparationContainer<Result: Sendable>(
    _ body: (DBContainer) async throws -> Result
) async throws -> Result {
    let schema = try Schema(
        entities: [try MutationPreparationBenchmarkEntity.schemaEntity],
        version: .init(1, 0, 0)
    )
    let container = try await DBContainer.open(
        for: schema,
        configuration: DBConfiguration(
            name: "mutation-preparation-benchmark",
            storageEngine: InMemoryEngine(),
            monotonicClock: BenchmarkProcessMonotonicClock(),
            wallClock: FixedBenchmarkWallClock()
        ),
        runtimeConfiguration: try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "mutation-preparation-benchmark",
                revision: 1
            ),
            entityRuntimes: [
                try EntityRuntimeDefinition(
                    MutationPreparationBenchmarkEntity.self
                ).registration()
            ]
        )
    )
    do {
        let result = try await body(container)
        await container.shutdown()
        return result
    } catch {
        await container.shutdown()
        throw error
    }
}

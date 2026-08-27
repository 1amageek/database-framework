import DatabaseKit
import DatabaseTypes
import DatabaseWire
import Testing
import TestSupport
@testable import DatabaseEngine

@Suite("Query-scoped field-value ownership")
struct DatabaseQueryScopedFieldValueTests {
    @Test("A one-byte-short budget rejects the producer before invocation")
    func oneByteShortRejectsProducerBeforeInvocation() throws {
        let maximum = DatabaseIntermediateFootprint(bytes: 4_096)
        let meter = makeMeter(maximumIntermediateBytes: maximum.bytes - 1)
        var invoked = false

        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximum,
                workMeter: meter,
                stage: .projection
            ) {
                invoked = true
                return .string("unreachable")
            }
        }

        #expect(!invoked)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Success shrinks to exact ownership and cancellation rolls back")
    func exactShrinkAndCancellationRollback() async throws {
        let value = FieldValue.string("owned-result")
        let measurementMeter = makeMeter(maximumIntermediateBytes: 1_000_000)
        let actual = try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: measurementMeter,
            stage: .projection
        )
        let maximum = try actual.adding(
            DatabaseIntermediateFootprint(bytes: 1_024)
        )
        let meter = makeMeter(
            maximumIntermediateBytes: maximum.bytes + 4_096
        )

        do {
            let scoped = try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximum,
                workMeter: meter,
                stage: .projection
            ) {
                value
            }
            #expect(meter.retainedIntermediateBytes == actual.bytes)
            scoped.withValue { borrowed in
                #expect(borrowed == value)
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)

        await #expect(throws: CancellationError.self) {
            _ = try await DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximum,
                workMeter: meter,
                stage: .projection
            ) { () async throws -> FieldValue in
                await Task.yield()
                throw CancellationError()
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("An oversized produced value is rejected and releases its admission")
    func oversizedProducedValueReleasesAdmission() throws {
        let value = FieldValue.string(String(repeating: "x", count: 1_024))
        let measurementMeter = makeMeter(maximumIntermediateBytes: 1_000_000)
        let actual = try CanonicalRelationalFootprintMeter.valueFootprint(
            of: value,
            workMeter: measurementMeter,
            stage: .projection
        )
        let maximum = DatabaseIntermediateFootprint(
            rows: actual.rows,
            bytes: actual.bytes - 1
        )
        let meter = makeMeter(maximumIntermediateBytes: actual.bytes + 1_024)

        do {
            _ = try DatabaseQueryScopedFieldValue.producing(
                maximumFootprint: maximum,
                workMeter: meter,
                stage: .projection
            ) {
                value
            }
            Issue.record("Expected the produced value to exceed its admission")
        } catch let error as DatabaseQueryScopedFieldValueError {
            #expect(
                error == .payloadFootprintExceeded(
                    maximumRows: maximum.rows,
                    maximumBytes: maximum.bytes,
                    actualRows: actual.rows,
                    actualBytes: actual.bytes
                )
            )
        }

        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 10_000,
                maximumIntermediateRows: 100,
                maximumIntermediateBytes: maximumIntermediateBytes,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}

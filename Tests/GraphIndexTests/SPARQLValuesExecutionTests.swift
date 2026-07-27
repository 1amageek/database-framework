import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import GraphIndex
import DatabaseKit
import StorageKit
import Testing

@Suite("SPARQL VALUES execution")
struct SPARQLValuesExecutionTests {
    @Test("VALUES preserves row order, duplicates, and UNDEF bindings")
    func valuesMultisetSemantics() async throws {
        let pattern = try GraphPatternConverter.convert(
            .values(
                variables: ["value", "optional"],
                bindings: [
                    [.int(1), nil],
                    [.int(1), .string("bound")],
                    [.int(1), nil],
                ]
            )
        )
        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(bindings.count == 3)
        #expect(bindings[0]["?optional"] == nil)
        #expect(bindings[1]["?optional"] != nil)
        #expect(bindings[2]["?optional"] == nil)
        for binding in bindings {
            guard case .rdfTerm(.literal(let literal)) = binding["?value"] else {
                Issue.record("Expected a canonical RDF literal")
                return
            }
            #expect(literal.lexicalForm == "1")
        }
    }

    @Test("An empty VALUES table has no solutions")
    func emptyValuesTable() async throws {
        let pattern = try GraphPatternConverter.convert(
            .values(variables: ["value"], bindings: [])
        )
        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(bindings.isEmpty)
    }

    @Test("VALUES participates in EXISTS without a fallback result")
    func valuesInsideExists() async throws {
        let nonEmpty = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .values(variables: ["value"], bindings: [[.int(1)]])
            )
        )
        let empty = SelectQuery(
            projection: .all,
            source: .graphPattern(
                .values(variables: ["value"], bindings: [])
            )
        )
        let executor = SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        )
        let meter = DatabaseWorkMeter(budget: ExecutionBudget())

        let (present, _) = try await executor.execute(
            pattern: .filter(
                .basic([]),
                .query(try SPARQLExpressionPlan(.exists(nonEmpty)))
            ),
            limit: nil,
            offset: 0,
            workMeter: meter
        )
        let (absent, _) = try await executor.execute(
            pattern: .filter(
                .basic([]),
                .query(try SPARQLExpressionPlan(.exists(empty)))
            ),
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(present.count == 1)
        #expect(absent.isEmpty)
    }

    @Test("Compatible shared VALUES variables merge without rebinding")
    func compatibleSharedVariableJoin() async throws {
        let pattern = try GraphPatternConverter.convert(
            .join(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(variables: ["value"], bindings: [[.int(1)]])
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(bindings.count == 1)
        #expect(bindings[0].isBound("?value"))
    }

    @Test("Incompatible shared VALUES variables produce no solution")
    func incompatibleSharedVariableJoin() async throws {
        let pattern = try GraphPatternConverter.convert(
            .join(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(variables: ["value"], bindings: [[.int(2)]])
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(bindings.isEmpty)
    }

    @Test("LATERAL VALUES respects compatible outer bindings")
    func lateralSharedVariableCompatibility() async throws {
        let pattern = try GraphPatternConverter.convert(
            .lateral(
                .values(variables: ["value"], bindings: [[.int(1)]]),
                .values(
                    variables: ["value", "visible"],
                    bindings: [[.int(1), .string("yes")]]
                )
            )
        )

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget()
            )
        )

        #expect(bindings.count == 1)
        #expect(bindings[0].isBound("?visible"))
    }

    @Test("A large VALUES table compiles and executes without recursive UNION")
    func largeValuesTableIsLinear() async throws {
        let rowCount = 10_000
        var rows: [[Literal?]] = []
        rows.reserveCapacity(rowCount)
        for value in 0..<rowCount {
            rows.append([.int(Int64(value))])
        }

        let pattern = try GraphPatternConverter.convert(
            .values(variables: ["value"], bindings: rows)
        )
        guard case .values(let table) = pattern else {
            Issue.record("Expected a first-class VALUES execution node")
            return
        }
        #expect(table.rowCount == rowCount)

        let (bindings, _) = try await SPARQLQueryExecutor(
            database: InMemoryEngine(),
            sources: []
        ).execute(
            pattern: pattern,
            limit: nil,
            offset: 0,
            workMeter: DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: UInt32(rowCount),
                    maximumWorkUnits: 100_000,
                    timeoutMilliseconds: 30_000
                )
            )
        )

        #expect(bindings.count == rowCount)
    }
}

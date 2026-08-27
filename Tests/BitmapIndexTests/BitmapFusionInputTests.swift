import DatabaseKit
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import BitmapIndex

@Persistable
private struct BitmapFusionInputItem {
    var id: String
    var status: String
}

@Persistable
private struct BitmapFusionExecutionItem {
    #Index(
        .bitmap(
            name: "bitmap_fusion_status",
            field: \BitmapFusionExecutionItem.status
        )
    )

    var id: String
    var status: String
    var region: String
    var priority: Int64
}

@Persistable
private struct BitmapFusionInt32Item {
    var id: String
    var value: Int32
}

@Suite("Bitmap Fusion input")
struct BitmapFusionInputTests {
    @Test("Bitmap lowers eligibility without claiming score ownership")
    func lowersToCanonicalInput() throws {
        let input = try Bitmap(
            BitmapFusionInputItem.fields.status,
            in: ["active", "pending"]
        )
        .index(named: "status_bitmap")
        .limit(8)
        .fusionInput

        #expect(input.scoring == nil)
        #expect(input.requirement == .unrestricted)
        #expect(input.limit == 8)
        guard case .index(let source) = input.operation else {
            Issue.record("Bitmap must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "status_bitmap",
            type: .bitmap
        ))
        #expect(source.referencedFields == [
            BitmapFusionInputItem.fields.status.identity,
        ])
        #expect(source.parameters[BitmapReadParameter.fieldName] == .string(
            "status"
        ))
        #expect(source.parameters[BitmapReadParameter.operation] == .string(
            BitmapReadParameter.inOperation
        ))
        #expect(source.parameters[BitmapReadParameter.values] == .array([
            .string("active"),
            .string("pending"),
        ]))
    }

    @Test("Bitmap preserves persisted integer widths")
    func preservesIntegerWidths() throws {
        let input = try Bitmap(
            BitmapFusionInt32Item.fields.value,
            equals: Int32(7)
        ).fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Bitmap must lower to an index operation")
            return
        }
        #expect(source.parameters[BitmapReadParameter.values] == .array([
            .int32(7),
        ]))
    }

    @Test("Bitmap physical validation rejects non-canonical contracts")
    func validatesPhysicalContract() throws {
        let entity = try BitmapFusionExecutionItem.schemaEntity
        let descriptor = try #require(entity.indexes.first)
        let input = try Bitmap(
            BitmapFusionExecutionItem.fields.status,
            equals: "active"
        ).fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Bitmap must lower to an index operation")
            return
        }
        let executor = BitmapFusionIndexReadExecutor()
        try executor.validate(
            FusionIndexValidationRequest(
                source: source,
                scoring: nil,
                descriptor: descriptor
            )
        )

        var unknown = source.parameters
        unknown["unknown"] = .bool(true)
        #expect {
            try executor.validate(
                FusionIndexValidationRequest(
                    source: FusionIndexSource(
                        selection: source.selection,
                        referencedFields: source.referencedFields,
                        parameters: unknown
                    ),
                    scoring: nil,
                    descriptor: descriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .bitmap,
                parameter: "unknown"
            )
        }

        #expect {
            try executor.validate(
                FusionIndexValidationRequest(
                    source: source,
                    scoring: .position,
                    descriptor: descriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .bitmap,
                parameter: "scoring"
            )
        }
    }

    @Test("Bitmap executes unrestricted and candidate-restricted Fusion")
    func executesUnrestrictedAndRestrictedFusion() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for item in [
            BitmapFusionExecutionItem(
                id: "active-high",
                status: "active",
                region: "north",
                priority: 30
            ),
            BitmapFusionExecutionItem(
                id: "active-low",
                status: "active",
                region: "south",
                priority: 10
            ),
            BitmapFusionExecutionItem(
                id: "pending-middle",
                status: "pending",
                region: "north",
                priority: 20
            ),
            BitmapFusionExecutionItem(
                id: "inactive-top",
                status: "inactive",
                region: "north",
                priority: 100
            ),
        ] {
            try context.insert(item)
        }
        try await context.save()
        let activeBitmap = try await bitmapSnapshot(
            for: "active",
            context: context,
            container: container
        )
        #expect(activeBitmap.cardinality == 2)
        #expect(activeBitmap.identifiers.count == 2)

        let active = try Bitmap(
            BitmapFusionExecutionItem.fields.status,
            equals: "active"
        )
        let unrestricted = try await context.execute(
            FusionQuery<BitmapFusionExecutionItem> {
                active
                Rank(BitmapFusionExecutionItem.fields.priority)
            }
        )
        #expect(unrestricted.results.map(\.item.id) == [
            "active-high",
            "active-low",
        ])

        let activeOrPending = try Bitmap(
            BitmapFusionExecutionItem.fields.status,
            in: ["active", "pending"]
        )
        let unrestrictedUnion = try await context.execute(
            FusionQuery<BitmapFusionExecutionItem> {
                activeOrPending
                Rank(BitmapFusionExecutionItem.fields.priority)
            }
        )
        #expect(unrestrictedUnion.results.map(\.item.id) == [
            "active-high",
            "pending-middle",
            "active-low",
        ])

        let north = try Filter(
            BitmapFusionExecutionItem.fields.region,
            equals: "north"
        )
        let restricted = try await context.execute(
            FusionQuery<BitmapFusionExecutionItem> {
                north
                activeOrPending
                Rank(BitmapFusionExecutionItem.fields.priority)
            }
        )
        #expect(restricted.results.map(\.item.id) == [
            "active-high",
            "pending-middle",
        ])
    }

    @Test("Bitmap canonical execution uses the retained regular model path")
    func executesCanonicalRegularRead() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            BitmapFusionExecutionItem(
                id: "canonical-active",
                status: "active",
                region: "north",
                priority: 10
            )
        )
        try await context.save()

        let response = try await context.query(
            SelectQuery(
                projection: .all,
                source: .table(
                    TableRef(BitmapFusionExecutionItem.persistableType)
                ),
                accessPath: .index(
                    IndexScanSource(
                        indexName: "bitmap_fusion_status",
                        indexType: .bitmap,
                        parameters: [
                            BitmapReadParameter.fieldName: .string("status"),
                            BitmapReadParameter.operation: .string(
                                BitmapReadParameter.equalsOperation
                            ),
                            BitmapReadParameter.values: .array([
                                .string("active")
                            ])
                        ]
                    )
                )
            )
        )
        let row = try #require(response.rows.first)
        #expect(response.rows.count == 1)
        #expect(row.fields["id"] == .string("canonical-active"))
        #expect(row.fields["status"] == .string("active"))
    }

    @Test("Missing bitmap ID mappings fail as corruption")
    func missingIdentifierMappingFailsExplicitly() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            BitmapFusionExecutionItem(
                id: "orphaned",
                status: "active",
                region: "north",
                priority: 1
            )
        )
        try await context.save()

        let activeBitmap = try await bitmapSnapshot(
            for: "active",
            context: context,
            container: container
        )
        #expect(activeBitmap.cardinality == 1)
        let identifier = try #require(activeBitmap.identifiers.first)
        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "bitmap_fusion_status",
                    indexType: .bitmap,
                    for: BitmapFusionExecutionItem.self,
                    transaction: transaction
                )
            )
            try transaction.clear(
                key: readable.subspace.subspace("ids").pack(
                    Tuple(Int(identifier))
                )
            )
        }

        let active = try Bitmap(
            BitmapFusionExecutionItem.fields.status,
            equals: "active"
        )
        await #expect {
            _ = try await context.execute(
                FusionQuery<BitmapFusionExecutionItem> {
                    active
                    Rank(BitmapFusionExecutionItem.fields.priority)
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .corruptedIndex(.bitmap)
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let entity = try BitmapFusionExecutionItem.schemaEntity
        let provider = BitmapIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            BitmapFusionExecutionItem.self
        )
        try BitmapReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(provider)
        return try await DBContainer.open(
            testing: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "bitmap-fusion-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    BitmapFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
    }

    private func bitmapSnapshot(
        for value: String,
        context: DatabaseContext,
        container: DBContainer
    ) async throws -> (cardinality: Int, identifiers: [UInt32]) {
        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "bitmap_fusion_status",
                    indexType: .bitmap,
                    for: BitmapFusionExecutionItem.self,
                    transaction: transaction
                )
            )
            let bitmap = try await BitmapIndexReader(
                subspace: readable.subspace
            ).bitmap(
                for: [try FieldValue.string(value).toTupleElement()],
                transaction: transaction,
                workMeter: DatabaseWorkMeter(
                    budget: ExecutionBudget(),
                    monotonicClock: TestProcessMonotonicClock()
                )
            ).promoteToOutput()
            return (bitmap.cardinality, Array(bitmap))
        }
    }
}

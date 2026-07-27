import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseIndexStatusPager: Sendable {
    private static let maximumPageSize = 256

    private let container: DBContainer
    private let wireLimits: DatabaseWireLimits

    init(
        container: DBContainer,
        wireLimits: DatabaseWireLimits
    ) {
        self.container = container
        self.wireLimits = wireLimits
    }

    func page(
        entity entityFilter: String?,
        index indexFilter: String?,
        partitions partitionFilter: FieldObject,
        continuation: ByteString?,
        budget: ExecutionBudget
    ) async throws -> DatabaseIndexStatusTargetPage {
        try validateFilters(
            entity: entityFilter,
            index: indexFilter,
            partitions: partitionFilter
        )
        let entities = try filteredEntities(
            entity: entityFilter,
            index: indexFilter
        )
        var state = try decodeState(
            continuation,
            entityFilter: entityFilter,
            indexFilter: indexFilter,
            partitionFilter: partitionFilter,
            entityCount: entities.count
        )
        let pageLimit = Int(
            min(
                UInt64(budget.maximumRows),
                budget.maximumWorkUnits,
                UInt64(Self.maximumPageSize)
            )
        )
        var remainingWork = budget.maximumWorkUnits
        var targets: [DatabaseIndexStatusTarget] = []
        targets.reserveCapacity(pageLimit)

        while state.entityPosition < entities.count,
              targets.count < pageLimit,
              remainingWork > 0 {
            let entity = entities[state.entityPosition]
            let indexes = filteredIndexes(entity, index: indexFilter)
            guard !indexes.isEmpty else {
                state.advanceEntity()
                remainingWork -= 1
                continue
            }
            guard state.indexPosition < indexes.count else {
                throw DatabaseMaintenanceRuntimeError.invalidContinuation
            }

            if !partitionFilter.isEmpty {
                appendTarget(
                    entity: entity,
                    indexes: indexes,
                    partitions: partitionFilter,
                    state: &state,
                    targets: &targets
                )
                remainingWork -= 1
                if state.indexPosition == indexes.count {
                    state.advanceEntity()
                }
                continue
            }

            if entity.hasDynamicDirectory {
                let catalogPage = try await container.partitionCatalogPage(
                    entity: entity.name,
                    continuation: state.partitionCatalogContinuation,
                    limit: 1
                )
                guard let entry = catalogPage.entries.first else {
                    state.advanceEntity()
                    remainingWork -= 1
                    continue
                }
                appendTarget(
                    entity: entity,
                    indexes: indexes,
                    partitions: entry.partitions,
                    state: &state,
                    targets: &targets
                )
                remainingWork -= 1
                if state.indexPosition == indexes.count {
                    if let next = catalogPage.continuation {
                        state.indexPosition = 0
                        state.partitionCatalogContinuation = next
                    } else {
                        state.advanceEntity()
                    }
                }
                continue
            }

            guard state.partitionCatalogContinuation == nil else {
                throw DatabaseMaintenanceRuntimeError.invalidContinuation
            }
            appendTarget(
                entity: entity,
                indexes: indexes,
                partitions: FieldObject(),
                state: &state,
                targets: &targets
            )
            remainingWork -= 1
            if state.indexPosition == indexes.count {
                state.advanceEntity()
            }
        }

        let next = state.entityPosition < entities.count
            ? try encodeState(
                state,
                entityFilter: entityFilter,
                indexFilter: indexFilter,
                partitionFilter: partitionFilter
            )
            : nil
        return DatabaseIndexStatusTargetPage(
            targets: targets,
            continuation: next
        )
    }

    private func validateFilters(
        entity: String?,
        index: String?,
        partitions: FieldObject
    ) throws {
        if let entity, entity.isEmpty {
            throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                "indexStatus entity filter must not be empty"
            )
        }
        if let index, index.isEmpty {
            throw DatabaseMaintenanceRuntimeError.invalidInvocation(
                "indexStatus index filter must not be empty"
            )
        }
        if !partitions.isEmpty, entity == nil {
            throw DatabaseMaintenanceRuntimeError.entityRequiredForPartitionFilter
        }
    }

    private func filteredEntities(
        entity entityFilter: String?,
        index indexFilter: String?
    ) throws -> [Schema.Entity] {
        if let entityFilter,
           container.schema.entitiesByName[entityFilter] == nil {
            throw DatabaseIndexRebuildError.entityNotFound(entityFilter)
        }
        let entities = container.schema.entities
            .filter { entityFilter == nil || $0.name == entityFilter }
            .sorted { $0.name < $1.name }
        if let indexFilter {
            let hasIndex = entities.contains { entity in
                entity.indexDescriptors.contains { $0.name == indexFilter }
            }
            guard hasIndex else {
                throw DatabaseIndexRebuildError.indexNotFound(
                    entity: entityFilter ?? "*",
                    index: indexFilter
                )
            }
        }
        return entities
    }

    private func filteredIndexes(
        _ entity: Schema.Entity,
        index indexFilter: String?
    ) -> [IndexDescriptor] {
        entity.indexDescriptors
            .filter { indexFilter == nil || $0.name == indexFilter }
            .sorted { $0.name < $1.name }
    }

    private func appendTarget(
        entity: Schema.Entity,
        indexes: [IndexDescriptor],
        partitions: FieldObject,
        state: inout State,
        targets: inout [DatabaseIndexStatusTarget]
    ) {
        let index = indexes[state.indexPosition]
        targets.append(
            DatabaseIndexStatusTarget(
                entity: entity.name,
                index: index.name,
                partitions: partitions
            )
        )
        state.indexPosition += 1
    }

    private func decodeState(
        _ continuation: ByteString?,
        entityFilter: String?,
        indexFilter: String?,
        partitionFilter: FieldObject,
        entityCount: Int
    ) throws -> State {
        guard let continuation else {
            return State()
        }
        let decoded: DatabaseIndexStatusContinuation
        do {
            decoded = try ServerPayloadDecoder.decode(
                DatabaseIndexStatusContinuation.self,
                from: continuation,
                limits: wireLimits
            )
        } catch {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
        guard decoded.entityFilter == entityFilter,
              decoded.indexFilter == indexFilter,
              decoded.partitionFilter == partitionFilter,
              let entityPosition = Int(exactly: decoded.entityPosition),
              let indexPosition = Int(exactly: decoded.indexPosition),
              entityPosition >= 0,
              entityPosition < entityCount,
              indexPosition >= 0 else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
        return State(
            entityPosition: entityPosition,
            indexPosition: indexPosition,
            partitionCatalogContinuation: decoded.partitionCatalogContinuation
        )
    }

    private func encodeState(
        _ state: State,
        entityFilter: String?,
        indexFilter: String?,
        partitionFilter: FieldObject
    ) throws -> ByteString {
        guard let entityPosition = UInt32(exactly: state.entityPosition),
              let indexPosition = UInt32(exactly: state.indexPosition) else {
            throw DatabaseMaintenanceRuntimeError.invalidContinuation
        }
        return try ServerPayloadEncoder.encode(
            DatabaseIndexStatusContinuation(
                entityFilter: entityFilter,
                indexFilter: indexFilter,
                partitionFilter: partitionFilter,
                entityPosition: entityPosition,
                indexPosition: indexPosition,
                partitionCatalogContinuation: state.partitionCatalogContinuation
            ),
            limits: wireLimits
        )
    }

    private struct State {
        var entityPosition: Int = 0
        var indexPosition: Int = 0
        var partitionCatalogContinuation: ByteString? = nil

        mutating func advanceEntity() {
            entityPosition += 1
            indexPosition = 0
            partitionCatalogContinuation = nil
        }
    }
}

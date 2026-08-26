import DatabaseKit

/// Logical plan and conservative field authority computed without exposing
/// selector-resolution failures before authorization.
struct FusionResolvedPlan: Sendable {
    enum IndexResolution: Sendable {
        case resolved(IndexDescriptor)
        case failed(FusionExecutionError)
    }

    enum ConnectedResolution: Sendable {
        case resolved(
            edgeEntity: Schema.Entity,
            descriptor: IndexDescriptor
        )
        case missingEdgeEntity(String)
        case failed(FusionExecutionError)
    }

    struct Stage: Sendable {
        let inputs: DatabaseSharedRetainedArray<Input>
    }

    struct Input: Sendable {
        enum Operation: Sendable {
            case index(
                source: FusionIndexSource,
                resolution: IndexResolution
            )
            case filter(Expression)
            case order([SortKey])
            case connected(
                source: FusionConnectedSource,
                resolution: ConnectedResolution
            )
        }

        let operation: Operation
        let scoring: FusionScoring?
        let requirement: FusionInputRequirement
        let limit: Int?
        let stageIndex: Int
        let inputIndex: Int
    }

    let stages: DatabaseSharedRetainedArray<Stage>
    let authorizationPlan: DatabaseFieldReadAuthorizationPlan
    let entity: Schema.Entity
    let tableRef: TableRef
    let listAuthorizationRequirement: DatabaseListReadAuthorizationRequirement
}

/// Fully schema-resolved Fusion plan produced before physical index I/O.
struct FusionPreparedPlan: Sendable {
    struct Stage: Sendable {
        let inputs: DatabaseSharedRetainedArray<Input>
    }

    struct Input: Sendable {
        enum Operation: Sendable {
            case index(
                source: FusionIndexSource,
                descriptor: IndexDescriptor,
                executor: any FusionIndexReadExecutor
            )
            case filter(Expression)
            case order([SortKey])
            case connected(
                source: FusionConnectedSource,
                edgeEntity: Schema.Entity,
                descriptor: IndexDescriptor,
                executor: any FusionConnectedReadExecutor
            )
        }

        let operation: Operation
        let scoring: FusionScoring?
        let requirement: FusionInputRequirement
        let limit: Int?
        let stageIndex: Int
        let inputIndex: Int
    }

    let stages: DatabaseSharedRetainedArray<Stage>
    let entity: Schema.Entity
    let tableRef: TableRef
    let listAuthorizationRequirement: DatabaseListReadAuthorizationRequirement
}

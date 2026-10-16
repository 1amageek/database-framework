import DatabaseKit
import DatabaseTypes

package enum DatabaseEntityMutationFootprintMeter {
    package static func encodedValueFootprint(
        _ value: QueryValue,
        type: QueryColumnType,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        let encoded = try QueryFieldStorageAdapter.measure(value, type: type)
        let source = try QueryValueValidator.measure(value)
        let entries = try DatabaseIntermediateFootprint(bytes: encoded.objectEntries)
            .adding(DatabaseIntermediateFootprint(bytes: encoded.arrayElements)).bytes
        let bytes = try DatabaseIntermediateFootprint(bytes: encoded.newUTF8Bytes)
            .adding(DatabaseIntermediateFootprint(bytes: source.retainedByteCount)).bytes
        try workMeter.consume(encoded.valueNodes, at: .mutationPlanning)
        return try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(
            nodeCount: encoded.valueNodes,
            collectionElementCount: entries,
            retainedByteCount: bytes
        )
    }

    package static func footprint(
        of change: EntityMutationChange,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try footprint(
            identity: change.identity,
            fields: change.fields,
            workMeter: workMeter
        )
    }

    package static func footprint(
        of identity: EntityReference,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try CanonicalRelationalFootprintMeter.sourceRowFootprint(
            fields: ["identity": .reference(identity)], sourceName: nil, annotations: [:],
            version: nil, workMeter: workMeter)
    }

    package static func footprint(
        of precondition: EntityMutationPrecondition,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        switch precondition {
        case .expectedVersion(let identity, let version):
            return try CanonicalRelationalFootprintMeter.sourceRowFootprint(
                fields: ["identity": .reference(identity), "version": .bytes(version)],
                sourceName: nil, annotations: [:], version: nil, workMeter: workMeter)
        case .mustExist(let identity), .mustNotExist(let identity):
            return try footprint(of: identity, workMeter: workMeter)
        }
    }

    package static func footprint(
        of mutation: PersistableMutation,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try footprint(
            identity: mutation.identity,
            fields: try DatabaseEntityProjection.fieldObject(
                for: mutation.model
            ),
            workMeter: workMeter
        )
    }

    package static func footprint(
        identity: EntityReference,
        model: PersistedModel?,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        guard let model else {
            return try footprint(of: identity, workMeter: workMeter)
        }
        return try footprint(
            identity: identity,
            fields: try DatabaseEntityProjection.fieldObject(for: model),
            workMeter: workMeter
        )
    }

    package static func footprint(
        of model: PersistedModel,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try CanonicalRelationalFootprintMeter.sourceRowFootprint(
            fields: ["model": .object(try DatabaseEntityProjection.fieldObject(for: model))],
            sourceName: nil, annotations: [:], version: nil, workMeter: workMeter)
    }

    private static func footprint(
        identity: EntityReference,
        fields: FieldObject,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        try CanonicalRelationalFootprintMeter.sourceRowFootprint(
            fields: ["identity": .reference(identity), "fields": .object(fields)],
            sourceName: nil, annotations: [:], version: nil, workMeter: workMeter)
    }
}

import DatabaseKit
import DatabaseTypes
import StorageKit

/// One request-owned polymorphic batch.
///
/// The backing Array, decoded models, and identifiers remain coupled to the
/// originating work meter. Consumers operate through index-based scoped
/// methods; neither an entry nor the aggregate storage can escape by value.
package struct DatabaseRetainedPolymorphicEntities: ~Copyable, Sendable {
    fileprivate struct Entry: Sendable {
        let model: DatabaseRetainedPersistedModels.Entry
        let identifier: DatabaseRetainedPrimaryKey
        let typeName: String
        let typeCode: Int64

        var workMeter: DatabaseWorkMeter { model.workMeter }
    }

    /// Linear admission for one present or missing result slot.
    package struct EntryAdmission: ~Copyable {
        fileprivate let storage:
            DatabaseRetainedArrayAppendAdmission<Entry?>

        fileprivate init(
            storage: consuming DatabaseRetainedArrayAppendAdmission<Entry?>
        ) {
            self.storage = storage
        }
    }

    /// Producer that admits every result slot before its payload is decoded.
    package struct Builder: ~Copyable {
        private var storage: DatabaseRetainedArrayBuilder<Entry?>

        package init(
            workMeter: DatabaseWorkMeter,
            stage: DatabaseWorkStage,
            expectedCount: Int = 0
        ) throws {
            self.storage = try DatabaseRetainedArrayBuilder(
                workMeter: workMeter,
                stage: stage,
                layout: try DatabaseRetainedArrayLayout.forElement(
                    Entry?.self
                ),
                expectedCount: expectedCount
            )
        }

        package var count: Int { storage.count }
        package var workMeter: DatabaseWorkMeter { storage.workMeter }

        package mutating func prepareEntry(
            at stage: DatabaseWorkStage
        ) throws -> EntryAdmission {
            EntryAdmission(
                storage: try storage.prepareAppend(
                    footprint: DatabaseIntermediateFootprint(rows: 1),
                    at: stage
                )
            )
        }

        package mutating func appendMissing(
            using admission: consuming EntryAdmission
        ) {
            storage.append(nil, using: admission.storage)
        }

        /// Seals runtime-derived type metadata together with the identifier
        /// and the model that was decoded for that same runtime.
        package mutating func append(
            model: consuming DatabaseRetainedStoredModel,
            identifier: consuming DatabaseRetainedPrimaryKey,
            runtime: EntityRuntimeRegistration,
            using admission: consuming EntryAdmission
        ) throws {
            guard model.workMeter === storage.workMeter,
                  identifier.workMeter === storage.workMeter else {
                throw DatabaseIntermediateReservationError.workMeterMismatch
            }
            let expectedTypeCode = PolymorphicTypeCode.value(
                for: runtime.entity.name
            )
            try identifier.withValue { tuple in
                guard tuple.count > 0,
                      case .signedInteger(let storedTypeCode) =
                        try tuple.value(at: 0),
                      storedTypeCode == expectedTypeCode else {
                    throw PolymorphicRuntimeError.invalidStoredIdentifier
                }
            }
            try model.withModel { model in
                guard model.entity == runtime.entity.name else {
                    throw SchemaDrivenEntityRuntimeError.entityMismatch(
                        expected: runtime.entity.name,
                        actual: model.entity
                    )
                }
            }
            let entry = Entry(
                model: model.makeEntry(),
                identifier: identifier,
                typeName: runtime.entity.name,
                typeCode: expectedTypeCode
            )
            storage.append(entry, using: admission.storage)
        }

        package consuming func finish()
            -> DatabaseRetainedPolymorphicEntities {
            DatabaseRetainedPolymorphicEntities(storage: storage.finish())
        }
    }

    private let storage: DatabaseRetainedBuffer<Entry?>

    private init(storage: consuming DatabaseRetainedBuffer<Entry?>) {
        self.storage = storage
    }

    package var count: Int { storage.count }
    package var workMeter: DatabaseWorkMeter { storage.workMeter }

    /// Borrows one vector field without exposing the Copyable model or vector
    /// across the retained-owner boundary.
    package borrowing func withVectorField<Result>(
        at index: Int,
        keyPath: String,
        workMeter: DatabaseWorkMeter,
        _ body: (borrowing DatabaseRetainedVectorFieldView) throws -> Result
    ) throws -> Result? {
        guard self.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var result: Result?
        try storage.withElement(at: index) { borrowedEntry in
            let entry = copy borrowedEntry
            guard let entry else { return }
            result = try entry.model.withVectorField(
                keyPath: keyPath,
                workMeter: workMeter
            ) { field in
                try body(field)
            }
        }
        return result
    }

    /// Copies decoded models only at the public result boundary, then ends
    /// every request-intermediate ownership claim.
    package consuming func promoteModelsToPublicOutput() -> [PersistedModel] {
        var output: [PersistedModel] = []
        output.reserveCapacity(storage.count)
        for index in 0..<storage.count {
            storage.withElement(at: index) { borrowedEntry in
                let entry = copy borrowedEntry
                guard let entry else { return }
                entry.model.withModel { model in
                    output.append(copy model)
                }
            }
        }
        _ = storage.promoteToOutput()
        return output
    }

    /// Copies complete polymorphic entities only at the public API boundary.
    /// Missing internal slots are intentionally omitted because the public
    /// contract returns only models that exist.
    package consuming func promoteEntitiesToPublicOutput()
        -> [PolymorphicEntity] {
        var output: [PolymorphicEntity] = []
        output.reserveCapacity(storage.count)
        for index in 0..<storage.count {
            storage.withElement(at: index) { borrowedEntry in
                let entry = copy borrowedEntry
                guard let entry else { return }
                entry.model.withModel { model in
                    entry.identifier.withValue { identifier in
                        output.append(
                            PolymorphicEntity(
                                item: copy model,
                                typeName: entry.typeName,
                                typeCode: entry.typeCode,
                                polymorphicIdentifier: copy identifier
                            )
                        )
                    }
                }
            }
        }
        _ = storage.promoteToOutput()
        return output
    }

    /// Appends one polymorphic index row after admitting its complete model,
    /// type metadata, and optional feature annotation footprint.
    @discardableResult
    package borrowing func appendIndexRow(
        at index: Int,
        to rows: inout IndexReadResultBuilder,
        additionalAnnotation: (name: String, value: FieldValue)? = nil
    ) throws -> Bool {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var isPresent = false
        try storage.withElement(at: index) { entry in
            let entry = copy entry
            guard let entry else { return }
            isPresent = true
            try entry.model.withModel { model in
                let typeName = FieldValue.string(entry.typeName)
                let typeCode = FieldValue.int64(entry.typeCode)
                var footprint = try CanonicalRelationalFootprintMeter.footprint(
                    of: model,
                    workMeter: workMeter
                )
                footprint = try CanonicalRelationalFootprintMeter.footprint(
                    footprint,
                    appendingAnnotationNamed: PolymorphicRowAnnotation.typeName,
                    value: typeName,
                    workMeter: workMeter
                )
                footprint = try CanonicalRelationalFootprintMeter.footprint(
                    footprint,
                    appendingAnnotationNamed: PolymorphicRowAnnotation.typeCode,
                    value: typeCode,
                    workMeter: workMeter
                )
                if let additionalAnnotation {
                    footprint = try CanonicalRelationalFootprintMeter.footprint(
                        footprint,
                        appendingAnnotationNamed: additionalAnnotation.name,
                        value: additionalAnnotation.value,
                        workMeter: workMeter
                    )
                }
                try rows.append(footprint: footprint) {
                    var annotations: [String: FieldValue] = [
                        PolymorphicRowAnnotation.typeName: typeName,
                        PolymorphicRowAnnotation.typeCode: typeCode,
                    ]
                    if let additionalAnnotation {
                        annotations[additionalAnnotation.name] =
                            additionalAnnotation.value
                    }
                    return try IndexReadRow.materializing(
                        model,
                        annotations: annotations
                    )
                }
            }
        }
        return isPresent
    }

    /// Builds one canonical source row directly inside its admitted
    /// destination. No QueryRow or flattened intermediate is created.
    @discardableResult
    borrowing func appendCanonicalSourceRow(
        at index: Int,
        sourceName: String,
        to rows: inout DatabaseRetainedArrayBuilder<CanonicalSourceRow>,
        stage: DatabaseWorkStage
    ) throws -> Bool {
        guard rows.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var isPresent = false
        try storage.withElement(at: index) { entry in
            let entry = copy entry
            guard let entry else { return }
            isPresent = true
            try entry.model.withModel { model in
                try workMeter.consume(at: stage)
                let typeName = FieldValue.string(entry.typeName)
                let typeCode = FieldValue.int64(entry.typeCode)
                var footprint = try CanonicalRelationalFootprintMeter
                    .sourceRowFootprint(
                        of: model,
                        sourceName: sourceName,
                        workMeter: workMeter,
                        stage: stage
                    )
                footprint = try CanonicalRelationalFootprintMeter.footprint(
                    footprint,
                    appendingAnnotationNamed: PolymorphicRowAnnotation.typeName,
                    value: typeName,
                    workMeter: workMeter
                )
                footprint = try CanonicalRelationalFootprintMeter.footprint(
                    footprint,
                    appendingAnnotationNamed: PolymorphicRowAnnotation.typeCode,
                    value: typeCode,
                    workMeter: workMeter
                )
                try rows.append(footprint: footprint, at: stage) {
                    var fields: [String: FieldValue] = [:]
                    fields.reserveCapacity(model.fields.count)
                    for field in model.fields {
                        fields[field.name] = field.value
                    }
                    let version = try PersistableVersionTokenCodec.token(
                        for: fields
                    )
                    return CanonicalSourceRow.fromBaseFields(
                        fields,
                        sourceName: sourceName,
                        annotations: [
                            PolymorphicRowAnnotation.typeName: typeName,
                            PolymorphicRowAnnotation.typeCode: typeCode,
                        ],
                        version: version
                    )
                }
            }
        }
        return isPresent
    }
}

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum FullTextReadParameter {
    static let fieldName = "fieldName"
    static let terms = "terms"
    static let matchMode = "matchMode"
    static let limit = "limit"
    static let returnScores = "returnScores"
    static let includeFacets = "includeFacets"
    static let bm25K1 = "bm25.k1"
    static let bm25B = "bm25.b"
    static let facetFields = "facetFields"
    static let facetLimit = "facetLimit"
    static let totalCount = "fulltext.totalCount"
    static let facetMetadataPrefix = "fulltext.facets."
}

public enum FullTextReadExecutors {
    public static var polymorphicIndexExecutor: any PolymorphicIndexReadExecutor {
        PolymorphicFullTextReadExecutor()
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(FullTextReadExecutor())
    }
}

private enum FullTextReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case invalidResultCount(field: String, count: Int64)
    case invalidExecutionPath(String)
    case fetchedItemCountMismatch(expected: Int, actual: Int)
    case missingFetchedEntity(ByteString)
}

private func retainedFullTextFacetMetadata(
    totalCount: Int,
    _ facets: [String: [(value: String, count: Int64)]],
    workMeter: DatabaseWorkMeter
) throws -> DatabaseRetainedIndexMetadata {
    guard let totalCount = UInt64(exactly: totalCount) else {
        throw FullTextReadError.invalidResultCount(
            field: FullTextReadParameter.totalCount,
            count: Int64(totalCount)
        )
    }
    var footprint = DatabaseIntermediateFootprint(
        bytes: UInt64(MemoryLayout<[String: FieldValue]>.stride) + 64
    )
    for (field, buckets) in facets {
        footprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                rows: 1,
                bytes: UInt64(
                    FullTextReadParameter.facetMetadataPrefix.utf8.count
                        + field.utf8.count
                ) + 160
            )
        )
        for bucket in buckets {
            footprint = try footprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(bucket.value.utf8.count) + 192
                )
            )
        }
    }
    return try DatabaseRetainedIndexMetadata.build(
        workMeter: workMeter,
        footprint: footprint
    ) {
        var metadata: [String: FieldValue] = [
            FullTextReadParameter.totalCount: .uint64(totalCount)
        ]
        for (field, buckets) in facets {
            metadata[
                FullTextReadParameter.facetMetadataPrefix + field
            ] = .array(
                try buckets.map { bucket in
                    guard let count = UInt64(exactly: bucket.count) else {
                        throw FullTextReadError.invalidResultCount(
                            field: field,
                            count: bucket.count
                        )
                    }
                    return .array([
                        .string(bucket.value),
                        .uint64(count),
                    ])
                }
            )
        }
        return metadata
    }
}

private func reserveFullTextCandidate(
    _ candidate: FullTextPostingCandidate,
    in reservation: DatabaseIntermediateReservation
) throws {
    try reservation.reserveAdditional(
        rows: candidate.retainedFootprint.rows,
        bytes: candidate.retainedFootprint.bytes,
        at: .indexScan
    )
}

private func reserveFullTextFacetEntry(
    _ value: String,
    in reservation: DatabaseIntermediateReservation
) throws {
    try reservation.reserveAdditional(
        rows: 1,
        bytes: UInt64(value.utf8.count) + 64,
        at: .indexScan
    )
}

private struct FullTextReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .text(.fullText)

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        guard indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue == true
        else { return [] }
        return Set(try requireStringArray(
            FullTextReadParameter.facetFields,
            from: indexScan.parameters
        ))
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let requestedLimit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        if limit == 0, !includeFacets {
            return .empty
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        guard index.type == indexType,
            index.fieldNames.contains(fieldName),
              entity.fieldMapByName[fieldName] != nil else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.fieldName
            )
        }
        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: try additionalRequiredFieldNames(
                indexScan: indexScan
            )
        )
        let configuration = try FullTextIndexConfiguration(definition: index.declaration.definition)

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = try optionalInteger(
                FullTextReadParameter.facetLimit,
                from: indexScan.parameters
            ) ?? 10
            return try await executeFacetedSearch(
                session: session,
                entity: entity,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                facetFields: facetFields,
                facetLimit: facetLimit,
                index: index,
                partitions: partitions,
                execution: execution,
                workMeter: options.workMeter
            )
        }

        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            return try await executeScoredSearch(
                session: session,
                entity: entity,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Parameters: BM25Parameters(
                    k1: Float(k1),
                    b: Float(b)
                ),
                index: index,
                partitions: partitions,
                execution: execution,
                workMeter: options.workMeter
            )
        }

        return try await executePlainSearch(
            session: session,
            entity: entity,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            index: index,
            partitions: partitions,
            execution: execution,
            workMeter: options.workMeter
        )
    }

    private func executePlainSearch(
        session: DatabaseReadSession,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        index: IndexDescriptor,
        partitions: FieldObject,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        let search = PolymorphicFullTextReadExecutor()
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            return .empty
        }
        let transaction = session.transaction
        do {
            let identifiers: FullTextRetainedKeys
            if matchMode == .phrase {
                identifiers = try await search.searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter,
                    snapshot: execution.consistency == .snapshot
                )
            } else {
                identifiers = try await search.searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter,
                    snapshot: execution.consistency == .snapshot
                )
            }
            let limited = identifiers.prefix(limit: limit)
            let fetched = try await session.fetchRetainedPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: limited,
                partitions: partitions,
                snapshot: execution.consistency == .snapshot
            )
            try requireModels(
                identifiers: limited,
                fetched: fetched
            )
            return try IndexReadResult.build(
                workMeter: workMeter,
                expectedCount: fetched.count
            ) { rows in
                for index in fetched.indices {
                    try withRequiredModel(
                        at: index,
                        in: fetched
                    ) { model, footprint in
                        try rows.append(footprint: footprint) {
                            try IndexReadRow.materializing(model)
                        }
                    }
                }
            }
        }
    }

    private func executeScoredSearch(
        session: DatabaseReadSession,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        bm25Parameters: BM25Parameters,
        index: IndexDescriptor,
        partitions: FieldObject,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        let search = PolymorphicFullTextReadExecutor()
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            return .empty
        }
        let transaction = session.transaction
        do {
            let scored = try await search.searchWithScores(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                bm25Params: bm25Parameters,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                limit: limit,
                workMeter: workMeter,
                snapshot: execution.consistency == .snapshot
            )
            let limitedScored = scored.prefix(limit: limit)
            let fetched = try await session.fetchRetainedPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: limitedScored,
                partitions: partitions,
                snapshot: execution.consistency == .snapshot
            )
            try requireModels(
                identifiers: limitedScored,
                fetched: fetched
            )
            return try IndexReadResult.build(
                workMeter: workMeter,
                expectedCount: fetched.count
            ) { rows in
                for index in fetched.indices {
                    try withRequiredModel(
                        at: index,
                        in: fetched
                    ) { model, footprint in
                        let score = FieldValue.float64(
                            scored.score(at: index)
                        )
                        let annotationName: StaticString = "score"
                        let annotatedFootprint = try
                            CanonicalRelationalFootprintMeter.footprint(
                                footprint,
                                appendingAnnotationNamed: annotationName,
                                value: score,
                                workMeter: workMeter
                            )
                        try rows.append(footprint: annotatedFootprint) {
                            try IndexReadRow.materializing(
                                model,
                                annotations: [
                                    "score": score
                                ]
                            )
                        }
                    }
                }
            }
        }
    }

    private func executeFacetedSearch(
        session: DatabaseReadSession,
        entity: Schema.Entity,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        facetFields: [String],
        facetLimit: Int,
        index: IndexDescriptor,
        partitions: FieldObject,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        let search = PolymorphicFullTextReadExecutor()
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            let metadata = try retainedFullTextFacetMetadata(
                totalCount: 0,
                [:],
                workMeter: workMeter
            )
            return try IndexReadResult.build(
                workMeter: workMeter,
                metadata: consume metadata,
                expectedCount: 0
            ) { _ in }
        }
        let transaction = session.transaction
        do {
            let identifiers: FullTextRetainedKeys
            if matchMode == .phrase {
                identifiers = try await search.searchPhrase(
                    configuration: configuration,
                    terms: terms,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter,
                    snapshot: execution.consistency == .snapshot
                )
            } else {
                identifiers = try await search.searchFullText(
                    terms: terms,
                    matchMode: matchMode,
                    configuration: configuration,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction,
                    workMeter: workMeter,
                    snapshot: execution.consistency == .snapshot
                )
            }
            let fetched = try await session.fetchRetainedPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: identifiers,
                partitions: partitions,
                snapshot: execution.consistency == .snapshot
            )
            try requireModels(
                identifiers: identifiers,
                fetched: fetched
            )
            let allRows = try buildPersistedRows(
                fetched: fetched,
                count: fetched.count,
                workMeter: workMeter
            )
            let metadata = try collectFacetMetadata(
                from: allRows,
                fields: facetFields,
                entity: entity,
                facetLimit: facetLimit,
                totalCount: fetched.count,
                workMeter: workMeter
            )
            let visibleCount = min(limit ?? fetched.count, fetched.count)
            let visibleRows = try IndexReadResult.build(
                workMeter: workMeter,
                expectedCount: visibleCount
            ) { rows in
                for index in 0..<visibleCount {
                    try withRequiredModel(
                        at: index,
                        in: fetched
                    ) { model, footprint in
                        try rows.append(footprint: footprint) {
                            try IndexReadRow.materializing(model)
                        }
                    }
                }
            }
            return try IndexReadResult.attachingMetadata(
                to: consume visibleRows,
                metadata: consume metadata
            )
        }
    }

    private func requireModels<PrimaryKeys: DatabaseRetainedPrimaryKeyCollection>(
        identifiers: PrimaryKeys,
        fetched: DatabaseRetainedPersistedModels
    ) throws {
        guard identifiers.count == fetched.count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: fetched.count
            )
        }
        for index in 0..<fetched.count {
            guard case .some = fetched[index] else {
                let identifier = packedIdentifier(
                    from: identifiers,
                    at: index
                )
                throw FullTextReadError.missingFetchedEntity(
                    identifier
                )
            }
        }
    }

    private func packedIdentifier<PrimaryKeys: DatabaseRetainedPrimaryKeyCollection>(
        from identifiers: PrimaryKeys,
        at index: Int
    ) -> ByteString {
        var packed = ByteString()
        identifiers.withRetainedPrimaryKey(at: index) { identifier in
            packed = identifier.pack()
        }
        return packed
    }

    private func withRequiredModel<Failure: Error>(
        at index: Int,
        in fetched: DatabaseRetainedPersistedModels,
        _ body: (
            borrowing PersistedModel,
            DatabaseIntermediateFootprint
        ) throws(Failure) -> Void
    ) throws(Failure) {
        guard let retained = fetched[index] else {
            preconditionFailure(
                "Validated full-text result contains a missing model"
            )
        }
        try retained.withModel {
            (model: borrowing PersistedModel) throws(Failure) in
            try body(model, retained.queryRowFootprint)
        }
    }

    private func buildPersistedRows(
        fetched: DatabaseRetainedPersistedModels,
        count: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> IndexReadResult {
        precondition(count >= 0 && count <= fetched.count)
        return try IndexReadResult.build(
            workMeter: workMeter,
            expectedCount: count
        ) { rows in
            for index in 0..<count {
                try withRequiredModel(
                    at: index,
                    in: fetched
                ) { model, footprint in
                    try rows.append(footprint: footprint) {
                        try IndexReadRow.materializing(model)
                    }
                }
            }
        }
    }

    private func collectFacetMetadata(
        from result: IndexReadResult,
        fields facetFields: [String],
        entity: Schema.Entity,
        facetLimit: Int,
        totalCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedIndexMetadata {
        var facets: [String: [(value: String, count: Int64)]] = [:]
        let outputReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[
                    String: [(value: String, count: Int64)]
                ]>.stride
            ),
            at: .indexScan
        )
        defer { outputReservation.release() }

        for fieldName in facetFields {
            guard let field = entity.fieldMapByName[fieldName] else {
                throw FullTextReadError.invalidParameter(fieldName)
            }
            var counts: [String: Int64] = [:]
            let countsReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
                at: .indexScan
            )
            defer { countsReservation.release() }

            for index in 0..<result.count {
                try workMeter.consume(at: .indexScan)
                try result.withRow(at: index) { row in
                    guard let value = row.fields[fieldName] else {
                        throw FullTextFieldValueError.missingField(
                            entity: entity.name,
                            field: FieldIdentity(
                                name: field.name,
                                number: field.fieldNumber
                            )
                        )
                    }
                    for string in try FullTextFieldValueExtractor.strings(
                        from: value,
                        entity: entity.name,
                        field: FieldIdentity(
                            name: field.name,
                            number: field.fieldNumber
                        )
                    ) where !string.isEmpty {
                        if counts[string] == nil {
                            try reserveFullTextFacetEntry(
                                string,
                                in: countsReservation
                            )
                        }
                        counts[string, default: 0] += 1
                    }
                }
            }

            let bucketScratch = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[(value: String, count: Int64)]>.stride
                ),
                at: .indexScan
            )
            defer { bucketScratch.release() }
            for value in counts.keys {
                try reserveFullTextFacetEntry(value, in: bucketScratch)
            }
            var buckets = counts.map { (value: $0.key, count: $0.value) }
            buckets.sort {
                $0.count == $1.count
                    ? $0.value < $1.value
                    : $0.count > $1.count
            }
            try outputReservation.reserveAdditional(
                bytes: UInt64(fieldName.utf8.count) + 48,
                at: .indexScan
            )
            for bucket in buckets.prefix(facetLimit) {
                try reserveFullTextFacetEntry(
                    bucket.value,
                    in: outputReservation
                )
            }
            facets[fieldName] = Array(buckets.prefix(facetLimit))
        }
        return try retainedFullTextFacetMetadata(
            totalCount: totalCount,
            facets,
            workMeter: workMeter
        )
    }

    private func decodeMatchMode(
        from parameters: [String: FieldValue]
    ) throws -> TextMatchMode {
        let rawValue = try requireString(FullTextReadParameter.matchMode, from: parameters)
        switch rawValue {
        case "all":
            return .all
        case "any":
            return .any
        case "phrase":
            return .phrase
        default:
            throw FullTextReadError.invalidParameter(FullTextReadParameter.matchMode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func optionalInteger(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> Int? {
        guard let value = parameters[name] else {
            return nil
        }
        guard let integer = value.int64Value,
              let result = Int(exactly: integer) else {
            throw FullTextReadError.invalidParameter(name)
        }
        return result
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [String] {
        guard let values = parameters[key]?.arrayValue else {
            throw FullTextReadError.missingParameter(key)
        }

        var strings: [String] = []
        strings.reserveCapacity(values.count)
        for value in values {
            guard let string = value.stringValue else {
                throw FullTextReadError.invalidParameter(key)
            }
            strings.append(string)
        }
        return strings
    }
}

private struct PolymorphicFullTextReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .text(.fullText)

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        guard indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue == true
        else { return [] }
        return Set(try requireStringArray(
            FullTextReadParameter.facetFields,
            from: indexScan.parameters
        ))
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let fieldName = try requireString(FullTextReadParameter.fieldName, from: indexScan.parameters)
        let terms = try requireStringArray(FullTextReadParameter.terms, from: indexScan.parameters)
        let matchMode = try decodeMatchMode(from: indexScan.parameters)
        let requestedLimit = try optionalInteger(
            FullTextReadParameter.limit,
            from: indexScan.parameters
        )
        guard requestedLimit.map({ $0 >= 0 }) ?? true else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.limit
            )
        }
        let budgetLimit = try options.workMeter.storageReadLimitWithSentinel()
        let limit = min(requestedLimit ?? budgetLimit, budgetLimit)
        let includeFacets = indexScan.parameters[FullTextReadParameter.includeFacets]?.boolValue ?? false
        let returnScores = indexScan.parameters[FullTextReadParameter.returnScores]?.boolValue ?? false

        if limit == 0, !includeFacets {
            return .empty
        }

        let execution = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .snapshot
        )
        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: try additionalRequiredFieldNames(
                indexScan: indexScan
            )
        )

        guard index.type == indexType,
            index.fieldNames.contains(fieldName) else {
            throw FullTextReadError.invalidParameter(index.name)
        }
        let configuration = try FullTextIndexConfiguration(
            definition: index.definition
        )

        if includeFacets {
            let facetFields = try requireStringArray(FullTextReadParameter.facetFields, from: indexScan.parameters)
            let facetLimit = try optionalInteger(
                FullTextReadParameter.facetLimit,
                from: indexScan.parameters
            ) ?? 10
            return try await executeFacetedSearch(
                session: session,
                group: group,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                facetFields: facetFields,
                facetLimit: facetLimit,
                index: index,
                execution: execution,
                workMeter: options.workMeter
            )
        }

        if returnScores {
            let k1 = indexScan.parameters[
                FullTextReadParameter.bm25K1
            ]?.float64Value ?? Double(BM25Parameters.default.k1)
            let b = indexScan.parameters[
                FullTextReadParameter.bm25B
            ]?.float64Value ?? Double(BM25Parameters.default.b)
            return try await executeScoredSearch(
                session: session,
                group: group,
                configuration: configuration,
                terms: terms,
                matchMode: matchMode,
                limit: limit,
                bm25Params: BM25Parameters(k1: Float(k1), b: Float(b)),
                index: index,
                execution: execution,
                workMeter: options.workMeter
            )
        }

        return try await executePlainSearch(
            session: session,
            group: group,
            configuration: configuration,
            terms: terms,
            matchMode: matchMode,
            limit: limit,
            index: index,
            execution: execution,
            workMeter: options.workMeter
        )
    }

    private func executePlainSearch(
        session: DatabaseReadSession,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            return .empty
        }
        let transaction = session.transaction
        let identifiers: FullTextRetainedKeys
        if matchMode == .phrase {
            identifiers = try await searchPhrase(
                configuration: configuration,
                terms: terms,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: execution.consistency == .snapshot
            )
        } else {
            identifiers = try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: execution.consistency == .snapshot
            )
        }
        let limited = identifiers.prefix(limit: limit)
        let fetched = try await session.fetchRetainedPolymorphicItemsPreservingOrder(
            group: group,
            ids: limited,
            snapshot: execution.consistency == .snapshot
        )
        return try buildPolymorphicRows(
            entities: fetched,
            identifiers: limited,
            count: fetched.count,
            workMeter: workMeter
        )
    }

    private func executeScoredSearch(
        session: DatabaseReadSession,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        bm25Params: BM25Parameters,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            return .empty
        }
        let transaction = session.transaction
        let scored = try await searchWithScores(
            terms: terms,
            matchMode: matchMode,
            configuration: configuration,
            bm25Params: bm25Params,
            indexSubspace: readableIndex.subspace,
            transaction: transaction,
            limit: limit,
            workMeter: workMeter,
            snapshot: execution.consistency == .snapshot
        )
        let fetched = try await session.fetchRetainedPolymorphicItemsPreservingOrder(
            group: group,
            ids: scored,
            snapshot: execution.consistency == .snapshot
        )
        return try buildPolymorphicRows(
            entities: fetched,
            identifiers: scored,
            count: fetched.count,
            workMeter: workMeter,
            scoreAt: { scored.score(at: $0) }
        )
    }

    private func executeFacetedSearch(
        session: DatabaseReadSession,
        group: PolymorphicGroup,
        configuration: FullTextIndexConfiguration,
        terms: [String],
        matchMode: TextMatchMode,
        limit: Int?,
        facetFields: [String],
        facetLimit: Int,
        index: IndexDeclaration<String>,
        execution: CanonicalReadExecution,
        workMeter: DatabaseWorkMeter
    ) async throws -> IndexReadResult {
        guard facetLimit >= 0 else {
            throw FullTextReadError.invalidParameter(
                FullTextReadParameter.facetLimit
            )
        }
        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            let metadata = try retainedFullTextFacetMetadata(
                totalCount: 0,
                [:],
                workMeter: workMeter
            )
            return try IndexReadResult.build(
                workMeter: workMeter,
                metadata: consume metadata
            ) { _ in }
        }
        let transaction = session.transaction
        let identifiers: FullTextRetainedKeys
        if matchMode == .phrase {
            identifiers = try await searchPhrase(
                configuration: configuration,
                terms: terms,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: execution.consistency == .snapshot
            )
        } else {
            identifiers = try await searchFullText(
                terms: terms,
                matchMode: matchMode,
                configuration: configuration,
                indexSubspace: readableIndex.subspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: execution.consistency == .snapshot
            )
        }
        let fetched = try await session.fetchRetainedPolymorphicItemsPreservingOrder(
            group: group,
            ids: identifiers,
            snapshot: execution.consistency == .snapshot
        )
        let allRows = try buildPolymorphicRows(
            entities: fetched,
            identifiers: identifiers,
            count: fetched.count,
            workMeter: workMeter
        )
        let metadata = try collectPolymorphicFacetMetadata(
            from: allRows,
            fields: facetFields,
            facetLimit: facetLimit,
            totalCount: fetched.count,
            workMeter: workMeter
        )
        let visibleCount = min(limit ?? fetched.count, fetched.count)
        let visibleRows = try IndexReadResult.build(
            workMeter: workMeter,
            expectedCount: visibleCount
        ) { rows in
            for index in 0..<visibleCount {
                _ = try fetched.appendIndexRow(
                    at: index,
                    to: &rows
                )
            }
        }
        return try IndexReadResult.attachingMetadata(
            to: consume visibleRows,
            metadata: consume metadata
        )
    }

    private func buildPolymorphicRows<PrimaryKeys>(
        entities: borrowing DatabaseRetainedPolymorphicEntities,
        identifiers: PrimaryKeys,
        count: Int,
        workMeter: DatabaseWorkMeter,
        scoreAt: ((Int) -> Double)? = nil
    ) throws -> IndexReadResult
    where PrimaryKeys: DatabaseRetainedPrimaryKeyCollection {
        precondition(count >= 0 && count <= entities.count)
        guard identifiers.count == count else {
            throw FullTextReadError.fetchedItemCountMismatch(
                expected: identifiers.count,
                actual: entities.count
            )
        }
        return try IndexReadResult.build(
            workMeter: workMeter,
            expectedCount: count
        ) { rows in
            for index in 0..<count {
                let annotation: (name: String, value: FieldValue)?
                if let scoreAt {
                    annotation = (
                        name: "score",
                        value: .float64(scoreAt(index))
                    )
                } else {
                    annotation = nil
                }
                guard try entities.appendIndexRow(
                    at: index,
                    to: &rows,
                    additionalAnnotation: annotation
                ) else {
                    throw FullTextReadError.missingFetchedEntity(
                        packedIdentifier(
                            from: identifiers,
                            at: index
                        )
                    )
                }
            }
        }
    }

    private func packedIdentifier<PrimaryKeys: DatabaseRetainedPrimaryKeyCollection>(
        from identifiers: PrimaryKeys,
        at index: Int
    ) -> ByteString {
        var packed = ByteString()
        identifiers.withRetainedPrimaryKey(at: index) { identifier in
            packed = identifier.pack()
        }
        return packed
    }

    private func collectPolymorphicFacetMetadata(
        from result: IndexReadResult,
        fields facetFields: [String],
        facetLimit: Int,
        totalCount: Int,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedIndexMetadata {
        var facets: [String: [(value: String, count: Int64)]] = [:]
        let outputReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[
                    String: [(value: String, count: Int64)]
                ]>.stride
            ),
            at: .indexScan
        )
        defer { outputReservation.release() }

        for fieldName in facetFields {
            var counts: [String: Int64] = [:]
            let countsReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
                at: .indexScan
            )
            defer { countsReservation.release() }

            for index in 0..<result.count {
                try workMeter.consume(at: .indexScan)
                try result.withRow(at: index) { row in
                    guard let value = row.fields[fieldName] else {
                        throw FullTextReadError.invalidParameter(fieldName)
                    }
                    for string in try FullTextFieldValueExtractor.strings(
                        from: value,
                        entity: "polymorphic",
                        field: FieldIdentity(name: fieldName, number: 0)
                    ) where !string.isEmpty {
                        if counts[string] == nil {
                            try reserveFullTextFacetEntry(
                                string,
                                in: countsReservation
                            )
                        }
                        counts[string, default: 0] += 1
                    }
                }
            }

            let bucketScratch = try workMeter.reserveIntermediate(
                bytes: UInt64(
                    MemoryLayout<[(value: String, count: Int64)]>.stride
                ),
                at: .indexScan
            )
            defer { bucketScratch.release() }
            for value in counts.keys {
                try reserveFullTextFacetEntry(value, in: bucketScratch)
            }
            var buckets = counts.map { (value: $0.key, count: $0.value) }
            buckets.sort {
                $0.count == $1.count
                    ? $0.value < $1.value
                    : $0.count > $1.count
            }
            try outputReservation.reserveAdditional(
                bytes: UInt64(fieldName.utf8.count) + 48,
                at: .indexScan
            )
            for bucket in buckets.prefix(facetLimit) {
                try reserveFullTextFacetEntry(
                    bucket.value,
                    in: outputReservation
                )
            }
            facets[fieldName] = Array(buckets.prefix(facetLimit))
        }
        return try retainedFullTextFacetMetadata(
            totalCount: totalCount,
            facets,
            workMeter: workMeter
        )
    }

    fileprivate func searchPhrase(
        configuration: FullTextIndexConfiguration,
        terms: [String],
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        snapshot: Bool = true
    ) async throws -> FullTextRetainedKeys {
        guard configuration.storePositions else {
            throw FullTextIndexError.invalidQuery(
                "Phrase search requires storePositions=true"
            )
        }
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        let normalizedTerms = normalizer
            .normalizedTerms(from: terms.joined(separator: " "))
        guard !normalizedTerms.isEmpty else {
            let empty = try FullTextRetainedKeys.Builder(
                workMeter: workMeter
            )
            return try empty.finish()
        }

        let termsSubspace = FullTextStorageLayout.terms(in: indexSubspace)
        let candidates = try await searchTermsAND(
            normalizedTerms,
            termsSubspace: termsSubspace,
            transaction: transaction,
            workMeter: workMeter,
            snapshot: snapshot
        )
        defer { candidates.release() }
        var retainedMatches = try FullTextRetainedKeys.Builder(
            workMeter: workMeter,
            expectedCount: candidates.count
        )
        for candidate in candidates.values {
            try workMeter.consume(at: .indexScan)
            let identifier = candidate.identifier
            var positionsByTerm: [[Int]] = []
            positionsByTerm.reserveCapacity(normalizedTerms.count)
            let positionReservation = try workMeter.reserveIntermediate(
                bytes: UInt64(MemoryLayout<[[Int]]>.stride),
                at: .indexScan
            )
            defer { positionReservation.release() }
            for term in normalizedTerms {
                try workMeter.consume(at: .indexScan)
                let key = termsSubspace.subspace(term).pack(identifier)
                guard let value = try await transaction.readPointValue(
                    for: key,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    at: .indexScan
                ) else {
                    try positionReservation.reserveAdditional(
                        rows: 1,
                        bytes: UInt64(MemoryLayout<[Int]>.stride) + 16,
                        at: .indexScan
                    )
                    positionsByTerm.append([])
                    continue
                }
                let admittedPositionBytes = try fullTextPositionByteCount(
                    maximumPositionCount: value.count
                )
                try positionReservation.reserveAdditional(
                    rows: 1,
                    bytes: admittedPositionBytes,
                    at: .indexScan
                )
                var positions: [Int] = []
                positions.reserveCapacity(value.count)
                _ = try FullTextStorageDecoder.postingPositions(
                    from: value,
                    term: term,
                    into: &positions,
                    workMeter: workMeter
                )
                let retainedPositionBytes = try fullTextPositionByteCount(
                    maximumPositionCount: positions.count
                )
                positionReservation.releaseGuaranteedPartial(
                    bytes: admittedPositionBytes - retainedPositionBytes
                )
                positionsByTerm.append(positions)
            }
            if try containsConsecutivePositions(
                positionsByTerm,
                workMeter: workMeter
            ) {
                try retainedMatches.append(
                    identifier,
                    packedByteCount: candidate.canonicalKey.count
                )
            }
        }
        return try retainedMatches.finish()
    }

    fileprivate func searchWithScores(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        bm25Params: BM25Parameters,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        limit: Int?,
        workMeter: DatabaseWorkMeter,
        snapshot: Bool = true
    ) async throws -> FullTextRetainedScoredKeys {
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })
        let matchingIdentifiers: FullTextRetainedKeys
        switch matchMode {
        case .all:
            matchingIdentifiers = try await searchFullText(
                terms: terms,
                matchMode: .all,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: snapshot
            )
        case .any:
            matchingIdentifiers = try await searchFullText(
                terms: terms,
                matchMode: .any,
                configuration: configuration,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: snapshot
            )
        case .phrase:
            matchingIdentifiers = try await searchPhrase(
                configuration: configuration,
                terms: terms,
                indexSubspace: indexSubspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: snapshot
            )
        }
        guard matchingIdentifiers.count > 0 else {
            return try FullTextRetainedScoredKeys(
                matches: [],
                workMeter: workMeter
            )
        }

        let totalDocuments = try await statistic(
            key: FullTextStorageLayout.documentCountKey(in: indexSubspace),
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        let totalLength = try await statistic(
            key: FullTextStorageLayout.totalDocumentLengthKey(
                in: indexSubspace
            ),
            transaction: transaction,
            snapshot: snapshot,
            workMeter: workMeter
        )
        guard totalDocuments > 0, totalLength > 0 else {
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        let scorer = BM25Scorer(
            params: bm25Params,
            statistics: BM25Statistics(
                totalDocuments: totalDocuments,
                totalLength: totalLength
            )
        )

        let documentFrequencySubspace = FullTextStorageLayout
            .documentFrequencies(in: indexSubspace)
        var frequencies: [String: Int64] = [:]
        frequencies.reserveCapacity(normalizedTerms.count)
        let frequencyReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(MemoryLayout<[String: Int64]>.stride),
            at: .indexScan
        )
        defer { frequencyReservation.release() }
        for term in normalizedTerms {
            try reserveFullTextFacetEntry(
                term,
                in: frequencyReservation
            )
            frequencies[term] = try await statistic(
                key: documentFrequencySubspace.pack(Tuple(term)),
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
        }

        let termsSubspace = FullTextStorageLayout.terms(in: indexSubspace)
        let documentsSubspace = FullTextStorageLayout.documents(
            in: indexSubspace
        )
        var scored: [(id: Tuple, score: Double)] = []
        scored.reserveCapacity(matchingIdentifiers.count)
        let scoredBuildReservation = try workMeter.reserveIntermediate(
            bytes: UInt64(
                MemoryLayout<[(id: Tuple, score: Double)]>.stride
            ),
            at: .indexScan
        )
        defer { scoredBuildReservation.release() }
        for index in 0..<matchingIdentifiers.count {
            try await matchingIdentifiers.withRetainedPrimaryKey(at: index) {
                identifier in
                try workMeter.consume(at: .indexScan)
                let metadataKey = documentsSubspace.pack(identifier)
                guard let metadataValue = try await transaction.readPointValue(
                    for: metadataKey,
                    snapshot: snapshot,
                    workMeter: workMeter,
                    at: .indexScan
                ) else {
                    throw FullTextStorageError.missingDocumentMetadata
                }
                let metadata = try FullTextStorageDecoder.documentMetadata(
                    from: metadataValue
                )
                var termFrequencies: [String: Int] = [:]
                let termFrequencyReservation =
                    try workMeter.reserveIntermediate(
                        bytes: UInt64(MemoryLayout<[String: Int]>.stride),
                        at: .indexScan
                    )
                defer { termFrequencyReservation.release() }
                for term in normalizedTerms {
                    let postingKey = termsSubspace.subspace(term).pack(
                        identifier
                    )
                    guard let postingValue =
                        try await transaction.readPointValue(
                            for: postingKey,
                            snapshot: snapshot,
                            workMeter: workMeter,
                            at: .indexScan
                        )
                    else {
                        continue
                    }
                    try reserveFullTextFacetEntry(
                        term,
                        in: termFrequencyReservation
                    )
                    termFrequencies[term] = try FullTextStorageDecoder
                        .postingFrequency(
                            from: postingValue,
                            positionsStored: configuration.storePositions,
                            term: term,
                            workMeter: workMeter
                        )
                }
                try scoredBuildReservation.reserveAdditional(
                    rows: 1,
                    bytes: UInt64(identifier.pack().count) + 80,
                    at: .indexScan
                )
                scored.append(
                    (
                        id: identifier,
                        score: scorer.score(
                            termFrequencies: termFrequencies,
                            documentFrequencies: frequencies,
                            docLength: Int(metadata.docLength)
                        )
                    )
                )
            }
        }
        let sortScratch = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(
                        1,
                        MemoryLayout<(id: Tuple, score: Double)>.stride
                    )
                )
            ).multiplied(by: UInt64(scored.count)).bytes,
            at: .indexScan
        )
        defer { sortScratch.release() }
        scored.sort {
            if $0.score == $1.score {
                return stableKey($0.id).lexicographicallyPrecedes(
                    stableKey($1.id)
                )
            }
            return $0.score > $1.score
        }
        if let limit {
            guard limit >= 0 else {
                throw FullTextReadError.invalidParameter(
                    FullTextReadParameter.limit
                )
            }
            if scored.count > limit {
                scored.removeLast(scored.count - limit)
            }
        }
        return try FullTextRetainedScoredKeys(
            matches: scored,
            workMeter: workMeter
        )
    }

    private func fullTextPositionByteCount(
        maximumPositionCount: Int
    ) throws -> UInt64 {
        precondition(maximumPositionCount >= 0)
        return try DatabaseIntermediateFootprint(
            bytes: UInt64(max(1, MemoryLayout<Int>.stride))
        ).multiplied(by: UInt64(maximumPositionCount)).adding(
            DatabaseIntermediateFootprint(bytes: 32)
        ).bytes
    }

    private func statistic(
        key: ByteString,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter
    ) async throws -> Int64 {
        guard let value = try await transaction.readPointValue(
            for: key,
            snapshot: snapshot,
            workMeter: workMeter,
            at: .indexScan
        ) else {
            return 0
        }
        return try ByteConversion.bytesToInt64(value)
    }

    private func containsConsecutivePositions(
        _ positionsByTerm: [[Int]],
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        guard let first = positionsByTerm.first, !first.isEmpty else {
            return false
        }
        for start in first {
            try workMeter.consume(at: .indexScan)
            var matches = true
            for (offset, positions) in positionsByTerm.enumerated() {
                try workMeter.consume(at: .indexScan)
                if !positions.contains(start + offset) {
                    matches = false
                    break
                }
            }
            if matches {
                return true
            }
        }
        return false
    }

    fileprivate func searchFullText(
        terms: [String],
        matchMode: TextMatchMode,
        configuration: FullTextIndexConfiguration,
        indexSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        snapshot: Bool = true
    ) async throws -> FullTextRetainedKeys {
        let termsSubspace = FullTextStorageLayout.terms(in: indexSubspace)
        let termGroups = normalizeQueryTermGroups(
            terms,
            configuration: configuration
        )
        let normalizedTerms = uniqueTerms(termGroups.flatMap { $0 })

        let matchingBatch: FullTextCandidateBatch
        switch matchMode {
        case .all:
            matchingBatch = try await searchTermsAND(
                normalizedTerms,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: snapshot
            )
        case .any:
            var union: [FullTextPostingCandidate] = []
            var unionReservation: DatabaseIntermediateReservation?
            defer { unionReservation?.release() }
            for group in termGroups {
                let batch = try await searchTermsAND(
                    group,
                    termsSubspace: termsSubspace,
                    transaction: transaction,
                    workMeter: workMeter,
                    snapshot: snapshot
                )
                let matches = batch.values
                let matchReservation = batch.reservation
                var matchReleased = false
                defer {
                    if !matchReleased {
                        matchReservation.release()
                    }
                }
                guard !matches.isEmpty else {
                    matchReservation.release()
                    matchReleased = true
                    continue
                }
                guard !union.isEmpty else {
                    union = matches
                    unionReservation = matchReservation
                    matchReleased = true
                    continue
                }

                let mergedReservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[FullTextPostingCandidate]>.stride
                    ),
                    at: .indexScan
                )
                do {
                    let merged = try FullTextPostingListAlgebra.union(
                        union,
                        matches,
                        reservingCapacity: false
                    ) { candidate in
                        try reserveFullTextCandidate(
                            candidate,
                            in: mergedReservation
                        )
                    }
                    unionReservation?.release()
                    matchReservation.release()
                    matchReleased = true
                    union = merged
                    unionReservation = mergedReservation
                } catch {
                    mergedReservation.release()
                    throw error
                }
            }
            let reservation: DatabaseIntermediateReservation
            if let unionReservation {
                reservation = unionReservation
            } else {
                reservation = try workMeter.reserveIntermediate(
                    at: .indexScan
                )
            }
            unionReservation = nil
            matchingBatch = FullTextCandidateBatch(
                values: union,
                reservation: reservation
            )
        case .phrase:
            throw FullTextReadError.invalidExecutionPath(
                "Phrase matching must use the position-aware search path"
            )
        }

        defer { matchingBatch.release() }
        var retainedKeys = try FullTextRetainedKeys.Builder(
            workMeter: workMeter,
            expectedCount: matchingBatch.count
        )
        for candidate in matchingBatch.values {
            try retainedKeys.append(
                candidate.identifier,
                packedByteCount: candidate.canonicalKey.count
            )
        }
        return try retainedKeys.finish()
    }

    private func normalizeQueryTermGroups(
        _ terms: [String],
        configuration: FullTextIndexConfiguration
    ) -> [[String]] {
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        return terms.map { term in
            uniqueTerms(normalizer.normalizedTerms(from: term))
        }
        .filter { !$0.isEmpty }
    }

    private func uniqueTerms(_ terms: [String]) -> [String] {
        var seen: Set<String> = []
        var result: [String] = []
        result.reserveCapacity(terms.count)
        for term in terms where !seen.contains(term) {
            seen.insert(term)
            result.append(term)
        }
        return result
    }

    /// Scored results do not retain the posting candidate's packed suffix.
    /// Their deterministic tie ordering therefore encodes the final Tuple at
    /// this separate scoring boundary; posting merges compare candidates
    /// directly and never call this helper.
    private func stableKey(_ tuple: Tuple) -> ByteString {
        tuple.pack()
    }

    private func searchTermsAND(
        _ terms: [String],
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        snapshot: Bool
    ) async throws -> FullTextCandidateBatch {
        guard !terms.isEmpty else {
            return FullTextCandidateBatch(
                values: [],
                reservation: try workMeter.reserveIntermediate(
                    at: .indexScan
                )
            )
        }

        var intersection: [FullTextPostingCandidate]?
        var intersectionReservation: DatabaseIntermediateReservation?
        defer { intersectionReservation?.release() }

        for term in terms {
            let results = try await searchTerm(
                term,
                termsSubspace: termsSubspace,
                transaction: transaction,
                workMeter: workMeter,
                snapshot: snapshot
            )
            let resultReservation = results.reservation
            let resultValues = results.values
            var resultTransferred = false
            defer {
                if !resultTransferred {
                    resultReservation.release()
                }
            }
            if let existing = intersection {
                let reducedReservation = try workMeter.reserveIntermediate(
                    bytes: UInt64(
                        MemoryLayout<[FullTextPostingCandidate]>.stride
                    ),
                    at: .indexScan
                )
                var transferredReducedReservation = false
                defer {
                    if !transferredReducedReservation {
                        reducedReservation.release()
                    }
                }
                let reduced = try FullTextPostingListAlgebra.intersection(
                    existing,
                    resultValues,
                    reservingCapacity: false
                ) { candidate in
                    try reserveFullTextCandidate(
                        candidate,
                        in: reducedReservation
                    )
                }
                intersectionReservation?.release()
                intersectionReservation = nil
                if reduced.isEmpty {
                    return FullTextCandidateBatch(
                        values: [],
                        reservation: try workMeter.reserveIntermediate(
                            at: .indexScan
                        )
                    )
                }
                intersection = reduced
                intersectionReservation = reducedReservation
                transferredReducedReservation = true
            } else {
                intersection = resultValues
                intersectionReservation = resultReservation
                resultTransferred = true
            }
        }

        guard let intersection else {
            return FullTextCandidateBatch(
                values: [],
                reservation: try workMeter.reserveIntermediate(
                    at: .indexScan
                )
            )
        }
        guard let reservation = intersectionReservation else {
            return FullTextCandidateBatch(
                values: intersection,
                reservation: try workMeter.reserveIntermediate(
                    at: .indexScan
                )
            )
        }
        intersectionReservation = nil
        return FullTextCandidateBatch(
            values: intersection,
            reservation: reservation
        )
    }

    private func searchTerm(
        _ term: String,
        termsSubspace: Subspace,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter,
        snapshot: Bool
    ) async throws -> FullTextCandidateBatch {
        let termSubspace = termsSubspace.subspace(term)
        let (begin, end) = termSubspace.range()
        let scanLimit = try workMeter.storageReadLimitWithSentinel()
        var cursor = transaction.rangeCursor(
            from: .firstGreaterOrEqual(begin),
            to: .firstGreaterOrEqual(end),
            limit: scanLimit,
            reverse: false,
            snapshot: snapshot,
            streamingMode: .wantAll
        )

        var results = try FullTextCandidateBatch(workMeter: workMeter)
        do {
            while let (key, _) = try await cursor.next() {
                guard termSubspace.contains(key) else { break }
                try workMeter.consume(at: .indexScan)
                let suffix = key[
                    (key.startIndex + termSubspace.prefix.count)..<key.endIndex
                ]
                try results.append(scannedSuffix: suffix)
            }
        } catch let cleanupError as StorageRangeCleanupError {
            results.release()
            throw cleanupError
        } catch let terminalCleanupError as StorageRangeTerminalCleanupError {
            results.release()
            throw terminalCleanupError
        } catch {
            let iterationError = error
            do {
                try await cursor.finish()
            } catch {
                results.release()
                throw StorageRangeCleanupError(
                    iterationError: iterationError,
                    cleanupError: error
                )
            }
            results.release()
            throw iterationError
        }
        do {
            try await cursor.finish()
        } catch {
            results.release()
            throw error
        }
        return results
    }

    private func optionalInteger(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> Int? {
        guard let value = parameters[name] else {
            return nil
        }
        guard let integer = value.int64Value,
              let result = Int(exactly: integer) else {
            throw FullTextReadError.invalidParameter(name)
        }
        return result
    }

    private func decodeMatchMode(
        from parameters: [String: FieldValue]
    ) throws -> TextMatchMode {
        let rawValue = try requireString(FullTextReadParameter.matchMode, from: parameters)
        switch rawValue {
        case "all":
            return .all
        case "any":
            return .any
        case "phrase":
            return .phrase
        default:
            throw FullTextReadError.invalidParameter(FullTextReadParameter.matchMode)
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw FullTextReadError.missingParameter(key)
        }
        return value
    }

    private func requireStringArray(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> [String] {
        guard let values = parameters[key]?.arrayValue else {
            throw FullTextReadError.missingParameter(key)
        }

        var strings: [String] = []
        strings.reserveCapacity(values.count)
        for value in values {
            guard let string = value.stringValue else {
                throw FullTextReadError.invalidParameter(key)
            }
            strings.append(string)
        }
        return strings
    }
}

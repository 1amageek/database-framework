import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit
import Synchronization

package struct FullTextFusionIndexReadExecutor: FusionIndexReadExecutor {
    package let indexType: IndexType = .text(.fullText)

    package init() {}

    package func validate(
        _ request: FusionIndexValidationRequest
    ) throws {
        try validateParameterNames(request.source.parameters)
        let fieldName = try string(
            FullTextReadParameter.fieldName,
            from: request.source.parameters
        )
        _ = try stringValues(
            FullTextReadParameter.terms,
            from: request.source.parameters
        )
        let mode = try matchMode(from: request.source.parameters)
        guard request.descriptor.type == indexType,
              request.descriptor.fieldNames == [fieldName],
              request.source.referencedFields.map(\.name) == [fieldName] else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: fieldName
            )
        }
        guard request.scoring == .annotation(
            name: "score",
            order: .higherIsBetter
        ) else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: "scoring"
            )
        }
        let configuration: FullTextIndexConfiguration
        do {
            configuration = try FullTextIndexConfiguration(
                definition: request.descriptor.declaration.definition
            )
        } catch {
            throw FusionExecutionError.executionContractViolation
        }
        if case .phrase = mode, !configuration.storePositions {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: FullTextReadParameter.matchMode
            )
        }
        _ = try parameters(from: request.source.parameters)
    }

    package func executeUnrestricted(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            return try await executeUnrestrictedImpl(request, output: output)
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    private func executeUnrestrictedImpl(
        _ request: FusionIndexReadRequest,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        guard request.limit > 0 else { return .satisfiedLimit }
        let prepared = try prepare(request)
        guard !prepared.uniqueTerms.isEmpty else { return .exhausted }
        var topK = try FullTextFusionTopK(
            limit: request.limit,
            workMeter: request.workMeter
        )
        var statistics: CorpusStatistics?
        let cursorLayout = try DatabaseRetainedArrayLayout.forElement(
            FullTextFusionPostingCursor.self
        )
        let cursorGrowth = try cursorLayout.growth(
            from: 0,
            toFit: prepared.uniqueTerms.count
        )
        var cursors: [FullTextFusionPostingCursor] = []
        let cursorReservation = try request.workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: cursorLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: cursorGrowth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        defer { cursorReservation.release() }
        let observedCountLayout = try DatabaseRetainedArrayLayout.forElement(
            Int64.self
        )
        let observedCountGrowth = try observedCountLayout.growth(
            from: 0,
            toFit: prepared.uniqueTerms.count
        )
        let observedCountReservation = try request.workMeter
            .reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: observedCountLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: observedCountGrowth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        defer { observedCountReservation.release() }
        var observedPostingCounts = [Int64](
            repeating: 0,
            count: prepared.uniqueTerms.count
        )
        cursors.reserveCapacity(cursorGrowth.capacity)
        let terms = FullTextStorageLayout.terms(
            in: request.access.index.subspace
        )
        for (termIndex, term) in prepared.uniqueTerms.enumerated() {
            let subspace = terms.subspace(term)
            cursors.append(
                FullTextFusionPostingCursor(
                    termIndex: termIndex,
                    term: term,
                    subspace: subspace,
                    cursor: try request.access.subspaceCursor(
                        subspace,
                        reverse: false
                    )
                )
            )
        }
        var heap = try FullTextFusionPostingHeap(
            capacity: cursors.count,
            workMeter: request.workMeter
        )
        var equalHeads = try FullTextFusionEqualPostingHeads(
            capacity: prepared.uniqueTerms.count,
            workMeter: request.workMeter
        )
        let decoded = try FullTextFusionDecodedScratch(
            termCount: prepared.uniqueTerms.count,
            workMeter: request.workMeter
        )
        for index in cursors.indices {
            if let head = try await nextPosting(
                cursor: cursors[index],
                request: request
            ) {
                try heap.insert(head)
            }
        }

        while let first = try heap.removeMinimum() {
            equalHeads.removeAll()
            decoded.reset()
            try equalHeads.append(first)
            while let next = heap.minimum {
                try DatabaseByteProcessingMeter.consume(
                    byteCount: max(
                        next.packedIdentifier.count,
                        first.packedIdentifier.count
                    ),
                    workMeter: request.workMeter,
                    stage: .indexScan
                )
                guard next.packedIdentifier == first.packedIdentifier else {
                    break
                }
                guard let equal = try heap.removeMinimum() else {
                    preconditionFailure("A non-empty posting heap must pop")
                }
                try equalHeads.append(equal)
            }

            for head in equalHeads.values {
                let (count, overflow) = observedPostingCounts[head.termIndex]
                    .addingReportingOverflow(1)
                guard !overflow else {
                    throw FullTextStorageError.corruptedCorpusStatistics
                }
                observedPostingCounts[head.termIndex] = count
                try decoded.decode(
                    value: head.row.value,
                    termIndex: head.termIndex,
                    term: head.term,
                    positionsStored: prepared.configuration.storePositions,
                    phrase: prepared.mode == .phrase
                )
            }

            if let score = try await scoreDecoded(
                identifier: first.identifier,
                packedIdentifier: first.packedIdentifier,
                termFrequencies: decoded.termFrequencies,
                positionsByTerm: decoded.positionsByTerm,
                prepared: prepared,
                request: request,
                statistics: &statistics
            ) {
                try topK.consider(
                    primaryKey: first.packedIdentifier,
                    score: score
                )
            }

            for head in equalHeads.values {
                if let next = try await nextPosting(
                    cursor: cursors[head.cursorIndex],
                    request: request
                ) {
                    try heap.insert(next)
                }
            }
        }
        if statistics == nil {
            statistics = try await loadStatistics(
                prepared: prepared,
                request: request
            )
        }
        guard let statistics else {
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        for index in observedPostingCounts.indices {
            try request.workMeter.consume(at: .indexScan)
            guard observedPostingCounts[index]
                    == statistics.documentFrequencies[index] else {
                throw FullTextStorageError.corruptedCorpusStatistics
            }
        }
        try topK.emit(to: output)
        withExtendedLifetime(prepared) {}
        withExtendedLifetime(statistics) {}
        return .exhausted
    }

    package func executeRestricted(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        do {
            return try await executeRestrictedImpl(
                request,
                candidates: candidates,
                output: output
            )
        } catch {
            throw sanitizedExecutionError(error)
        }
    }

    private func sanitizedExecutionError(_ error: any Error) -> any Error {
        if error is FullTextStorageError
            || error is ByteConversionError
            || error is TupleError {
            return FusionExecutionError.corruptedIndex(indexType)
        }
        if error is FullTextIndexError {
            return FusionExecutionError.executionContractViolation
        }
        if let cleanup = error as? StorageRangeTerminalCleanupError {
            return StorageRangeTerminalCleanupError(
                cleanupError: sanitizedExecutionError(cleanup.cleanupError)
            )
        }
        if let cleanup = error as? StorageRangeCleanupError {
            return StorageRangeCleanupError(
                iterationError: sanitizedExecutionError(
                    cleanup.iterationError
                ),
                cleanupError: sanitizedExecutionError(
                    cleanup.cleanupError
                )
            )
        }
        return error
    }

    private func executeRestrictedImpl(
        _ request: FusionIndexReadRequest,
        candidates: FusionCandidateDomain,
        output: FusionMatchSink
    ) async throws -> FusionInputCoverage {
        guard request.limit > 0 else { return .satisfiedLimit }
        let prepared = try prepare(request)
        guard !prepared.uniqueTerms.isEmpty else { return .exhausted }
        var topK = try FullTextFusionTopK(
            limit: request.limit,
            workMeter: request.workMeter
        )
        var statistics: CorpusStatistics?
        let decoded = try FullTextFusionDecodedScratch(
            termCount: prepared.uniqueTerms.count,
            workMeter: request.workMeter
        )
        for index in 0..<candidates.count {
            let packed = candidates.primaryKey(at: index)
            let temporary = try request.workMeter.reserveIntermediate(
                bytes: try temporaryIdentifierByteCount(packed.count),
                at: .indexScan
            )
            do {
                let identifier = try Tuple(packed: packed)
                if let score = try await score(
                    identifier: identifier,
                    packedIdentifier: packed,
                    prepared: prepared,
                    request: request,
                    statistics: &statistics,
                    scratch: decoded
                ) {
                    try topK.consider(primaryKey: packed, score: score)
                }
                temporary.release()
            } catch {
                temporary.release()
                throw error
            }
        }
        try topK.emit(to: output)
        withExtendedLifetime(prepared) {}
        withExtendedLifetime(statistics) {}
        return .exhausted
    }

    private func prepare(
        _ request: FusionIndexReadRequest
    ) throws -> PreparedQuery {
        let termValues = try stringValues(
            FullTextReadParameter.terms,
            from: request.source.parameters
        )
        let mode = try matchMode(from: request.source.parameters)
        let configuration = try FullTextIndexConfiguration(
            definition: request.access.index.descriptor.declaration.definition
        )
        let bm25 = try parameters(from: request.source.parameters)
        var totalRawBytes: UInt64 = 0
        var maximumRawBytes: UInt64 = 0
        for value in termValues {
            guard case .string(let term) = value else {
                throw FusionExecutionError.invalidIndexInput(
                    indexType: indexType,
                    parameter: FullTextReadParameter.terms
                )
            }
            let byteCount = UInt64(term.utf8.count)
            totalRawBytes = try DatabaseIntermediateFootprint(
                bytes: totalRawBytes
            ).adding(DatabaseIntermediateFootprint(bytes: byteCount)).bytes
            maximumRawBytes = max(maximumRawBytes, byteCount)
        }
        let separatorBytes = UInt64(max(0, termValues.count - 1))
        let phraseSourceBytes = try DatabaseIntermediateFootprint(
            bytes: totalRawBytes
        ).adding(DatabaseIntermediateFootprint(bytes: separatorBytes)).bytes
        let normalizationSourceBytes = mode == .phrase
            ? phraseSourceBytes
            : maximumRawBytes
        let uniqueTermsLayout = try DatabaseRetainedArrayLayout.forElement(
            String.self
        )
        let groupLayout = try DatabaseRetainedArrayLayout.forElement(
            [Int].self
        )
        let termIndexLayout = try DatabaseRetainedArrayLayout.forElement(
            Int.self
        )
        let uniqueTermIndexLayout = try DatabaseRetainedHashTableLayout
            .validated(
                containerByteCount: UInt64(
                    MemoryLayout<[String: Int]>.stride
                ),
                elementCapacitySlotByteCount: UInt64(
                    max(
                        1,
                        MemoryLayout<FullTextFusionNormalizedTermSlot>.stride
                    )
                )
            )
        let retainedContainerBytes = try DatabaseIntermediateFootprint(
            bytes: uniqueTermsLayout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: groupLayout.containerByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: uniqueTermIndexLayout.containerByteCount
            )
        )
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: retainedContainerBytes.bytes,
            at: .indexScan
        )
        let normalizationScratchBytes = try normalizationTemporaryByteCount(
            sourceBytes: normalizationSourceBytes
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: mode == .phrase ? phraseSourceBytes : 0
            )
        ).bytes
        let normalizationScratch = try request.workMeter.reserveIntermediate(
            bytes: normalizationScratchBytes,
            at: .indexScan
        )
        defer { normalizationScratch.release() }
        let normalizer = FullTextTermNormalizer(
            tokenizer: configuration.tokenizer,
            ngramSize: configuration.ngramSize,
            minTermLength: configuration.minTermLength
        )
        var uniqueTerms: [String] = []
        var uniqueTermIndices: [String: Int] = [:]
        var groupTermIndices: [[Int]] = []
        var phraseTermIndices: [Int] = []
        var uniqueTermCapacity = 0
        var uniqueTermIndexCapacity = 0
        var groupCapacity = 0

        func incrementedCount(_ current: Int) throws -> Int {
            let (next, overflow) = current.addingReportingOverflow(1)
            guard !overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: current
                )
            }
            return next
        }

        func normalizeGroup(_ source: String) throws -> [Int] {
            let groupReservation = try reservation.reserveChild(
                bytes: termIndexLayout.containerByteCount,
                at: .indexScan
            )
            var groupReservationBytes = termIndexLayout.containerByteCount
            var indices: [Int] = []
            var indexCapacity = 0
            try DatabaseByteProcessingMeter.consume(
                byteCount: source.utf8.count,
                passes: 3,
                workMeter: request.workMeter,
                stage: .indexScan
            )
            try normalizer.forEachNormalizedTerm(from: source) { term in
                try request.workMeter.consume(at: .indexScan)
                let index: Int
                if let existing = uniqueTermIndices[term] {
                    index = existing
                } else {
                    index = uniqueTerms.count
                    let requiredTermCount = try incrementedCount(index)
                    let termGrowth = try uniqueTermsLayout.growth(
                        from: uniqueTermCapacity,
                        toFit: requiredTermCount
                    )
                    let hashGrowth = try uniqueTermIndexLayout.growth(
                        from: uniqueTermIndexCapacity,
                        toFit: requiredTermCount
                    )
                    try reservation.reserveAdditional(
                        rows: 1,
                        bytes: try DatabaseIntermediateFootprint(
                            bytes: UInt64(term.utf8.count)
                        ).adding(
                            DatabaseIntermediateFootprint(
                                bytes: termGrowth.additionalByteCount
                            )
                        ).adding(
                            DatabaseIntermediateFootprint(
                                bytes: hashGrowth.additionalByteCount
                            )
                        ).bytes,
                        at: .indexScan
                    )
                    if termGrowth.capacity != uniqueTermCapacity {
                        uniqueTerms.reserveCapacity(termGrowth.capacity)
                        uniqueTermCapacity = termGrowth.capacity
                    }
                    if hashGrowth.capacity != uniqueTermIndexCapacity {
                        uniqueTermIndices.reserveCapacity(hashGrowth.capacity)
                        uniqueTermIndexCapacity = hashGrowth.capacity
                    }
                    uniqueTermIndices[term] = index
                    uniqueTerms.append(term)
                }
                let indexGrowth = try termIndexLayout.growth(
                    from: indexCapacity,
                    toFit: try incrementedCount(indices.count)
                )
                if indexGrowth.capacity != indexCapacity {
                    try groupReservation.reserveAdditional(
                        bytes: indexGrowth.additionalByteCount,
                        at: .indexScan
                    )
                    groupReservationBytes = try DatabaseIntermediateFootprint(
                        bytes: groupReservationBytes
                    ).adding(
                        DatabaseIntermediateFootprint(
                            bytes: indexGrowth.additionalByteCount
                        )
                    ).bytes
                    indices.reserveCapacity(indexGrowth.capacity)
                    indexCapacity = indexGrowth.capacity
                }
                indices.append(index)
            }
            guard !indices.isEmpty else {
                groupReservation.release()
                return []
            }
            reservation.absorbGuaranteedPartial(
                from: groupReservation,
                bytes: groupReservationBytes
            )
            return indices
        }

        func appendGroup(_ indices: [Int]) throws {
            let growth = try groupLayout.growth(
                from: groupCapacity,
                toFit: try incrementedCount(groupTermIndices.count)
            )
            if growth.capacity != groupCapacity {
                try reservation.reserveAdditional(
                    bytes: growth.additionalByteCount,
                    at: .indexScan
                )
                groupTermIndices.reserveCapacity(growth.capacity)
                groupCapacity = growth.capacity
            }
            groupTermIndices.append(indices)
        }

        switch mode {
        case .phrase:
            guard let joinedCapacity = Int(exactly: phraseSourceBytes) else {
                throw FusionExecutionError.executionContractViolation
            }
            var joined = ""
            joined.reserveCapacity(joinedCapacity)
            for value in termValues {
                guard case .string(let term) = value else {
                    throw FusionExecutionError.invalidIndexInput(
                        indexType: indexType,
                        parameter: FullTextReadParameter.terms
                    )
                }
                if !joined.isEmpty { joined.append(" ") }
                joined.append(contentsOf: term)
            }
            phraseTermIndices = try normalizeGroup(joined)
            if !phraseTermIndices.isEmpty {
                try appendGroup(phraseTermIndices)
            }
        case .all, .any:
            for value in termValues {
                guard case .string(let rawTerm) = value else {
                    throw FusionExecutionError.invalidIndexInput(
                        indexType: indexType,
                        parameter: FullTextReadParameter.terms
                    )
                }
                let indices = try normalizeGroup(rawTerm)
                guard !indices.isEmpty else { continue }
                try appendGroup(indices)
            }
        }
        return PreparedQuery(
            mode: mode,
            groupTermIndices: groupTermIndices,
            uniqueTerms: uniqueTerms,
            phraseTermIndices: phraseTermIndices,
            configuration: configuration,
            bm25: bm25,
            reservation: reservation
        )
    }

    private func score(
        identifier: Tuple,
        packedIdentifier: ByteString,
        prepared: PreparedQuery,
        request: FusionIndexReadRequest,
        statistics: inout CorpusStatistics?,
        scratch: FullTextFusionDecodedScratch
    ) async throws -> Double? {
        let termsSubspace = FullTextStorageLayout.terms(
            in: request.access.index.subspace
        )
        scratch.reset()

        for (index, term) in prepared.uniqueTerms.enumerated() {
            try request.workMeter.consume(at: .indexScan)
            let key = termsSubspace.subspace(term).pack(identifier)
            guard let retainedValue = try await request.access.getValue(key: key) else {
                continue
            }
            try scratch.decode(
                value: retainedValue.bytes,
                termIndex: index,
                term: term,
                positionsStored: prepared.configuration.storePositions,
                phrase: prepared.mode == .phrase
            )
        }

        return try await scoreDecoded(
            identifier: identifier,
            packedIdentifier: packedIdentifier,
            termFrequencies: scratch.termFrequencies,
            positionsByTerm: scratch.positionsByTerm,
            prepared: prepared,
            request: request,
            statistics: &statistics
        )
    }

    private func scoreDecoded(
        identifier: Tuple,
        packedIdentifier: ByteString,
        termFrequencies: [Int],
        positionsByTerm: [[Int]],
        prepared: PreparedQuery,
        request: FusionIndexReadRequest,
        statistics: inout CorpusStatistics?
    ) async throws -> Double? {
        // No posting is an ordinary miss. Once any posting is observed, the
        // complete document-level physical contract must be validated before
        // semantic match evaluation. Otherwise corruption can become an empty
        // successful result for `.all` or `.phrase` queries.
        var hasPosting = false
        for frequency in termFrequencies {
            try request.workMeter.consume(at: .indexScan)
            if frequency > 0 {
                hasPosting = true
                break
            }
        }
        guard hasPosting else { return nil }
        if statistics == nil {
            statistics = try await loadStatistics(
                prepared: prepared,
                request: request
            )
        }
        guard let statistics else {
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        let metadataKey = FullTextStorageLayout.documents(
            in: request.access.index.subspace
        ).pack(identifier)
        guard let retainedMetadata = try await request.access.getValue(
            key: metadataKey
        ) else {
            throw FullTextStorageError.missingDocumentMetadata
        }
        let metadataValue = retainedMetadata.bytes
        let metadata = try FullTextStorageDecoder.documentMetadataCursor(
            from: metadataValue
        )
        guard metadata.uniqueTermCount > 0,
              metadata.uniqueTermCount <= metadata.docLength,
              let documentLength = Int(exactly: metadata.docLength),
              documentLength > 0,
              metadata.docLength <= statistics.totalLength else {
            throw FullTextStorageError.corruptedDocumentMetadata
        }
        for (termIndex, frequency) in termFrequencies.enumerated()
            where frequency > 0 {
            try request.workMeter.consume(at: .indexScan)
            let documentFrequency = statistics.documentFrequencies[termIndex]
            guard frequency <= documentLength else {
                throw FullTextStorageError.corruptedPosting(
                    term: prepared.uniqueTerms[termIndex]
                )
            }
            guard documentFrequency > 0,
                  documentFrequency <= statistics.totalDocuments else {
                throw FullTextStorageError.corruptedCorpusStatistics
            }
        }
        if case .phrase = prepared.mode {
            for (termIndex, positions) in positionsByTerm.enumerated() {
                try request.workMeter.consume(at: .indexScan)
                var previous: Int?
                for position in positions {
                    try request.workMeter.consume(at: .indexScan)
                    guard position < documentLength,
                          previous.map({ $0 < position }) ?? true else {
                        throw FullTextStorageError.corruptedPosting(
                            term: prepared.uniqueTerms[termIndex]
                        )
                    }
                    previous = position
                }
            }
        }
        let matches: Bool
        switch prepared.mode {
        case .all:
            var allMatch = true
            for frequency in termFrequencies {
                try request.workMeter.consume(at: .indexScan)
                guard frequency > 0 else {
                    allMatch = false
                    break
                }
            }
            matches = allMatch
        case .any:
            var anyMatch = false
            for group in prepared.groupTermIndices {
                try request.workMeter.consume(at: .indexScan)
                var groupMatches = true
                for index in group {
                    try request.workMeter.consume(at: .indexScan)
                    guard termFrequencies[index] > 0 else {
                        groupMatches = false
                        break
                    }
                }
                if groupMatches {
                    anyMatch = true
                    break
                }
            }
            matches = anyMatch
        case .phrase:
            matches = try containsPhrase(
                positionsByTerm,
                termIndices: prepared.phraseTermIndices,
                workMeter: request.workMeter
            )
        }
        guard matches else { return nil }

        let scorer = BM25Scorer(
            params: prepared.bm25,
            statistics: BM25Statistics(
                totalDocuments: statistics.totalDocuments,
                totalLength: statistics.totalLength
            )
        )
        var score = 0.0
        for index in prepared.uniqueTerms.indices
            where termFrequencies[index] > 0 {
            try request.workMeter.consume(at: .indexScan)
            let documentFrequency = statistics.documentFrequencies[index]
            score += scorer.scoreForTerm(
                termFrequency: termFrequencies[index],
                documentFrequency: documentFrequency,
                docLength: documentLength
            )
        }
        guard score.isFinite else {
            throw FusionExecutionError.invalidIndexScore(indexType)
        }
        return score
    }

    private func nextPosting(
        cursor: FullTextFusionPostingCursor,
        request: FusionIndexReadRequest
    ) async throws -> FullTextFusionPostingHead? {
        guard let row = try await cursor.cursor.next() else { return nil }
        guard cursor.subspace.contains(row.key) else {
            throw FusionExecutionError.corruptedIndex(
                request.access.index.descriptor.type
            )
        }
        try request.workMeter.consume(at: .indexScan)
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: try temporaryIdentifierByteCount(row.key.count),
            at: .indexScan
        )
        do {
            let identifierBytes = row.key[
                cursor.subspace.prefix.count..<row.key.count
            ]
            try DatabaseByteProcessingMeter.consume(
                byteCount: identifierBytes.count,
                passes: 2,
                workMeter: request.workMeter,
                stage: .indexScan
            )
            let identifier = try Tuple(packed: identifierBytes)
            guard identifier.pack() == identifierBytes else {
                throw FullTextStorageError.corruptedPosting(term: cursor.term)
            }
            return FullTextFusionPostingHead(
                cursorIndex: cursor.termIndex,
                termIndex: cursor.termIndex,
                term: cursor.term,
                identifier: identifier,
                packedIdentifier: identifierBytes,
                row: row,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func loadStatistics(
        prepared: PreparedQuery,
        request: FusionIndexReadRequest
    ) async throws -> CorpusStatistics {
        let documentCountBytes = try await request.access.getValue(
            key: FullTextStorageLayout.documentCountKey(
                in: request.access.index.subspace
            )
        )
        let totalLengthBytes = try await request.access.getValue(
            key: FullTextStorageLayout.totalDocumentLengthKey(
                in: request.access.index.subspace
            )
        )
        let totalDocuments: Int64
        let totalLength: Int64
        switch (documentCountBytes, totalLengthBytes) {
        case (nil, nil):
            totalDocuments = 0
            totalLength = 0
        case (.some(let documentCountBytes), .some(let totalLengthBytes)):
            totalDocuments = try ByteConversion.bytesToInt64(
                documentCountBytes.bytes
            )
            totalLength = try ByteConversion.bytesToInt64(
                totalLengthBytes.bytes
            )
            guard totalDocuments >= 0,
                  totalLength >= 0,
                  totalDocuments != 0 || totalLength == 0 else {
                throw FullTextStorageError.corruptedCorpusStatistics
            }
        default:
            throw FullTextStorageError.corruptedCorpusStatistics
        }
        let frequencyLayout = try DatabaseRetainedArrayLayout.forElement(
            Int64.self
        )
        let frequencyGrowth = try frequencyLayout.growth(
            from: 0,
            toFit: prepared.uniqueTerms.count
        )
        let reservation = try request.workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: frequencyLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: frequencyGrowth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        var frequencies = [Int64](
            repeating: 0,
            count: prepared.uniqueTerms.count
        )
        let frequencySubspace = FullTextStorageLayout.documentFrequencies(
            in: request.access.index.subspace
        )
        for (index, term) in prepared.uniqueTerms.enumerated() {
            if let retainedBytes = try await request.access.getValue(
                key: frequencySubspace.pack(Tuple(term))
            ) {
                let frequency = try ByteConversion.bytesToInt64(
                    retainedBytes.bytes
                )
                guard frequency >= 0, frequency <= totalDocuments else {
                    throw FullTextStorageError.corruptedCorpusStatistics
                }
                frequencies[index] = frequency
            }
        }
        return CorpusStatistics(
            totalDocuments: totalDocuments,
            totalLength: totalLength,
            documentFrequencies: frequencies,
            reservation: reservation
        )
    }

    private func containsPhrase(
        _ positionsByTerm: [[Int]],
        termIndices: [Int],
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        guard let firstTermIndex = termIndices.first else {
            return false
        }
        let first = positionsByTerm[firstTermIndex]
        guard !first.isEmpty else {
            return false
        }
        let cursorLayout = try DatabaseRetainedArrayLayout.forElement(Int.self)
        let cursorGrowth = try cursorLayout.growth(
            from: 0,
            toFit: termIndices.count
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: cursorLayout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: cursorGrowth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        defer { reservation.release() }
        var cursors = [Int](repeating: 0, count: termIndices.count)
        for start in first {
            try workMeter.consume(at: .indexScan)
            var isMatch = true
            for offset in termIndices.indices {
                try workMeter.consume(at: .indexScan)
                let (target, overflow) = start.addingReportingOverflow(offset)
                guard !overflow else { return false }
                let positions = positionsByTerm[termIndices[offset]]
                while cursors[offset] < positions.count,
                      positions[cursors[offset]] < target {
                    try workMeter.consume(at: .indexScan)
                    cursors[offset] += 1
                }
                guard cursors[offset] < positions.count,
                      positions[cursors[offset]] == target else {
                    isMatch = false
                    break
                }
            }
            if isMatch { return true }
        }
        return false
    }

    private func string(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard case .string(let value) = parameters[name] else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: name
            )
        }
        return value
    }

    private func stringValues(
        _ name: String,
        from parameters: [String: FieldValue]
    ) throws -> [FieldValue] {
        guard case .array(let values) = parameters[name] else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: name
            )
        }
        for value in values {
            guard case .string = value else {
                throw FusionExecutionError.invalidIndexInput(
                    indexType: indexType,
                    parameter: name
                )
            }
        }
        return values
    }

    private func normalizationTemporaryByteCount(
        sourceBytes: UInt64
    ) throws -> DatabaseIntermediateFootprint {
        let boundedSourceBytes = max(1, sourceBytes)
        let lowercaseAndTermMaterializationScratch = try DatabaseIntermediateFootprint(
            bytes: boundedSourceBytes
        ).multiplied(by: 16)
        return try lowercaseAndTermMaterializationScratch.adding(
            DatabaseIntermediateFootprint(bytes: 256)
        )
    }

    private func matchMode(
        from parameters: [String: FieldValue]
    ) throws -> TextMatchMode {
        switch try string(FullTextReadParameter.matchMode, from: parameters) {
        case "all": return .all
        case "any": return .any
        case "phrase": return .phrase
        default:
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: FullTextReadParameter.matchMode
            )
        }
    }

    private func parameters(
        from values: [String: FieldValue]
    ) throws -> BM25Parameters {
        let k1 = try optionalFloat64(
            FullTextReadParameter.bm25K1,
            from: values,
            defaultValue: Double(BM25Parameters.default.k1)
        )
        let b = try optionalFloat64(
            FullTextReadParameter.bm25B,
            from: values,
            defaultValue: Double(BM25Parameters.default.b)
        )
        guard k1.isFinite, Float(k1).isFinite, k1 >= 0 else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: FullTextReadParameter.bm25K1
            )
        }
        guard b.isFinite, Float(b).isFinite, b >= 0, b <= 1 else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: FullTextReadParameter.bm25B
            )
        }
        return BM25Parameters(k1: Float(k1), b: Float(b))
    }

    private func optionalFloat64(
        _ name: String,
        from values: [String: FieldValue],
        defaultValue: Double
    ) throws -> Double {
        guard let value = values[name] else { return defaultValue }
        guard case .float64(let decoded) = value else {
            throw FusionExecutionError.invalidIndexInput(
                indexType: indexType,
                parameter: name
            )
        }
        return decoded
    }

    private func validateParameterNames(
        _ parameters: [String: FieldValue]
    ) throws {
        for name in parameters.keys {
            switch name {
            case FullTextReadParameter.fieldName,
                 FullTextReadParameter.terms,
                 FullTextReadParameter.matchMode,
                 FullTextReadParameter.bm25K1,
                 FullTextReadParameter.bm25B:
                continue
            default:
                throw FusionExecutionError.invalidIndexInput(
                    indexType: indexType,
                    parameter: name
                )
            }
        }
    }

    private func temporaryIdentifierByteCount(
        _ encodedKeyByteCount: Int
    ) throws -> UInt64 {
        try DatabaseIntermediateFootprint(
            bytes: UInt64(encodedKeyByteCount) + 96
        ).adding(
            try DatabaseIntermediateFootprint(
                bytes: UInt64(
                    max(1, MemoryLayout<any TupleElement>.stride + 32)
                )
            ).multiplied(by: UInt64(encodedKeyByteCount))
        ).bytes
    }

    private struct PreparedQuery: Sendable {
        let mode: TextMatchMode
        let groupTermIndices: [[Int]]
        let uniqueTerms: [String]
        let phraseTermIndices: [Int]
        let configuration: FullTextIndexConfiguration
        let bm25: BM25Parameters
        let reservation: DatabaseIntermediateReservation
    }

    private struct CorpusStatistics: Sendable {
        let totalDocuments: Int64
        let totalLength: Int64
        let documentFrequencies: [Int64]
        let reservation: DatabaseIntermediateReservation
    }
}

private struct FullTextFusionNormalizedTermSlot: Sendable {
    let term: String
    let index: Int
}

private struct FullTextFusionPostingCursor: Sendable {
    let termIndex: Int
    let term: String
    let subspace: Subspace
    let cursor: FusionIndexReadCursor
}

private struct FullTextFusionPostingHead: Sendable {
    let cursorIndex: Int
    let termIndex: Int
    let term: String
    let identifier: Tuple
    let packedIdentifier: ByteString
    let row: FusionIndexReadRow
    private let reservation: DatabaseIntermediateReservation

    init(
        cursorIndex: Int,
        termIndex: Int,
        term: String,
        identifier: Tuple,
        packedIdentifier: ByteString,
        row: FusionIndexReadRow,
        reservation: DatabaseIntermediateReservation
    ) {
        self.cursorIndex = cursorIndex
        self.termIndex = termIndex
        self.term = term
        self.identifier = identifier
        self.packedIdentifier = packedIdentifier
        self.row = row
        self.reservation = reservation
    }
}

private struct FullTextFusionPostingHeap {
    private var values: [FullTextFusionPostingHead] = []
    private let capacity: Int
    private let workMeter: DatabaseWorkMeter
    private let reservation: DatabaseIntermediateReservation

    init(capacity: Int, workMeter: DatabaseWorkMeter) throws {
        self.capacity = capacity
        self.workMeter = workMeter
        let layout = try DatabaseRetainedArrayLayout.forElement(
            FullTextFusionPostingHead.self
        )
        let growth = try layout.growth(from: 0, toFit: capacity)
        self.reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: layout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        values.reserveCapacity(growth.capacity)
    }

    var minimum: FullTextFusionPostingHead? { values.first }

    mutating func insert(_ value: FullTextFusionPostingHead) throws {
        precondition(values.count < capacity)
        values.append(value)
        var child = values.count - 1
        while child > 0 {
            let parent = (child - 1) / 2
            guard try ordered(values[child], before: values[parent]) else {
                break
            }
            values.swapAt(child, parent)
            child = parent
        }
    }

    mutating func removeMinimum() throws -> FullTextFusionPostingHead? {
        guard !values.isEmpty else { return nil }
        if values.count == 1 { return values.removeLast() }
        let minimum = values[0]
        values[0] = values.removeLast()
        var parent = 0
        while true {
            let left = parent * 2 + 1
            guard left < values.count else { break }
            let right = left + 1
            var child = left
            if right < values.count,
               try ordered(values[right], before: values[left]) {
                child = right
            }
            guard try ordered(values[child], before: values[parent]) else {
                break
            }
            values.swapAt(parent, child)
            parent = child
        }
        return minimum
    }

    private func ordered(
        _ lhs: FullTextFusionPostingHead,
        before rhs: FullTextFusionPostingHead
    ) throws -> Bool {
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(
                lhs.packedIdentifier.count,
                rhs.packedIdentifier.count
            ),
            workMeter: workMeter,
            stage: .indexScan
        )
        if lhs.packedIdentifier != rhs.packedIdentifier {
            return lhs.packedIdentifier < rhs.packedIdentifier
        }
        return lhs.termIndex < rhs.termIndex
    }
}

private struct FullTextFusionEqualPostingHeads {
    private(set) var values: [FullTextFusionPostingHead] = []
    private let capacity: Int
    private let reservation: DatabaseIntermediateReservation

    init(capacity: Int, workMeter: DatabaseWorkMeter) throws {
        self.capacity = capacity
        let layout = try DatabaseRetainedArrayLayout.forElement(
            FullTextFusionPostingHead.self
        )
        let growth = try layout.growth(from: 0, toFit: capacity)
        self.reservation = try workMeter.reserveIntermediate(
            bytes: try DatabaseIntermediateFootprint(
                bytes: layout.containerByteCount
            ).adding(
                DatabaseIntermediateFootprint(
                    bytes: growth.additionalByteCount
                )
            ).bytes,
            at: .indexScan
        )
        values.reserveCapacity(growth.capacity)
    }

    mutating func append(_ value: FullTextFusionPostingHead) throws {
        guard values.count < capacity else {
            throw FusionExecutionError.corruptedIndex(.text(.fullText))
        }
        values.append(value)
    }

    mutating func removeAll() {
        values.removeAll(keepingCapacity: true)
    }
}

private final class FullTextFusionDecodedScratch {
    private let reservation: DatabaseIntermediateReservation
    private let positionLayout: DatabaseRetainedArrayLayout
    private let workMeter: DatabaseWorkMeter
    private var accountedPositionCapacities: [Int]
    private(set) var termFrequencies: [Int]
    private(set) var positionsByTerm: [[Int]]

    init(termCount: Int, workMeter: DatabaseWorkMeter) throws {
        precondition(termCount >= 0)
        let frequencyLayout = try DatabaseRetainedArrayLayout.forElement(
            Int.self
        )
        let positionsLayout = try DatabaseRetainedArrayLayout.forElement(
            [Int].self
        )
        let frequencyGrowth = try frequencyLayout.growth(
            from: 0,
            toFit: termCount
        )
        let positionsGrowth = try positionsLayout.growth(
            from: 0,
            toFit: termCount
        )
        let capacityGrowth = try frequencyLayout.growth(
            from: 0,
            toFit: termCount
        )
        let byteCount = try DatabaseIntermediateFootprint(
            bytes: frequencyLayout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: frequencyGrowth.additionalByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: frequencyLayout.containerByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: capacityGrowth.additionalByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: positionsLayout.containerByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: positionsGrowth.additionalByteCount
            )
        ).bytes
        self.reservation = try workMeter.reserveIntermediate(
            bytes: byteCount,
            at: .indexScan
        )
        self.positionLayout = frequencyLayout
        self.workMeter = workMeter
        self.accountedPositionCapacities = [Int](
            repeating: 0,
            count: termCount
        )
        self.termFrequencies = [Int](repeating: 0, count: termCount)
        self.positionsByTerm = [[Int]](repeating: [], count: termCount)
    }

    func reset() {
        for index in termFrequencies.indices {
            termFrequencies[index] = 0
            positionsByTerm[index].removeAll(keepingCapacity: true)
        }
    }

    func decode(
        value: ByteString,
        termIndex: Int,
        term: String,
        positionsStored: Bool,
        phrase: Bool
    ) throws {
        precondition(termFrequencies.indices.contains(termIndex))
        if phrase {
            let growth = try positionLayout.growth(
                from: accountedPositionCapacities[termIndex],
                toFit: value.count
            )
            if growth.capacity != accountedPositionCapacities[termIndex] {
                try reservation.reserveAdditional(
                    bytes: growth.additionalByteCount,
                    at: .indexScan
                )
                positionsByTerm[termIndex].reserveCapacity(growth.capacity)
                accountedPositionCapacities[termIndex] = growth.capacity
            }
            termFrequencies[termIndex] = try FullTextStorageDecoder
                .postingPositions(
                    from: value,
                    term: term,
                    into: &positionsByTerm[termIndex],
                    workMeter: workMeter
                )
        } else {
            termFrequencies[termIndex] = try FullTextStorageDecoder
                .postingFrequency(
                    from: value,
                    positionsStored: positionsStored,
                    term: term,
                    workMeter: workMeter
                )
        }
    }
}

struct FullTextFusionTopK {
    private struct Match: Sendable {
        let primaryKey: ByteString
        let score: Double
    }

    private var matches: [Match]
    private let limit: Int
    private let reservation: DatabaseIntermediateReservation
    private let layout: DatabaseRetainedArrayLayout
    private let workMeter: DatabaseWorkMeter
    private var accountedCapacity: Int

    init(limit: Int, workMeter: DatabaseWorkMeter) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Match.self)
        self.reservation = try workMeter.reserveIntermediate(
            bytes: layout.containerByteCount,
            at: .indexScan
        )
        self.matches = []
        self.limit = limit
        self.layout = layout
        self.workMeter = workMeter
        self.accountedCapacity = 0
    }

    mutating func consider(
        primaryKey: ByteString,
        score: Double
    ) throws {
        guard limit > 0 else { return }
        try workMeter.consume(at: .indexScan)
        let keyBytes = UInt64(primaryKey.count)
        if matches.count < limit {
            let (requiredCount, countOverflow) = matches.count
                .addingReportingOverflow(1)
            guard !countOverflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: accountedCapacity
                )
            }
            let growth = try layout.growth(
                from: accountedCapacity,
                toFit: requiredCount
            )
            let keyReservation = try reservation.reserveChild(
                bytes: keyBytes,
                at: .indexScan
            )
            let retainedPrimaryKey = try DatabaseRetainedByteString.copying(
                primaryKey,
                reservation: keyReservation,
                at: .indexScan
            )
            try reservation.reserveAdditional(
                rows: 1,
                bytes: growth.additionalByteCount,
                at: .indexScan
            )
            if growth.capacity != accountedCapacity {
                matches.reserveCapacity(growth.capacity)
                accountedCapacity = growth.capacity
            }
            matches.append(
                Match(
                    primaryKey: retainedPrimaryKey,
                    score: score
                )
            )
            try siftUp(from: matches.count - 1)
            return
        }
        try workMeter.consume(at: .sortComparison)
        guard try isBetter(
            Match(
                primaryKey: primaryKey,
                score: score
            ),
            than: matches[0]
        ) else {
            return
        }
        // Admit the complete replacement while the old key is still alive.
        // The independent key owner releases the complete old claim on swap.
        let keyReservation = try reservation.reserveChild(
            bytes: keyBytes,
            at: .indexScan
        )
        let retainedPrimaryKey = try DatabaseRetainedByteString.copying(
            primaryKey,
            reservation: keyReservation,
            at: .indexScan
        )
        matches[0] = Match(
            primaryKey: retainedPrimaryKey,
            score: score
        )
        try siftDown(from: 0, through: matches.count - 1)
    }

    mutating func emit(to output: FusionMatchSink) throws {
        if matches.count > 1 {
            var end = matches.count - 1
            while end > 0 {
                matches.swapAt(0, end)
                end -= 1
                try siftDown(from: 0, through: end)
            }
        }
        for match in matches {
            try output.submit(
                primaryKey: match.primaryKey,
                numericSignal: match.score
            )
        }
    }

    private mutating func siftUp(from start: Int) throws {
        var child = start
        while child > 0 {
            let parent = (child - 1) / 2
            try workMeter.consume(at: .sortComparison)
            guard try isBetter(
                matches[parent],
                than: matches[child]
            ) else {
                return
            }
            matches.swapAt(parent, child)
            child = parent
        }
    }

    private mutating func siftDown(
        from start: Int,
        through end: Int
    ) throws {
        var parent = start
        while true {
            let left = parent * 2 + 1
            guard left <= end else { return }
            let right = left + 1
            var worseChild = left
            if right <= end {
                try workMeter.consume(at: .sortComparison)
                if try isBetter(matches[left], than: matches[right]) {
                    worseChild = right
                }
            }
            try workMeter.consume(at: .sortComparison)
            guard try isBetter(
                matches[parent],
                than: matches[worseChild]
            ) else {
                return
            }
            matches.swapAt(parent, worseChild)
            parent = worseChild
        }
    }

    private func isBetter(
        _ lhs: borrowing Match,
        than rhs: borrowing Match
    ) throws -> Bool {
        if lhs.score == rhs.score {
            try DatabaseByteProcessingMeter.consume(
                byteCount: max(
                    lhs.primaryKey.count,
                    rhs.primaryKey.count
                ),
                workMeter: workMeter,
                stage: .sortComparison
            )
            return lhs.primaryKey.lexicographicallyPrecedes(rhs.primaryKey)
        }
        return lhs.score > rhs.score
    }
}

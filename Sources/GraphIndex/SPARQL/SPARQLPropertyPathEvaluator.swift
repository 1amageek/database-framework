import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

/// Executes SPARQL property-path algebra over retained RDF endpoint pairs.
/// VariableBinding dictionaries are constructed only once at the outer edge.
struct SPARQLPropertyPathEvaluator: Sendable {
    private struct MatchResult: ~Copyable, Sendable {
        var matches: SPARQLPropertyPathMatches
        var statistics: ExecutionStatistics
    }

    private let datasetScanner: any RDFDatasetScanner
    private let readMode: RDFDatasetReadMode
    private let ontologyContext: OntologyContext?
    private let workMeter: DatabaseWorkMeter
    private let configuration: ExecutionPropertyPathConfiguration

    init(
        datasetScanner: any RDFDatasetScanner,
        readMode: RDFDatasetReadMode,
        ontologyContext: OntologyContext?,
        workMeter: DatabaseWorkMeter,
        configuration: ExecutionPropertyPathConfiguration
    ) {
        self.datasetScanner = datasetScanner
        self.readMode = readMode
        self.ontologyContext = ontologyContext
        self.workMeter = workMeter
        self.configuration = configuration
    }

    func execute(
        subject: ExecutionTerm,
        path: ExecutionPropertyPath,
        object: ExecutionTerm,
        seed: consuming VariableBinding,
        resultLimit: Int?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> SPARQLPropertyPathExecutionResult {
        try validateConfiguration()
        guard resultLimit.map({ $0 >= 0 }) ?? true else {
            throw SPARQLQueryError.invalidPagination
        }
        let structureValidator = try SPARQLPropertyPathStructureValidator.make(
            workMeter: workMeter
        )
        try structureValidator.validate(
            path,
            maximumDepth: configuration.maximumExpressionDepth
        )
        structureValidator.shutdown()

        let correlatedSubject = subject.substitute(seed)
        let correlatedObject = object.substitute(seed)
        let startConstraint = try boundRDFTerm(correlatedSubject)
        let endConstraint = try boundRDFTerm(correlatedObject)
        if resultLimit == 0 {
            return SPARQLPropertyPathExecutionResult(
                bindings: .empty,
                statistics: ExecutionStatistics()
            )
        }
        let evaluated = try await evaluate(
            path,
            startConstraint: startConstraint,
            endConstraint: endConstraint,
            graphTarget: graphTarget,
            transaction: transaction
        )
        let statistics = evaluated.statistics

        var firstCompatibleIndex: Int?
        for index in 0..<evaluated.matches.count {
            let compatible = try evaluated.matches.withElement(
                at: index
            ) { match in
                try SPARQLPropertyPathEndpointCompatibility.matches(
                    subject: correlatedSubject,
                    start: match.start,
                    object: correlatedObject,
                    end: match.end,
                    workMeter: workMeter
                )
            }
            guard compatible else { continue }
            firstCompatibleIndex = index
            break
        }
        guard let firstCompatibleIndex else {
            return SPARQLPropertyPathExecutionResult(
                bindings: .empty,
                statistics: statistics
            )
        }
        guard configuration.maximumResults > 0 else {
            throw SPARQLQueryError.propertyPathResultLimitExceeded(
                maximum: configuration.maximumResults
            )
        }

        var bindings = try SPARQLRetainedBindingBuilder.make(
            workMeter: workMeter,
            stage: .pathExpansion,
            expectedCount: 0
        )
        let footprintMeter = try SPARQLPropertyPathBindingFootprintMeter.make(
            workMeter: workMeter,
            stage: .pathExpansion
        )
        for index in firstCompatibleIndex..<evaluated.matches.count {
            if let resultLimit, bindings.count >= resultLimit {
                break
            }
            try workMeter.consume(at: .pathExpansion)
            try evaluated.matches.withElement(at: index) { match in
                if index != firstCompatibleIndex {
                    guard try SPARQLPropertyPathEndpointCompatibility.matches(
                        subject: correlatedSubject,
                        start: match.start,
                        object: correlatedObject,
                        end: match.end,
                        workMeter: workMeter
                    ) else {
                        return
                    }
                }
                try bindings.appendCompatiblePropertyPathMatch(
                    propertyPathMatch: match,
                    retaining: seed,
                    subject: correlatedSubject,
                    object: correlatedObject,
                    footprintMeter: footprintMeter,
                    maximumResults: configuration.maximumResults,
                    at: .pathExpansion
                )
            }
        }
        footprintMeter.shutdown()
        return SPARQLPropertyPathExecutionResult(
            bindings: bindings.finish(),
            statistics: statistics
        )
    }

    private func evaluate(
        _ path: ExecutionPropertyPath,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        try workMeter.consume(at: .pathExpansion)
        switch path {
        case .empty:
            return try await evaluateIdentity(
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .iri(let predicate):
            return try await evaluatePredicate(
                predicate,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .negatedPropertySet(let exclusions):
            return try await evaluateNegatedPropertySet(
                exclusions,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .inverse(let inner):
            let innerResult = try await evaluate(
                inner,
                startConstraint: endConstraint,
                endConstraint: startConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
            return MatchResult(
                matches: (consume innerResult.matches).inverted(),
                statistics: innerResult.statistics
            )
        case .sequence(let left, let right):
            return try await evaluateSequence(
                left: left,
                right: right,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .alternative(let left, let right):
            return try await evaluateAlternative(
                left: left,
                right: right,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .zeroOrMore(let inner):
            return try await evaluateRepetition(
                inner,
                minimum: 0,
                maximum: nil,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .oneOrMore(let inner):
            return try await evaluateRepetition(
                inner,
                minimum: 1,
                maximum: nil,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .zeroOrOne(let inner):
            return try await evaluateRepetition(
                inner,
                minimum: 0,
                maximum: 1,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        case .range(let inner, let bounds):
            if bounds.maximum == bounds.minimum {
                return try await evaluateFixedLength(
                    inner,
                    repetitions: bounds.minimum,
                    startConstraint: startConstraint,
                    endConstraint: endConstraint,
                    graphTarget: graphTarget,
                    transaction: transaction
                )
            }
            return try await evaluateRepetition(
                inner,
                minimum: bounds.minimum,
                maximum: bounds.maximum,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        }
    }

    /// Evaluates an exact repetition as a fixed-length sequence. SPARQL
    /// translates fixed-length paths to joined triple patterns, so distinct
    /// hidden intermediate bindings must retain endpoint multiplicity.
    private func evaluateFixedLength(
        _ inner: ExecutionPropertyPath,
        repetitions: Int,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        guard repetitions <= configuration.maximumTraversalDepth else {
            throw SPARQLQueryError.propertyPathTraversalDepthLimitExceeded(
                maximum: configuration.maximumTraversalDepth
            )
        }
        guard repetitions > 0 else {
            return try await evaluateIdentity(
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
        }

        let first = try await evaluate(
            inner,
            startConstraint: startConstraint,
            endConstraint: repetitions == 1 ? endConstraint : nil,
            graphTarget: graphTarget,
            transaction: transaction
        )
        var statistics = first.statistics
        var frontier = consume first.matches
        var level = 1

        while level < repetitions, !frontier.isEmpty {
            let nextLevel = try checkedIncrement(level)
            let advanced = try await advancePreservingMultiplicity(
                frontier,
                through: inner,
                endConstraint: nextLevel == repetitions
                    ? endConstraint
                    : nil,
                graphTarget: graphTarget,
                transaction: transaction
            )
            try mergeStatistics(advanced.statistics, into: &statistics)
            frontier = consume advanced.matches
            level = nextLevel
        }

        return MatchResult(
            matches: frontier,
            statistics: statistics
        )
    }

    private func evaluatePredicate(
        _ predicate: RDFPredicateIRI,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        var builder = try makeMatchBuilder()
        var seen = try SPARQLPropertyPathMatchSet.make(
            workMeter: workMeter
        )
        var statistics = ExecutionStatistics(patternsEvaluated: 1)
        if let scans = ontologyContext?.knownEntailedPropertyScans(
            of: predicate.rawValue
        ) {
            for scan in scans {
                let typedPredicate: RDFPredicateIRI
                do {
                    typedPredicate = try RDFPredicateIRI(
                        scan.predicateIRI
                    )
                } catch {
                    throw SPARQLQueryError.invalidOntologyPredicateIRI(
                        scan.predicateIRI
                    )
                }
                try await appendPredicateScan(
                    typedPredicate,
                    inverse: scan.isInverse,
                    startConstraint: startConstraint,
                    endConstraint: endConstraint,
                    graphTarget: graphTarget,
                    transaction: transaction,
                    builder: &builder,
                    seen: &seen,
                    statistics: &statistics
                )
            }
        } else {
            try await appendPredicateScan(
                predicate,
                inverse: false,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction,
                builder: &builder,
                seen: &seen,
                statistics: &statistics
            )
        }
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func appendPredicateScan(
        _ predicate: RDFPredicateIRI,
        inverse: Bool,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess,
        builder: inout SPARQLPropertyPathMatchBuilder,
        seen: inout SPARQLPropertyPathMatchSet,
        statistics: inout ExecutionStatistics
    ) async throws {
        let scan = try await datasetScanner.scan(
            subject: inverse ? endConstraint : startConstraint,
            predicate: predicate.term,
            object: inverse ? startConstraint : endConstraint,
            graphTarget: graphTarget,
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
        try addIndexScans(scan.physicalScanCount, to: &statistics)
        for quad in scan {
            try workMeter.consume(at: .pathExpansion)
            let match = inverse
                ? SPARQLPropertyPathMatch(
                    start: quad.object,
                    end: quad.subject
                )
                : SPARQLPropertyPathMatch(
                    start: quad.subject,
                    end: quad.object
                )
            try appendDistinct(match, builder: &builder, seen: &seen)
        }
    }

    private func evaluateNegatedPropertySet(
        _ exclusions: PropertyPathNegatedSet,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        var builder = try makeMatchBuilder()
        var statistics = ExecutionStatistics(patternsEvaluated: 1)
        if let forward = exclusions.forward {
            var seen = try SPARQLPropertyPathMatchSet.make(
                workMeter: workMeter
            )
            try await appendNegatedScan(
                excludedPredicates: forward,
                inverse: false,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction,
                builder: &builder,
                seen: &seen,
                statistics: &statistics
            )
        }
        if let inverse = exclusions.inverse {
            var seen = try SPARQLPropertyPathMatchSet.make(
                workMeter: workMeter
            )
            try await appendNegatedScan(
                excludedPredicates: inverse,
                inverse: true,
                startConstraint: startConstraint,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction,
                builder: &builder,
                seen: &seen,
                statistics: &statistics
            )
        }
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func appendNegatedScan(
        excludedPredicates: Set<RDFPredicateIRI>,
        inverse: Bool,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess,
        builder: inout SPARQLPropertyPathMatchBuilder,
        seen: inout SPARQLPropertyPathMatchSet,
        statistics: inout ExecutionStatistics
    ) async throws {
        let scan = try await datasetScanner.scan(
            subject: inverse ? endConstraint : startConstraint,
            predicate: nil,
            object: inverse ? startConstraint : endConstraint,
            graphTarget: graphTarget,
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
        try addIndexScans(scan.physicalScanCount, to: &statistics)
        for quad in scan {
            try workMeter.consume(at: .pathExpansion)
            guard case .iri(let predicateIRI) = quad.predicate else {
                continue
            }
            let predicate = RDFPredicateIRI(predicateIRI)
            guard !excludedPredicates.contains(predicate) else { continue }
            let match = inverse
                ? SPARQLPropertyPathMatch(
                    start: quad.object,
                    end: quad.subject
                )
                : SPARQLPropertyPathMatch(
                    start: quad.subject,
                    end: quad.object
                )
            try appendDistinct(match, builder: &builder, seen: &seen)
        }
    }

    private func evaluateSequence(
        left: ExecutionPropertyPath,
        right: ExecutionPropertyPath,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        let leftResult = try await evaluate(
            left,
            startConstraint: startConstraint,
            endConstraint: nil,
            graphTarget: graphTarget,
            transaction: transaction
        )
        var statistics = leftResult.statistics
        var builder = try makeMatchBuilder()
        for leftIndex in 0..<leftResult.matches.count {
            let leftMatch = leftResult.matches.withElement(
                at: leftIndex
            ) { copy $0 }
            let rightResult = try await evaluate(
                right,
                startConstraint: leftMatch.end,
                endConstraint: endConstraint,
                graphTarget: graphTarget,
                transaction: transaction
            )
            try mergeStatistics(rightResult.statistics, into: &statistics)
            for rightIndex in 0..<rightResult.matches.count {
                try rightResult.matches.withElement(at: rightIndex) {
                    rightMatch in
                    try builder.append(
                        start: leftMatch.start,
                        end: rightMatch.end
                    )
                }
            }
        }
        statistics.patternsEvaluated = try checkedIncrement(
            statistics.patternsEvaluated
        )
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func evaluateAlternative(
        left: ExecutionPropertyPath,
        right: ExecutionPropertyPath,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        let leftResult = try await evaluate(
            left,
            startConstraint: startConstraint,
            endConstraint: endConstraint,
            graphTarget: graphTarget,
            transaction: transaction
        )
        var statistics = leftResult.statistics
        var builder = try SPARQLPropertyPathMatchBuilder.resumeIntermediate(
            consume leftResult.matches,
            workMeter: workMeter
        )
        let rightResult = try await evaluate(
            right,
            startConstraint: startConstraint,
            endConstraint: endConstraint,
            graphTarget: graphTarget,
            transaction: transaction
        )
        try mergeStatistics(rightResult.statistics, into: &statistics)
        try builder.appendBorrowed(contentsOf: rightResult.matches)
        statistics.patternsEvaluated = try checkedIncrement(
            statistics.patternsEvaluated
        )
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func evaluateIdentity(
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        var statistics = ExecutionStatistics(patternsEvaluated: 1)
        if let startConstraint, let endConstraint,
           startConstraint != endConstraint {
            return MatchResult(matches: .empty, statistics: statistics)
        }
        if let node = startConstraint ?? endConstraint {
            var builder = try makeMatchBuilder(expectedCount: 1)
            try builder.append(start: node, end: node)
            return MatchResult(
                matches: builder.finish(),
                statistics: statistics
            )
        }

        let scan = try await datasetScanner.scan(
            subject: nil,
            predicate: nil,
            object: nil,
            graphTarget: graphTarget,
            limit: nil,
            readMode: readMode,
            transaction: transaction,
            workMeter: workMeter
        )
        try addIndexScans(scan.physicalScanCount, to: &statistics)
        var builder = try makeMatchBuilder()
        var seen = try SPARQLPropertyPathMatchSet.make(
            workMeter: workMeter
        )
        for quad in scan {
            try appendDistinct(
                SPARQLPropertyPathMatch(
                    start: quad.subject,
                    end: quad.subject
                ),
                builder: &builder,
                seen: &seen
            )
            try appendDistinct(
                SPARQLPropertyPathMatch(
                    start: quad.object,
                    end: quad.object
                ),
                builder: &builder,
                seen: &seen
            )
        }
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func evaluateRepetition(
        _ inner: ExecutionPropertyPath,
        minimum: Int,
        maximum: Int?,
        startConstraint: RDFTerm?,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        guard minimum <= configuration.maximumTraversalDepth,
              (maximum ?? minimum) <= configuration.maximumTraversalDepth else {
            throw SPARQLQueryError.propertyPathTraversalDepthLimitExceeded(
                maximum: configuration.maximumTraversalDepth
            )
        }

        let identity = try await evaluateIdentity(
            startConstraint: startConstraint,
            endConstraint: nil,
            graphTarget: graphTarget,
            transaction: transaction
        )
        var statistics = identity.statistics
        var frontier = consume identity.matches
        var results = try makeMatchBuilder()
        var resultSet = try SPARQLPropertyPathMatchSet.make(
            workMeter: workMeter
        )
        var level = 0

        if minimum == 0 {
            try appendEligibleDistinct(
                frontier,
                endConstraint: endConstraint,
                builder: &results,
                seen: &resultSet
            )
            // A zero-length path with one fixed endpoint binds the variable
            // endpoint to that RDF term even when the term is absent from the
            // active graph. The graph node set constrains only the case where
            // both endpoints are unbound.
            if startConstraint == nil, let endConstraint {
                try appendDistinct(
                    SPARQLPropertyPathMatch(
                        start: endConstraint,
                        end: endConstraint
                    ),
                    builder: &results,
                    seen: &resultSet
                )
            }
        }

        while level < minimum {
            let advanced = try await advance(
                frontier,
                through: inner,
                graphTarget: graphTarget,
                transaction: transaction
            )
            try mergeStatistics(advanced.statistics, into: &statistics)
            frontier = consume advanced.matches
            level = try checkedIncrement(level)
            if frontier.isEmpty {
                return MatchResult(
                    matches: results.finish(),
                    statistics: statistics
                )
            }
        }

        if minimum > 0 {
            try appendEligibleDistinct(
                frontier,
                endConstraint: endConstraint,
                builder: &results,
                seen: &resultSet
            )
        }

        if let maximum {
            while level < maximum, !frontier.isEmpty {
                let advanced = try await advance(
                    frontier,
                    through: inner,
                    graphTarget: graphTarget,
                    transaction: transaction
                )
                try mergeStatistics(advanced.statistics, into: &statistics)
                frontier = consume advanced.matches
                level = try checkedIncrement(level)
                try appendEligibleDistinct(
                    frontier,
                    endConstraint: endConstraint,
                    builder: &results,
                    seen: &resultSet
                )
            }
            return MatchResult(
                matches: results.finish(),
                statistics: statistics
            )
        }

        var visited = try SPARQLPropertyPathMatchSet.make(
            workMeter: workMeter,
            stage: .pathExpansion
        )
        if configuration.detectCycles {
            for index in 0..<frontier.count {
                try frontier.withElement(at: index) { match in
                    _ = try visited.insert(match, at: .pathExpansion)
                }
            }
        }

        while !frontier.isEmpty {
            let advanced = try await advance(
                frontier,
                through: inner,
                graphTarget: graphTarget,
                transaction: transaction
            )
            try mergeStatistics(advanced.statistics, into: &statistics)
            let nextLevel = try checkedIncrement(level)

            if nextLevel > configuration.maximumTraversalDepth {
                var hasUnseen = false
                for index in 0..<advanced.matches.count {
                    let unseen = advanced.matches.withElement(
                        at: index
                    ) { match in
                        !configuration.detectCycles
                            || !visited.contains(match)
                    }
                    if unseen {
                        hasUnseen = true
                        break
                    }
                }
                guard !hasUnseen else {
                    throw SPARQLQueryError
                        .propertyPathTraversalDepthLimitExceeded(
                            maximum: configuration.maximumTraversalDepth
                        )
                }
                break
            }

            try appendEligibleDistinct(
                advanced.matches,
                endConstraint: endConstraint,
                builder: &results,
                seen: &resultSet
            )
            var nextFrontier = try makeMatchBuilder()
            for index in 0..<advanced.matches.count {
                try advanced.matches.withElement(at: index) { match in
                    if configuration.detectCycles {
                        guard try visited.insert(
                            match,
                            at: .pathExpansion
                        ) else { return }
                    }
                    try nextFrontier.appendBorrowed(match)
                }
            }
            frontier = nextFrontier.finish()
            level = nextLevel
        }

        return MatchResult(
            matches: results.finish(),
            statistics: statistics
        )
    }

    private func advance(
        _ frontier: borrowing SPARQLPropertyPathMatches,
        through path: ExecutionPropertyPath,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        var builder = try makeMatchBuilder()
        var seen = try SPARQLPropertyPathMatchSet.make(
            workMeter: workMeter,
            stage: .pathExpansion
        )
        var statistics = ExecutionStatistics()
        for frontierIndex in 0..<frontier.count {
            let current = frontier.withElement(
                at: frontierIndex
            ) { copy $0 }
            let hop = try await evaluate(
                path,
                startConstraint: current.end,
                endConstraint: nil,
                graphTarget: graphTarget,
                transaction: transaction
            )
            try mergeStatistics(hop.statistics, into: &statistics)
            for hopIndex in 0..<hop.matches.count {
                try hop.matches.withElement(at: hopIndex) { edge in
                    let candidate = SPARQLPropertyPathMatch(
                        start: current.start,
                        end: edge.end
                    )
                    try appendDistinct(
                        candidate,
                        builder: &builder,
                        seen: &seen
                    )
                }
            }
        }
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    /// Advances one fixed-length sequence step without collapsing rows that
    /// reached the same endpoint through different hidden intermediates.
    private func advancePreservingMultiplicity(
        _ frontier: borrowing SPARQLPropertyPathMatches,
        through path: ExecutionPropertyPath,
        endConstraint: RDFTerm?,
        graphTarget: RDFGraphScanTarget,
        transaction: any TransactionReadAccess
    ) async throws -> MatchResult {
        var builder = try makeMatchBuilder()
        var statistics = ExecutionStatistics()
        for frontierIndex in 0..<frontier.count {
            try await frontier.withElement(at: frontierIndex) { current in
                let hop = try await evaluate(
                    path,
                    startConstraint: current.end,
                    endConstraint: endConstraint,
                    graphTarget: graphTarget,
                    transaction: transaction
                )
                try mergeStatistics(hop.statistics, into: &statistics)
                for hopIndex in 0..<hop.matches.count {
                    try hop.matches.withElement(at: hopIndex) { edge in
                        try builder.append(
                            start: current.start,
                            end: edge.end
                        )
                    }
                }
            }
        }
        return MatchResult(
            matches: builder.finish(),
            statistics: statistics
        )
    }

    private func appendEligibleDistinct(
        _ source: borrowing SPARQLPropertyPathMatches,
        endConstraint: RDFTerm?,
        builder: inout SPARQLPropertyPathMatchBuilder,
        seen: inout SPARQLPropertyPathMatchSet
    ) throws {
        for index in 0..<source.count {
            try source.withElement(at: index) { match in
                if let endConstraint, match.end != endConstraint {
                    return
                }
                try appendDistinct(match, builder: &builder, seen: &seen)
            }
        }
    }

    private func appendDistinct(
        _ match: borrowing SPARQLPropertyPathMatch,
        builder: inout SPARQLPropertyPathMatchBuilder,
        seen: inout SPARQLPropertyPathMatchSet
    ) throws {
        try workMeter.consume(at: .deduplication)
        guard !seen.contains(match) else { return }
        try builder.checkAppendAllowed()
        let inserted = try seen.insert(match)
        precondition(inserted, "Distinctness changed after duplicate probe")
        try builder.appendBorrowed(match)
    }

    private func makeMatchBuilder(
        expectedCount: Int = 0
    ) throws -> SPARQLPropertyPathMatchBuilder {
        try SPARQLPropertyPathMatchBuilder.makeIntermediate(
            workMeter: workMeter,
            expectedCount: expectedCount
        )
    }

    private func boundRDFTerm(
        _ term: ExecutionTerm
    ) throws -> RDFTerm? {
        guard term.isBound else { return nil }
        guard let value = term.literalValue,
              case .rdfTerm(let rdfTerm) = value else {
            throw SPARQLQueryError.invalidRDFTerm(term.description)
        }
        return rdfTerm
    }

    private func validateConfiguration() throws {
        guard configuration.maximumExpressionDepth >= 0,
              configuration.maximumTraversalDepth >= 0,
              configuration.maximumResults >= 0 else {
            throw SPARQLQueryError.invalidPropertyPathConfiguration(
                maximumExpressionDepth: configuration.maximumExpressionDepth,
                maximumTraversalDepth: configuration.maximumTraversalDepth,
                maximumResults: configuration.maximumResults
            )
        }
    }

    private func addIndexScans(
        _ count: Int,
        to statistics: inout ExecutionStatistics
    ) throws {
        let (result, overflow) = statistics.indexScans
            .addingReportingOverflow(count)
        guard !overflow else {
            throw SPARQLPropertyPathExecutionError.statisticsOverflow
        }
        statistics.indexScans = result
    }

    private func mergeStatistics(
        _ other: borrowing ExecutionStatistics,
        into statistics: inout ExecutionStatistics
    ) throws {
        guard statistics.joinStrategies.isEmpty,
              statistics.joinFallbackReasons.isEmpty,
              other.joinStrategies.isEmpty,
              other.joinFallbackReasons.isEmpty else {
            throw SPARQLPropertyPathExecutionError.unexpectedJoinStatistics
        }
        statistics.indexScans = try checkedAdd(
            statistics.indexScans,
            other.indexScans
        )
        statistics.joinOperations = try checkedAdd(
            statistics.joinOperations,
            other.joinOperations
        )
        statistics.intermediateResults = try checkedAdd(
            statistics.intermediateResults,
            other.intermediateResults
        )
        statistics.patternsEvaluated = try checkedAdd(
            statistics.patternsEvaluated,
            other.patternsEvaluated
        )
        statistics.optionalMisses = try checkedAdd(
            statistics.optionalMisses,
            other.optionalMisses
        )
        let (duration, durationOverflow) = statistics.durationNs
            .addingReportingOverflow(other.durationNs)
        guard !durationOverflow else {
            throw SPARQLPropertyPathExecutionError.statisticsOverflow
        }
        statistics.durationNs = duration
    }

    private func checkedAdd(_ lhs: Int, _ rhs: Int) throws -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw SPARQLPropertyPathExecutionError.statisticsOverflow
        }
        return result
    }

    private func checkedIncrement(_ value: Int) throws -> Int {
        let (result, overflow) = value.addingReportingOverflow(1)
        guard !overflow else {
            throw SPARQLPropertyPathExecutionError.statisticsOverflow
        }
        return result
    }
}

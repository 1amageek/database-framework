import DatabaseKit
import DatabaseTypes
import StorageKit

extension DatabaseContext {
    /// Mutation supplies an admitted positional frame and retains every returned
    /// payload lease through its persistence boundary. Scalar/lazy semantics
    /// remain the same selected path used by SELECT.
    static func evaluateCanonicalScalar(
        _ bound: CanonicalBoundExpression, on row: CanonicalSourceRow,
        temporalEnvironment: SQLTemporalEnvironment? = nil, session: SQLSessionBinding? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> (value: QueryValue, leases: [DatabaseQueryPayloadLease]) {
        guard row.layout.reservation.map({ $0.workMeter === workMeter }) ?? true,
              row.layout.payloadLeases.allSatisfy({ $0.workMeter === workMeter }),
              row.payloadLeases.allSatisfy({ $0.workMeter === workMeter }) else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let validationClaim = try workMeter.reserveIntermediate(at: .expressionEvaluation)
        do {
            @AdmittedArray var pending: [CanonicalBoundExpression] = []
            try CanonicalBindingStorage.append(bound, to: &$pending, reservation: validationClaim)
            while let expression = pending.popLast() {
                try workMeter.checkpoint(at: .expressionEvaluation)
                switch expression.expression {
                case .aggregate, .subquery, .arraySubquery, .exists, .inSubquery,
                     .quantifiedComparison(_, _, _, .subquery):
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar execution requires a context-free expression")
                default:
                    for child in expression.children {
                        try CanonicalBindingStorage.append(child, to: &$pending, reservation: validationClaim)
                    }
                }
            }
        }
        validationClaim.release()
        let result: ProjectedQueryValue
        if let session {
            result = try await ActiveSQLSession.$binding.withValue(session) {
                try await evaluateSelectedValue(bound, on: row, context: nil, databaseContext: nil,
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter)
            }
        } else {
            result = try await evaluateSelectedValue(bound, on: row, context: nil, databaseContext: nil,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        }
        return (result.value, result.leases)
    }

    struct ProjectedQueryValue: Sendable {
        let value: QueryValue
        let leases: [DatabaseQueryPayloadLease]
        let payloadIsRetained: Bool
    }

    func evaluateBoundBoolean(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> Bool {
        let cell = try await evaluateBoundValue(bound, on: row, context: context, workMeter: workMeter)
        switch cell.value {
        case .primitive(.bool(let value)): return value
        case .primitive(.null): return false
        default: throw CanonicalReadError.expressionEvaluation(.typeMismatch(operation: "bound predicate"))
        }
    }

    func evaluateBoundScopedValue(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseQueryScopedFieldValue {
        let cell = try await evaluateBoundValue(bound, on: row, group: group,
            context: context, workMeter: workMeter)
        let value: FieldValue
        switch cell.value {
        case .primitive(let primitive): value = primitive
        case .numeric(let numeric): value = .decimal(numeric.value)
        default:
            throw CanonicalReadError.unsupportedSelectQuery("This bound scalar kernel does not accept rich values")
        }
        // The destination scalar owns its claim after a nested result releases
        // its original owner; source and derived COW payloads remain immutable.
        return try .retaining(value, workMeter: workMeter, stage: .expressionEvaluation)
    }

    func evaluateBoundValue(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        return try await Self.evaluateSelectedValue(bound, on: row, group: group,
            effectiveRow: suppliedRow, context: context, databaseContext: self, workMeter: workMeter)
    }

    private enum ScalarInstruction: Sendable {
        case evaluate(CanonicalBoundExpression)
        case kernel(CanonicalBoundExpression, Int)
        case coalesce(CanonicalBoundExpression, Int)
        case extreme(CanonicalBoundExpression, Int, ProjectedQueryValue?)
        case boolean(CanonicalBoundExpression)
        case normalize(CanonicalBoundExpression)
        case caseOperand(CanonicalBoundExpression)
        case caseCondition(CanonicalBoundExpression, Int, ProjectedQueryValue?, ProjectedQueryValue?)
    }

    static func evaluateSelectedValue(
        _ root: CanonicalBoundExpression, on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil, effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?, databaseContext: DatabaseContext?,
        temporalEnvironment suppliedTemporalEnvironment: SQLTemporalEnvironment? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        // A synchronous instruction run can otherwise return into an unbounded
        // immediate caller continuation chain. Suspend once at the async adapter,
        // independent of expression size, rather than once per scalar node.
        await Task.yield()
        let temporal = suppliedTemporalEnvironment ?? context?.options.temporalEnvironment ?? databaseContext?.inheritedSQLTemporalEnvironment
        let overlayClaim = try suppliedRow == nil ? workMeter.reserveIntermediate(bytes:
            prospectiveSourceCompositionFootprint(row, outer: context?.outerRow, workMeter: workMeter).bytes,
            at: .expressionEvaluation) : nil
        let effective = suppliedRow ?? row.overlaying(outer: context?.outerRow)
        defer { withExtendedLifetime(overlayClaim) {} }
        if root.columnSlot != nil, group == nil, root.window == nil {
            do {
                try workMeter.checkpoint(at: .expressionEvaluation)
                try workMeter.consume(at: .expressionEvaluation)
                guard let result = try selectedReference(root, on: effective, group: nil, context: context,
                    databaseContext: databaseContext, temporalEnvironment: temporal, workMeter: workMeter) else {
                    throw CanonicalReadError.unsupportedSelectQuery("A positional root has no admitted frame value")
                }
                try workMeter.checkpoint(at: .expressionEvaluation)
                return result
            } catch { throw selectedEvaluationFailure(error, occurrence: root.occurrence) }
        }
        let scratch = try workMeter.reserveIntermediate(at: .expressionEvaluation)
        defer { scratch.release() }
        @AdmittedArray var instructions: [ScalarInstruction] = []
        @AdmittedArray var values: [ProjectedQueryValue] = []
        func push(_ instruction: ScalarInstruction) throws {
            try CanonicalBindingStorage.append(instruction, to: &$instructions, reservation: scratch)
        }
        func append(_ value: ProjectedQueryValue) throws {
            try CanonicalBindingStorage.append(value, to: &$values, reservation: scratch)
        }
        func isAnd(_ expression: Expression) -> Bool {
            if case .and = expression { return true }; return false
        }
        func normalized(_ value: ProjectedQueryValue, to bound: CanonicalBoundExpression) throws -> ProjectedQueryValue {
            ProjectedQueryValue(value: try group != nil && value.value.isNull
                ? CanonicalQueryTypeBinder.normalize(value.value, to: bound.type) : bound.normalize(value.value),
                leases: value.leases, payloadIsRetained: value.payloadIsRetained)
        }
        func scheduleCase(_ bound: CanonicalBoundExpression, index: Int, operand: ProjectedQueryValue?) throws {
            guard case .caseWhen(let original, let pairs, let fallback) = bound.expression else {
                throw CanonicalReadError.unsupportedSelectQuery("Invalid selected CASE instruction")
            }
            let offset = original == nil ? 0 : 1
            if index == pairs.count {
                if fallback != nil { try push(.normalize(bound)); try push(.evaluate(bound.children[offset + pairs.count * 2])) }
                else { try append(normalized(.init(value: .primitive(.null), leases: [], payloadIsRetained: false), to: bound)) }
                return
            }
            let condition = bound.children[offset + index]
            if let operand {
                guard case .equal = condition.expression, condition.children.count == 2 else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "simple CASE selected equality") }
                let left: ProjectedQueryValue
                do { left = try coerceSelectedComparisonValue(operand.value, from: bound.children[0].type,
                    to: condition.children[0].type, temporalEnvironment: temporal, workMeter: workMeter) }
                catch let cause as DatabaseExpressionEvaluationError { throw CanonicalReadError.expressionEvaluation(cause, occurrence: condition.occurrence) }
                try push(.caseCondition(bound, index, operand, left)); try push(.evaluate(condition.children[1]))
            } else { try push(.caseCondition(bound, index, nil, nil)); try push(.evaluate(condition)) }
        }
        try push(.evaluate(root))
        var occurrence = root.occurrence
        do {
            while let instruction = instructions.popLast() {
                try workMeter.checkpoint(at: .expressionEvaluation)
                switch instruction {
                case .evaluate(let bound):
                    occurrence = bound.occurrence
                    try workMeter.consume(at: .expressionEvaluation)
                    if let value = try selectedReference(bound, on: effective, group: group, context: context,
                        databaseContext: databaseContext, temporalEnvironment: temporal, workMeter: workMeter) {
                        try append(value); continue
                    }
                    switch bound.expression {
                    case .defaultValue: throw DatabaseExpressionEvaluationError.unsupportedExpression("DEFAULT requires a selected mutation column")
                    case .caseWhen(let operand, _, _):
                        if operand != nil { try push(.caseOperand(bound)); try push(.evaluate(bound.children[0])) }
                        else { try scheduleCase(bound, index: 0, operand: nil) }
                    case .coalesce:
                        if bound.children.isEmpty { try append(normalized(.init(value: .primitive(.null), leases: [], payloadIsRetained: false), to: bound)) }
                        else { try push(.coalesce(bound, 0)); try push(.evaluate(bound.children[0])) }
                    case .greatest, .least:
                        if bound.children.isEmpty { try append(normalized(.init(value: .primitive(.null), leases: [], payloadIsRetained: false), to: bound)) }
                        else { try push(.extreme(bound, 0, nil)); try push(.evaluate(bound.children[0])) }
                    case .and, .or:
                        try push(.boolean(bound)); try push(.evaluate(bound.children[0]))
                    case .arraySubquery, .subquery, .exists, .inSubquery, .aggregate,
                         .quantifiedComparison(_, _, _, .subquery):
                        await Task.yield()
                        let value = try await evaluateSelectedContext(bound, on: row, group: group,
                            effectiveRow: effective, context: context, databaseContext: databaseContext,
                            temporalEnvironment: temporal, workMeter: workMeter)
                        await Task.yield()
                        try append(value)
                    default:
                        if case .literal(let literal) = bound.expression {
                            let value: QueryValue
                            if case .queryValue(let rich, _) = literal { value = rich }
                            else { value = .primitive(try literal.toFieldValue()) }
                            let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: value,
                                workMeter: workMeter, stage: .expressionEvaluation)
                            let claim = try workMeter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
                            try append(normalized(.init(value: value, leases: [DatabaseQueryPayloadLease(retaining: claim)], payloadIsRetained: true), to: bound))
                        } else {
                            try push(.kernel(bound, values.count))
                            for child in bound.children.reversed() { try push(.evaluate(child)) }
                        }
                    }
                case .kernel(let bound, let base):
                    occurrence = bound.occurrence
                    let operandsClaim = try workMeter.reserveIntermediate(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                        count: values.count - base, element: ProjectedQueryValue.self).bytes, at: .expressionEvaluation)
                    let children = Array(values[base...]); values.removeLast(values.count - base)
                    defer { withExtendedLifetime(operandsClaim) {} }
                    if case .function(let function) = bound.expression,
                       ["nextval", "currval", "lastval", "setval"].contains(function.name) {
                        let arguments = try children.map { cell -> FieldValue in
                            guard case .primitive(let value) = cell.value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function.name) }
                            return value
                        }
                        guard let value = try await SQLSequenceRuntime.evaluate(function.name, arguments: arguments, occurrence: bound.expression) else {
                            throw DatabaseExpressionEvaluationError.unsupportedFunction(function.name)
                        }
                        try append(normalized(.init(value: .primitive(value), leases: [], payloadIsRetained: false), to: bound))
                    } else {
                        try append(evaluateScalarKernel(bound, children: children, on: effective, context: context,
                            databaseContext: databaseContext, temporalEnvironment: temporal, workMeter: workMeter, groupingNulls: group != nil))
                    }
                case .normalize(let bound):
                    occurrence = bound.occurrence
                    let value = values.removeLast(); try append(normalized(value, to: bound))
                case .coalesce(let bound, let index):
                    occurrence = bound.occurrence
                    let value = values.removeLast()
                    if !value.value.isNull { try append(normalized(value, to: bound)) }
                    else if index + 1 < bound.children.count {
                        try push(.coalesce(bound, index + 1)); try push(.evaluate(bound.children[index + 1]))
                    } else { try append(normalized(.init(value: .primitive(.null), leases: [], payloadIsRetained: false), to: bound)) }
                case .extreme(let bound, let index, let current):
                    occurrence = bound.occurrence
                    let candidate = values.removeLast()
                    let selected: ProjectedQueryValue?
                    if candidate.value.isNull { selected = current }
                    else if let current {
                        let greatest: Bool
                        if case .greatest = bound.expression { greatest = true } else { greatest = false }
                        selected = try selectedComparison(candidate.value, current.value,
                            comparison: greatest ? .greaterThan : .lessThan, leftType: bound.type, rightType: bound.type,
                            temporalEnvironment: temporal, mode: bound.comparisonMode, workMeter: workMeter) == true ? candidate : current
                    } else { selected = candidate }
                    if index + 1 < bound.children.count {
                        try push(.extreme(bound, index + 1, selected)); try push(.evaluate(bound.children[index + 1]))
                    } else {
                        try append(normalized(selected ?? .init(value: .primitive(.null), leases: [], payloadIsRetained: false), to: bound))
                    }
                case .boolean(let bound):
                    occurrence = bound.occurrence
                    let left = values.last!
                    if (isAnd(bound.expression) && left.value == .primitive(.bool(false)))
                        || (!isAnd(bound.expression) && left.value == .primitive(.bool(true))) {
                        values.removeLast(); try append(normalized(left, to: bound))
                    } else { try push(.kernel(bound, values.count - 1)); try push(.evaluate(bound.children[1])) }
                case .caseOperand(let bound):
                    occurrence = bound.occurrence
                    try scheduleCase(bound, index: 0, operand: values.removeLast())
                case .caseCondition(let bound, let index, let operand, let left):
                    occurrence = bound.occurrence
                    let condition = values.removeLast(), offset = operand == nil ? 0 : 1
                    let matches: Bool
                    if let left {
                        let selected = bound.children[offset + index]
                        occurrence = selected.occurrence
                        matches = try selectedComparison(left.value, condition.value, comparison: .equal,
                            leftType: selected.children[0].type, rightType: selected.children[1].type,
                            temporalEnvironment: temporal, mode: bound.comparisonMode, workMeter: workMeter) == true
                    } else {
                        switch condition.value {
                        case .primitive(.bool(true)): matches = true
                        case .primitive(.bool(false)), .primitive(.null): matches = false
                        default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "CASE condition")
                        }
                    }
                    if matches, case .caseWhen(_, let pairs, _) = bound.expression {
                        try push(.normalize(bound)); try push(.evaluate(bound.children[offset + pairs.count + index]))
                    } else { try scheduleCase(bound, index: index + 1, operand: operand) }
                }
            }
            guard values.count == 1 else { throw CanonicalReadError.unsupportedSelectQuery("Invalid selected evaluation stack") }
            try workMeter.checkpoint(at: .expressionEvaluation)
            return values.removeLast()
        } catch { throw selectedEvaluationFailure(error, occurrence: occurrence) }
    }

    private static func selectedEvaluationFailure(_ error: any Error, occurrence: QueryOccurrenceID?) -> any Error {
        switch error {
        case let cause as SQLTimeZoneError:
            return CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: occurrence)
        case let cause as DatabaseExpressionEvaluationError:
            return CanonicalReadError.expressionEvaluation(cause, occurrence: occurrence)
        case let cause as QueryValueError:
            return CanonicalReadError.expressionEvaluation(.queryValue(cause), occurrence: occurrence)
        case CanonicalReadError.expressionEvaluation(let cause, let child):
            return CanonicalReadError.expressionEvaluation(cause, occurrence: child ?? occurrence)
        default: return error
        }
    }

    static func selectedReference(_ bound: CanonicalBoundExpression, on effectiveRow: CanonicalSourceRow,
        group: CanonicalGroupedRow?, context: CanonicalQueryEvaluationContext?, databaseContext: DatabaseContext?,
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> ProjectedQueryValue? {
        func normalized(_ cell: ProjectedQueryValue) throws -> ProjectedQueryValue {
            ProjectedQueryValue(value: try group != nil && cell.value.isNull
                ? CanonicalQueryTypeBinder.normalize(cell.value, to: bound.type) : bound.normalize(cell.value),
                leases: cell.leases, payloadIsRetained: cell.payloadIsRetained)
        }
        if let group {
            for index in group.key.selectedLeaves.indices {
                let leaf = group.key.selectedLeaves[index]
                if try CanonicalBoundKey(expression: bound).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: leaf),
                    workMeter: workMeter, isVolatileSubquery: { databaseContext?.selectedPlanIsVolatile($0) ?? true }) {
                    guard let ordinal = group.key.activeLeaves.firstIndex(of: index) else {
                        return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
                    }
                    return try normalized(ProjectedQueryValue(value: group.key.values[ordinal],
                        leases: [group.rows.payloadLease], payloadIsRetained: true))
                }
            }
        }
        if case .grouping = bound.expression {
            guard let group else { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING requires a grouped query")) }
            var bits: Int32 = 0
            for child in bound.children {
                var index: Int?
                for candidate in group.key.selectedLeaves.indices {
                    if try CanonicalBoundKey(expression: child).isSemanticallyEquivalent(to: CanonicalBoundKey(expression: group.key.selectedLeaves[candidate]),
                        workMeter: workMeter, isVolatileSubquery: { databaseContext?.selectedPlanIsVolatile($0) ?? true }) { index = candidate; break }
                }
                guard let index else { throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("GROUPING argument is not a grouping expression")) }
                bits = (bits << 1) | (group.key.activeLeaves.contains(index) ? 0 : 1)
            }
            return try normalized(ProjectedQueryValue(value: .primitive(.int64(Int64(bits))), leases: [], payloadIsRetained: false))
        }
        if let window = bound.window {
            guard let cache = context?.windowValues,
                  let slot = cache.plans.firstIndex(where: { $0 === window }),
                  cache.results[slot].values.indices.contains(cache.row) else {
                throw CanonicalReadError.unsupportedSelectQuery("A window expression has no analytic evaluation frame")
            }
            let value = cache.results[slot].values[cache.row]
            return try normalized(ProjectedQueryValue(value: value.value,
                leases: value.leases + [cache.results[slot].payloadLease], payloadIsRetained: true))
        }
        if let slot = bound.columnSlot {
            guard effectiveRow.layout.bindingSlots.indices.contains(slot),
                  let physical = effectiveRow.layout.bindingSlots[slot].valueIndex,
                  effectiveRow.values.indices.contains(physical) else {
                throw CanonicalReadError.unsupportedSelectQuery("A bound frame column is outside its admitted row")
            }
            // Projection and analytic callback owners retain the source once; a borrowed column adds no lease vector.
            return try normalized(ProjectedQueryValue(value: effectiveRow.values[physical], leases: [], payloadIsRetained: true))
        }
        if let whole = bound.wholeRow {
            guard try effectiveRow.sourceIsPresent(at: whole.presenceIndex) else {
                return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            }
            let limits = selectedValueLimits(context: context, meter: workMeter)
            let fieldClaim = try workMeter.reserveIntermediate(bytes:
                DatabaseIntermediateCollectionMeter.arrayFootprint(count: whole.fieldSlots.count,
                    element: QueryValue.self).bytes, at: .expressionEvaluation)
            defer { fieldClaim.release() }
            let fields = try whole.fieldSlots.map { slot -> QueryValue in
                guard effectiveRow.layout.bindingSlots.indices.contains(slot),
                      let physical = effectiveRow.layout.bindingSlots[slot].valueIndex,
                      effectiveRow.values.indices.contains(physical) else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "whole-row field address")
                }
                return effectiveRow.values[physical]
            }
            let result = try CanonicalSQLCompositeEvaluator.construct(type: whole.type, fields: fields, workMeter: workMeter, limits: limits)
            let leases = effectiveRow.payloadLeases + effectiveRow.layout.payloadLeases + (result.lease.map { [$0] } ?? [])
            return try normalized(ProjectedQueryValue(value: result.value, leases: leases, payloadIsRetained: true))
        }
        return nil
    }

    static func evaluateSelectedContext(
        _ bound: CanonicalBoundExpression,
        on row: CanonicalSourceRow,
        group: CanonicalGroupedRow? = nil,
        effectiveRow suppliedRow: CanonicalSourceRow? = nil,
        context: CanonicalQueryEvaluationContext?,
        databaseContext: DatabaseContext?,
        temporalEnvironment suppliedTemporalEnvironment: SQLTemporalEnvironment? = nil,
        workMeter: DatabaseWorkMeter
    ) async throws -> ProjectedQueryValue {
        let temporalEnvironment = suppliedTemporalEnvironment ?? context?.options.temporalEnvironment ?? databaseContext?.inheritedSQLTemporalEnvironment
        do {
        let claim = try workMeter.reserveIntermediate(
            bytes: UInt64(bound.children.count) * 512 + 256, at: .expressionEvaluation)
        defer { claim.release() }
        let overlayClaim: DatabaseIntermediateReservation?
        let effectiveRow: CanonicalSourceRow
        if let suppliedRow { effectiveRow = suppliedRow; overlayClaim = nil }
        else {
            overlayClaim = try workMeter.reserveIntermediate(
                bytes: prospectiveSourceCompositionFootprint(row, outer: context?.outerRow,
                    workMeter: workMeter).bytes, at: .expressionEvaluation)
            effectiveRow = row.overlaying(outer: context?.outerRow)
        }
        defer { overlayClaim?.release() }
        func child(_ index: Int) async throws -> ProjectedQueryValue {
            guard bound.children.indices.contains(index) else {
                throw CanonicalReadError.unsupportedSelectQuery("An executable expression has no selected child")
            }
            return try await evaluateSelectedValue(bound.children[index], on: row, group: group,
                effectiveRow: effectiveRow, context: context, databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        }
        func normalized(_ cell: ProjectedQueryValue) throws -> ProjectedQueryValue {
            ProjectedQueryValue(value: try (group != nil && cell.value.isNull
                ? CanonicalQueryTypeBinder.normalize(cell.value, to: bound.type) : bound.normalize(cell.value)), leases: cell.leases,
                payloadIsRetained: cell.payloadIsRetained)
        }
        #if DATABASE_MULTI_BASE
        if bound.subqueryBinding != nil, let context, let dispatch = context.sourceEvaluation {
            guard let query = context.sourceQuery, let plan = context.boundPlan, let binding = context.queryBinding else {
                throw CanonicalReadError.unsupportedSelectQuery("A Composition source read lost its selected query scope")
            }
            // Provenance selects authority only when a selected nested query actually reads a source.
            // Global scalar, grouping and window evaluation keeps the dispatcher for each input row.
            return try await dispatch(row.annotations) { origin, session in
                let active = try origin.compositionEvaluationContext(query, plan: plan, binding: binding,
                    execution: context.options, session: session).usingWindows(context.windowValues)
                return try await Self.evaluateSelectedValue(bound, on: row, group: group,
                    effectiveRow: effectiveRow, context: active, databaseContext: origin, workMeter: workMeter)
            }
        }
        #endif
        switch bound.expression {
        case .quantifiedComparison(_, let comparison, let quantifier, let source):
            let left = try await child(0)
            var sawNull = false
            func examine(_ member: QueryValue, sourceType: QueryColumnType) throws -> Bool? {
                try workMeter.consume(at: .expressionEvaluation)
                let selected = try coerceSelectedComparisonValue(member, from: sourceType,
                    to: bound.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                defer { withExtendedLifetime(selected) {} }
                guard let value = try selectedComparison(left.value, selected.value, comparison: comparison, leftType: bound.children[0].type, rightType: bound.children[0].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) else {
                    sawNull = true
                    return nil
                }
                return value
            }
            switch source {
            case .array:
                let array = try await child(1)
                if array.value.isNull {
                    return ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)
                }
                guard case .array(let value) = array.value else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "ANY/ALL array")
                }
                for member in value.elements {
                    if let result = try examine(member, sourceType: .sql(value.elementType)),
                       (quantifier == .any && result) || (quantifier == .all && !result) {
                        return ProjectedQueryValue(value: .primitive(.bool(result)), leases: [], payloadIsRetained: false)
                    }
                }
            case .subquery(let query):
                guard let plan = bound.subqueryBinding, !plan.columns.isEmpty else {
                    throw CanonicalReadError.invalidMembershipSubquery(columnCount: bound.subqueryBinding?.columns.count ?? 0)
                }
                guard let databaseContext else {
                    throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar subquery requires a query context")
                }
                let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan, outerRow: effectiveRow, context: context)
                let rows = response.visibleRows
                for index in 0..<rows.count {
                    var fields: [QueryValue] = []
                    rows.withElement(at: index) { fields = $0.values }
                    let member = try selectedMembershipValue(fields, columns: plan.columns, bound: bound,
                        context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    let result = try selectedComparison(left.value, member.value, comparison: comparison,
                        leftType: bound.children[0].type, rightType: bound.children[0].type,
                        temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                    if result == nil { sawNull = true }
                    if let result, (quantifier == .any && result) || (quantifier == .all && !result) {
                        return ProjectedQueryValue(value: .primitive(.bool(result)), leases: [], payloadIsRetained: false)
                    }
                }
            }
            return ProjectedQueryValue(value: .primitive(sawNull ? .null : .bool(quantifier == .all)),
                leases: [], payloadIsRetained: false)
        case .arraySubquery(let query):
            guard let plan = bound.subqueryBinding, plan.columns.count == 1, let databaseContext else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: nil, columnCount: bound.subqueryBinding?.columns.count)
            }
            let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan,
                outerRow: effectiveRow, context: context)
            let rows = response.visibleRows
            let scratch = try workMeter.reserveIntermediate(bytes: DatabaseIntermediateCollectionMeter.arrayFootprint(
                count: rows.count, element: QueryValue.self).bytes, at: .expressionEvaluation)
            defer { scratch.release() }
            var values: [QueryValue] = []; values.reserveCapacity(rows.count)
            for index in 0..<rows.count { rows.withElement(at: index) { values.append($0.values[0]) } }
            let result = try CanonicalSQLArrayEvaluator.assembleSubquery(values, columnType: plan.columns[0].type,
                resultType: bound.type, temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                limits: selectedValueLimits(context: context, meter: workMeter))
            return try normalized(ProjectedQueryValue(value: result.value,
                leases: response.rows.originPayloadLeases + (result.lease.map { [$0] } ?? []), payloadIsRetained: true))
        case .subquery(let query), .exists(let query), .inSubquery(_, let query):
            guard let plan = bound.subqueryBinding else {
                throw CanonicalReadError.unsupportedSelectQuery("A nested expression has no executable SELECT binding")
            }
            let candidate: ProjectedQueryValue?
            if case .inSubquery = bound.expression { candidate = try await child(0) } else { candidate = nil }
            guard let databaseContext else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar subquery requires a query context")
            }
            if case .exists = bound.expression, query.quantifier == .all,
               !canonicalQueryRequiresAggregation(query) {
                let exists = try await databaseContext.executeExists(query, plan: plan,
                    outerRow: effectiveRow, context: context)
                return ProjectedQueryValue(value: .primitive(.bool(exists)), leases: [], payloadIsRetained: false)
            }
            let response = try await databaseContext.executeNestedQuery(query, boundPlan: plan,
                outerRow: effectiveRow, context: context)
            let rows = response.visibleRows
            if case .exists = bound.expression {
                return ProjectedQueryValue(value: .primitive(.bool(!rows.isEmpty)), leases: [], payloadIsRetained: false)
            }
            if case .sql(.composite(let type)) = bound.type, plan.columns.count != 1 {
                guard rows.count <= 1 else { throw CanonicalReadError.invalidScalarSubquery(rowCount: rows.count, columnCount: nil) }
                if rows.isEmpty { return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false)) }
                var fields: [QueryValue] = []
                rows.withElement(at: 0) { fields = $0.values }
                let fieldClaim = try workMeter.reserveIntermediate(bytes:
                    DatabaseIntermediateCollectionMeter.arrayFootprint(count: fields.count,
                        element: ProjectedQueryValue.self).multiplied(by: 2).bytes, at: .expressionEvaluation)
                defer { fieldClaim.release() }
                var selected: [ProjectedQueryValue] = []; selected.reserveCapacity(fields.count)
                for index in fields.indices {
                    selected.append(try coerceSelectedComparisonValue(fields[index], from: plan.columns[index].type,
                        to: type.columns[index].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
                }
                let result = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: selected.map { $0.value },
                    workMeter: workMeter, limits: selectedValueLimits(context: context, meter: workMeter))
                return try normalized(ProjectedQueryValue(value: result.value,
                    leases: response.rows.originPayloadLeases + selected.flatMap { $0.leases }
                        + (result.lease.map { [$0] } ?? []), payloadIsRetained: true))
            }
            if let candidate {
                var sawNull = false
                for index in 0..<rows.count {
                    var fields: [QueryValue] = []
                    rows.withElement(at: index) { fields = $0.values }
                    let member = try selectedMembershipValue(fields, columns: plan.columns, bound: bound,
                        context: context, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    let result = try selectedComparison(candidate.value, member.value, comparison: .equal,
                        leftType: bound.children[0].type, rightType: bound.children[0].type,
                        temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                    if result == true { return ProjectedQueryValue(value: .primitive(.bool(true)), leases: [], payloadIsRetained: false) }
                    if result == nil { sawNull = true }
                }
                return ProjectedQueryValue(value: .primitive(sawNull ? .null : .bool(false)), leases: [], payloadIsRetained: false)
            }
            guard plan.columns.count == 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: nil, columnCount: plan.columns.count)
            }
            guard rows.count <= 1 else {
                throw CanonicalReadError.invalidScalarSubquery(rowCount: rows.count, columnCount: nil)
            }
            if rows.isEmpty {
                return try normalized(ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            }
            var value: QueryValue?
            rows.withElement(at: 0) { value = $0.values[0] }
            guard let value else { throw CanonicalReadError.invalidScalarSubquery(rowCount: 0, columnCount: 1) }
            return try normalized(ProjectedQueryValue(value: value, leases: response.rows.originPayloadLeases,
                payloadIsRetained: true))
        case .aggregate(let aggregate):
            guard let databaseContext else {
                throw DatabaseExpressionEvaluationError.unsupportedExpression("scalar aggregate requires a query context")
            }
            guard let group else {
                throw CanonicalReadError.aggregateEvaluation(.invalidGroupedExpression("An aggregate requires a group"))
            }
            if bound.aggregate != nil {
                return try await SQLAggregateEvaluator.evaluate(bound: bound, rowCount: group.rows.count,
                    evaluate: { index, selected in
                        let member = group.rows[index]
                        let value = try await evaluateSelectedValue(selected, on: member, context: context,
                            databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                        return ProjectedQueryValue(value: value.value,
                            leases: value.leases + member.payloadLeases + [group.rows.payloadLease], payloadIsRetained: value.payloadIsRetained)
                    }, direct: { selected in
                        try await evaluateSelectedValue(selected, on: row, group: group, context: context,
                            databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    }, temporalEnvironment: temporalEnvironment, workMeter: workMeter,
                    limits: selectedValueLimits(context: context, meter: workMeter))
            }
            if case .sum = aggregate {
                return try await databaseContext.evaluateNumericAggregate(aggregate, bound: bound, rows: group.rows,
                    workMeter: workMeter, evaluationContext: context)
            }
            if case .avg = aggregate {
                return try await databaseContext.evaluateNumericAggregate(aggregate, bound: bound, rows: group.rows,
                    workMeter: workMeter, evaluationContext: context)
            }
            if case .count(_, let distinct) = aggregate, let input = bound.children.first,
               CanonicalSQLValueSemantics.isTemporal(input.type) || selectedCollectionType(input.type) {
                let scratch = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<CanonicalSQLValueIdentity>.stride + 32))
                    .multiplied(by: UInt64(distinct ? group.rows.count : 0))
                let reservation = try workMeter.reserveIntermediate(bytes: scratch.bytes, at: .aggregateInput)
                defer { reservation.release() }
                var seen = try CanonicalSQLIdentitySet(workMeter: workMeter,
                    stage: .aggregateInput, expectedCount: distinct ? group.rows.count : 0)
                var count: Int64 = 0
                for member in group.rows {
                    let cell = try await evaluateSelectedValue(input, on: member, context: context,
                        databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    guard !cell.value.isNull else { continue }
                    if distinct {
                        let key = try databaseContext.canonicalQueryValueIdentity(cell.value, operation: "COUNT(DISTINCT)",
                            type: input.type, workMeter: workMeter)
                        guard try seen.insert(key) else { continue }
                    }
                    let increment = count.addingReportingOverflow(1)
                    guard !increment.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    count = increment.partialValue
                }
                return try normalized(ProjectedQueryValue(value: .primitive(.int64(count)), leases: [], payloadIsRetained: false))
            }
            switch aggregate {
            case .min, .max, .sample:
                guard case .sql = bound.type else { break }
                guard let input = bound.children.first else {
                    throw CanonicalReadError.unsupportedSelectQuery("A scalar aggregate has no selected input")
                }
                var selected: ProjectedQueryValue?
                for row in group.rows {
                    try workMeter.consume(at: .aggregateInput)
                    let cell = try await evaluateSelectedValue(input, on: row, context: context, databaseContext: databaseContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                    guard !cell.value.isNull else { continue }
                    if let current = selected {
                        switch aggregate {
                        case .min:
                            if try selectedComparison(cell.value, current.value, comparison: .lessThan, leftType: input.type, rightType: input.type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true { selected = cell }
                        case .max:
                            if try selectedComparison(cell.value, current.value, comparison: .greaterThan, leftType: input.type, rightType: input.type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter) == true { selected = cell }
                        default: break
                        }
                    } else { selected = cell }
                }
                return try normalized(selected ?? ProjectedQueryValue(value: .primitive(.null), leases: [], payloadIsRetained: false))
            default: break
            }
            let value = try await databaseContext.evaluateAggregate(aggregate, bound: bound, rows: group.rows,
                workMeter: workMeter, evaluationContext: context)
            let result = try value.withValue { try bound.normalize(.primitive(copy $0)) }
            return ProjectedQueryValue(value: result,
                leases: value.retainedReservation.map { [DatabaseQueryPayloadLease(retaining: $0)] } ?? [],
                payloadIsRetained: value.retainedReservation != nil)
        default: throw CanonicalReadError.unsupportedSelectQuery("Invalid contextual expression instruction")
        }
        } catch let cause as SQLTimeZoneError {
            throw CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: bound.occurrence)
        } catch let cause as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: bound.occurrence)
        } catch let cause as QueryValueError {
            throw CanonicalReadError.expressionEvaluation(.queryValue(cause), occurrence: bound.occurrence)
        } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: occurrence ?? bound.occurrence)
        }
    }

    static func selectedValueLimits(context: CanonicalQueryEvaluationContext?, meter: DatabaseWorkMeter) -> QueryValueLimits {
        let structural = context?.options.queryStructuralLimits ?? .default
        return QueryValueLimits(maximumRetainedBytes: min(structural.maximumRetainedBytes, meter.budget.maximumIntermediateBytes),
            maximumTotalNodes: min(structural.maximumTotalNodes, meter.budget.maximumWorkUnits),
            maximumNestingDepth: min(structural.maximumNestingDepth, meter.budget.maximumWorkUnits),
            maximumCollectionElements: min(structural.maximumCollectionElements, meter.budget.maximumWorkUnits))
    }

    static func selectedCollectionType(_ type: QueryColumnType?) -> Bool {
        switch type {
        case .sql(.array)?, .sql(.json)?, .sql(.jsonb)?, .sql(.composite)?: return true
        default: return false
        }
    }

    static func selectedCollectionCast(_ bound: CanonicalBoundExpression) -> Bool {
        if selectedCollectionType(bound.type) { return true }
        return bound.children.first?.type == .sql(.jsonb) && !CanonicalSQLScalarCast.usesPayload(bound.type)
    }

    static func selectedArrayFunction(_ name: String) -> Bool {
        switch name {
        case "array_length", "array_lower", "array_upper", "array_ndims", "array_dims", "cardinality",
             "array_append", "array_prepend", "array_cat", "array_position", "array_positions",
             "array_remove", "array_replace", "array_to_string", "string_to_array": return true
        default: return false
        }
    }

    static func selectedJSONFunction(_ name: String) -> Bool {
        switch name {
        case "to_json", "to_jsonb", "array_to_json", "row_to_json", "json_build_array", "jsonb_build_array",
             "json_build_object", "jsonb_build_object", "json_typeof", "jsonb_typeof",
             "json_array_length", "jsonb_array_length", "json_extract_path", "jsonb_extract_path",
             "json_extract_path_text", "jsonb_extract_path_text", "jsonb_set", "jsonb_insert",
             "jsonb_strip_nulls": return true
        default: return false
        }
    }

    static func selectedNumericCastTarget(_ target: DataType) -> Bool {
        switch target {
        case .smallint, .integer, .bigint, .real, .doublePrecision, .decimal, .boolean: return true
        default: return false
        }
    }

    static func selectedNumericCastSource(_ source: QueryColumnType) -> Bool {
        switch source {
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.real), .sql(.doublePrecision),
             .sql(.decimal), .sql(.boolean): return true
        case .primitive(let kind, false):
            switch kind {
            case .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal: return true
            default: return false
            }
        default: return false
        }
    }

    static func selectedScalarCastTarget(_ target: QueryColumnType) -> Bool {
        if CanonicalQueryTypeBinder.temporalType(target) != nil { return true }
        if CanonicalSQLScalarCast.usesPayload(target) { return true }
        switch target {
        case .sql(let type): return type == .uuid || selectedNumericCastTarget(type)
        case .primitive(let kind, false):
            switch kind {
            case .uuid, .bool, .int8, .int16, .int32, .int64, .uint8, .uint16, .uint32, .uint64,
                 .float32, .float64, .decimal: return true
            default: return false
            }
        default: return false
        }
    }

    static func selectedNumericFunction(_ name: String) -> Bool {
        switch name.lowercased() {
        case "abs", "sign", "ceil", "ceiling", "floor", "round", "trunc", "mod", "div", "gcd", "lcm",
             "sqrt", "cbrt", "power", "exp", "ln", "log", "log10", "scale", "min_scale", "trim_scale",
             "pi", "degrees", "radians", "sin", "cos", "tan", "sind", "cosd", "tand", "cot", "cotd",
             "asin", "acos", "atan", "atan2", "asind", "acosd", "atand", "atan2d",
             "sinh", "cosh", "tanh", "asinh", "acosh", "atanh", "width_bucket": return true
        default: return false
        }
    }

    static func selectedOrdering(_ left: QueryValue, _ right: QueryValue,
        bound: CanonicalBoundExpression, using key: SortKey,
        environment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter) throws -> QueryComparison {
        do {
            let selected = SortKey(key.expression, direction: key.direction,
                nulls: key.nulls ?? (key.direction == .ascending ? .last : .first))
            return try CanonicalSQLValueSemantics.compareValues(left, right, type: bound.type,
                using: selected, environment: environment, workMeter: workMeter)
        } catch let cause as SQLTimeZoneError {
            throw CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: bound.occurrence)
        } catch let cause as DatabaseExpressionEvaluationError {
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: bound.occurrence)
        }
    }

    static func selectedMembershipValue(_ fields: [QueryValue], columns: [QueryColumn],
        bound: CanonicalBoundExpression, context: CanonicalQueryEvaluationContext?,
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter
    ) throws -> ProjectedQueryValue {
        guard fields.count == columns.count else {
            throw CanonicalReadError.invalidMembershipSubquery(columnCount: fields.count)
        }
        if columns.count == 1 {
            return try coerceSelectedComparisonValue(fields[0], from: columns[0].type,
                to: bound.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
        }
        guard case .sql(.composite(let type)) = bound.children[0].type,
              bound.subqueryComparisonTypes.count == fields.count else {
            throw CanonicalReadError.invalidMembershipSubquery(columnCount: columns.count)
        }
        let claim = try workMeter.reserveIntermediate(bytes:
            DatabaseIntermediateCollectionMeter.arrayFootprint(count: fields.count,
                element: ProjectedQueryValue.self).multiplied(by: 2).bytes, at: .expressionEvaluation)
        defer { claim.release() }
        var selected: [ProjectedQueryValue] = []
        selected.reserveCapacity(fields.count)
        for index in fields.indices {
            selected.append(try coerceSelectedComparisonValue(fields[index], from: columns[index].type,
                to: bound.subqueryComparisonTypes[index], temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        }
        let result = try CanonicalSQLCompositeEvaluator.construct(type: type, fields: selected.map { $0.value },
            workMeter: workMeter, limits: selectedValueLimits(context: context, meter: workMeter))
        return ProjectedQueryValue(value: result.value,
            leases: selected.flatMap { $0.leases } + (result.lease.map { [$0] } ?? []), payloadIsRetained: true)
    }

    static func selectedComparison(
        _ left: QueryValue, _ right: QueryValue, comparison: DatabaseKit.ComparisonOperator,
        leftType: QueryColumnType, rightType: QueryColumnType,
        temporalEnvironment: SQLTemporalEnvironment? = nil, mode: CanonicalSQLCompositeComparisonMode = .record,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool? {
        try workMeter.consume(at: .expressionEvaluation)
        if left.isNull || right.isNull { return nil }
        if case .sql(.composite) = leftType {
            return try CanonicalSQLCompositeEvaluator.comparePredicate(left, right,
                leftType: leftType, rightType: rightType, comparison: comparison,
                mode: mode, environment: temporalEnvironment, workMeter: workMeter)
        }
        do {
            if comparison == .equal || comparison == .notEqual {
                let equal = try CanonicalSQLValueSemantics.equalValues(left, right,
                    leftType: leftType, rightType: rightType, environment: temporalEnvironment, workMeter: workMeter)
                return comparison == .equal ? equal : !equal
            }
            let result = try CanonicalSQLValueSemantics.compareValues(left, right,
                leftType: leftType, rightType: rightType, environment: temporalEnvironment, workMeter: workMeter)
            switch comparison {
            case .lessThan: return result == .lessThan
            case .lessThanOrEqual: return result != .greaterThan
            case .greaterThan: return result == .greaterThan
            case .greaterThanOrEqual: return result != .lessThan
            case .equal, .notEqual: preconditionFailure("Equality is handled before ordering")
            }
        } catch let failure as FieldValueComparisonError {
            throw canonicalComparisonReadError(failure, operation: "selected scalar comparison")
        }
    }

    static func coerceSelectedComparisonValue(
        _ value: QueryValue, from source: QueryColumnType, to target: QueryColumnType,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter
    ) throws -> ProjectedQueryValue {
        let result = try CanonicalSQLScalarCast.coerce(value, from: source, to: target,
            temporalEnvironment: temporalEnvironment, workMeter: workMeter,
            limits: selectedValueLimits(context: nil, meter: workMeter))
        return ProjectedQueryValue(value: result.value, leases: result.lease.map { [$0] } ?? [],
            payloadIsRetained: result.lease != nil || QueryValueValidator.accepts(value, type: target))
    }

    static func evaluateScalarKernel(
        _ bound: CanonicalBoundExpression,
        children: [ProjectedQueryValue],
        on row: CanonicalSourceRow,
        context: CanonicalQueryEvaluationContext?, databaseContext: DatabaseContext?,
        temporalEnvironment: SQLTemporalEnvironment? = nil, workMeter: DatabaseWorkMeter, groupingNulls: Bool = false
    ) throws -> ProjectedQueryValue {
        let claim = try workMeter.reserveIntermediate(bytes: UInt64(children.count) * 512 + 256,
            at: .expressionEvaluation)
        defer { claim.release() }
        let structural = context?.options.queryStructuralLimits ?? .default
        let limits = QueryValueLimits(
            maximumRetainedBytes: min(structural.maximumRetainedBytes, workMeter.budget.maximumIntermediateBytes),
            maximumTotalNodes: min(structural.maximumTotalNodes, workMeter.budget.maximumWorkUnits),
            maximumNestingDepth: min(structural.maximumNestingDepth, workMeter.budget.maximumWorkUnits),
            maximumCollectionElements: min(structural.maximumCollectionElements, workMeter.budget.maximumWorkUnits))
        func normalize(_ value: QueryValue) throws -> QueryValue {
            if groupingNulls, value.isNull {
                return try CanonicalQueryTypeBinder.normalize(value, to: bound.type)
            }
            return try bound.normalize(value)
        }
        func booleanResult(_ value: Bool?) throws -> ProjectedQueryValue {
            let result = value.map { QueryValue.primitive(.bool($0)) } ?? .primitive(.null)
            return ProjectedQueryValue(value: try normalize(result), leases: [], payloadIsRetained: false)
        }
        func payloadResult(_ scoped: consuming DatabaseQueryScopedFieldValue) throws -> ProjectedQueryValue {
            let result = try scoped.withValue { try normalize(.primitive(copy $0)) }
            var leases = children.flatMap { $0.leases }
            if let reservation = scoped.retainedReservation { leases.append(DatabaseQueryPayloadLease(retaining: reservation)) }
            return ProjectedQueryValue(value: result, leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        func patternResult() throws -> ProjectedQueryValue {
            let result = try CanonicalSQLPatternEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter, limits: limits)
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        func collectionResult(array: Bool) throws -> ProjectedQueryValue {
            let result: (value: QueryValue, lease: DatabaseQueryPayloadLease?)
            if case .cast = bound.expression {
                guard let input = bound.children.first, let value = children.first?.value else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected collection CAST arity")
                }
                result = try CanonicalSQLScalarCast.evaluateCollection(value, from: input.type, to: bound.type,
                    context: bound.castContext, temporalEnvironment: temporalEnvironment,
                    workMeter: workMeter, limits: limits)
            } else if array {
                result = try CanonicalSQLArrayEvaluator.evaluate(bound, operands: children.map { $0.value },
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            } else {
                result = try CanonicalSQLJSONEvaluator.evaluate(bound, operands: children.map { $0.value },
                    temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            }
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases,
                payloadIsRetained: !leases.isEmpty)
        }
        func temporalResult() throws -> ProjectedQueryValue {
            guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
            let value = try CanonicalSQLTemporalEvaluator.evaluate(bound, operands: children.map { $0.value },
                environment: temporalEnvironment, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(value), leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        }
        if case .cast = bound.expression, bound.sourceAlignment {
            guard let input = bound.children.first, let value = children.first?.value else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected source alignment arity")
            }
            let result = try CanonicalSQLScalarCast.alignSource(value, from: input.type, to: bound.type,
                temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            var leases = children.flatMap { $0.leases }
            if let lease = result.lease { leases.append(lease) }
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases,
                payloadIsRetained: !leases.isEmpty)
        }
        let compositeOperation: Bool
        switch bound.expression {
        case .row, .field, .fieldExpansion, .collate: compositeOperation = true
        case .equal, .notEqual, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual, .isNull, .isNotNull:
            compositeOperation = bound.children.contains { if case .sql(.composite) = $0.type { return true }; return false }
        default: compositeOperation = false
        }
        if compositeOperation {
            let result = try CanonicalSQLCompositeEvaluator.evaluate(bound, operands: children.map { $0.value },
                temporalEnvironment: temporalEnvironment, workMeter: workMeter, limits: limits)
            let leases = children.flatMap { $0.leases } + (result.lease.map { [$0] } ?? [])
            return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
        }
        switch bound.expression {
        case .truthTest(_, let target, let negated):
            let matches: Bool
            switch (children[0].value, target) {
            case (.primitive(.bool(true)), .true), (.primitive(.bool(false)), .false), (.primitive(.null), .unknown): matches = true
            case (.primitive(.bool), _), (.primitive(.null), _): matches = false
            default: throw DatabaseExpressionEvaluationError.typeMismatch(operation: "SQL truth test")
            }
            return try booleanResult(negated ? !matches : matches)
        case .isDistinctFrom(_, _, let negated):
            let left = children[0].value, right = children[1].value
            let equal = left.isNull || right.isNull ? left.isNull && right.isNull
                : try selectedComparison(left, right, comparison: .equal, leftType: bound.children[0].type,
                    rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment,
                    mode: bound.comparisonMode, workMeter: workMeter) == true
            return try booleanResult(negated ? equal : !equal)
        case .nullIf:
            let left = children[0], right = children[1]
            let equal = try selectedComparison(left.value, right.value, comparison: .equal,
                leftType: bound.children[0].type, rightType: bound.children[1].type,
                temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
            if equal == true { return ProjectedQueryValue(value: try normalize(.primitive(.null)), leases: [], payloadIsRetained: false) }
            return ProjectedQueryValue(value: try normalize(left.value), leases: left.leases, payloadIsRetained: left.payloadIsRetained)
        case .quantifiedComparison(_, let comparison, let quantifier, .array):
            if children[1].value.isNull { return try booleanResult(nil) }
            guard case .array(let array) = children[1].value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "ANY/ALL array") }
            var sawNull = false
            for member in array.elements {
                try workMeter.consume(at: .expressionEvaluation)
                let selected = try coerceSelectedComparisonValue(member, from: .sql(array.elementType),
                    to: bound.children[0].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
                defer { withExtendedLifetime(selected) {} }
                let result = try selectedComparison(children[0].value, selected.value, comparison: comparison,
                    leftType: bound.children[0].type, rightType: bound.children[0].type,
                    temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                if result == nil { sawNull = true }
                if let result, (quantifier == .any && result) || (quantifier == .all && !result) { return try booleanResult(result) }
            }
            return try booleanResult(sawNull ? nil : quantifier == .all)
        case .isNull, .isNotNull:
            let isNull = children[0].value.isNull
            if case .isNull = bound.expression { return try booleanResult(isNull) }
            return try booleanResult(!isNull)
        default: break
        }
        if case .function(let function) = bound.expression {
            if function.name == "merge_action" {
                guard let action = row.annotations["sql.merge.action"], case .string = action else {
                    throw DatabaseExpressionEvaluationError.unsupportedFunction(function.name)
                }
                return ProjectedQueryValue(value: try normalize(.primitive(action)), leases: [], payloadIsRetained: false)
            }
            if ["current_schema", "current_schemas", "current_database", "current_user", "session_user"].contains(function.name) {
                let arguments = try children.map { cell -> FieldValue in
                    guard case .primitive(let value) = cell.value else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: function.name) }
                    return value
                }
                if let result = try SQLSessionRuntime.evaluate(function.name, arguments: arguments,
                    resolution: context?.options.sqlResolution, databaseName: databaseContext?.inheritedSQLDatabaseName, workMeter: workMeter) {
                    return ProjectedQueryValue(value: try normalize(result.value), leases: result.lease.map { [$0] } ?? [], payloadIsRetained: result.lease != nil)
                }
            }
        }
        switch bound.expression {
        case .array, .arrayAccess: return try collectionResult(array: true)
        case .collectionOperation:
            let array: Bool
            if case .sql(.array)? = bound.children.first?.type { array = true }
            else { array = false }
            return try collectionResult(array: array)
        case .cast where selectedCollectionCast(bound): return try collectionResult(array: false)
        case .currentValue: return try temporalResult()
        case .equal, .notEqual, .lessThan, .lessThanOrEqual, .greaterThan, .greaterThanOrEqual:
            guard children.count == 2 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected comparison arity")
            }
            let comparison: DatabaseKit.ComparisonOperator
            switch bound.expression {
            case .equal: comparison = .equal
            case .notEqual: comparison = .notEqual
            case .lessThan: comparison = .lessThan
            case .lessThanOrEqual: comparison = .lessThanOrEqual
            case .greaterThan: comparison = .greaterThan
            case .greaterThanOrEqual: comparison = .greaterThanOrEqual
            default: preconditionFailure("The selected expression is a scalar comparison")
            }
            return try booleanResult(selectedComparison(children[0].value, children[1].value,
                comparison: comparison, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        case .between:
            guard children.count == 3 else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected BETWEEN arity")
            }
            let lower = try selectedComparison(children[0].value, children[1].value,
                comparison: .greaterThanOrEqual, leftType: bound.children[0].type, rightType: bound.children[1].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
            let upper = try selectedComparison(children[0].value, children[2].value,
                comparison: .lessThanOrEqual, leftType: bound.children[0].type, rightType: bound.children[2].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
            if lower == false || upper == false { return try booleanResult(false) }
            if lower == nil || upper == nil { return try booleanResult(nil) }
            return try booleanResult(true)
        case .inList, .notInList:
            guard let candidate = children.first else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected IN arity")
            }
            let negated: Bool
            if case .notInList = bound.expression { negated = true } else { negated = false }
            var sawNull = false
            for (index, member) in children.enumerated().dropFirst() {
                let result = try selectedComparison(candidate.value, member.value,
                    comparison: .equal, leftType: bound.children[0].type, rightType: bound.children[index].type, temporalEnvironment: temporalEnvironment, mode: bound.comparisonMode, workMeter: workMeter)
                if result == true { return try booleanResult(!negated) }
                if result == nil { sawNull = true }
            }
            return try booleanResult(sawNull ? nil : negated)
        case .add, .subtract, .multiply, .divide, .modulo, .negate:
            if case .subtract = bound.expression, bound.children.first?.type == .sql(.jsonb) {
                return try collectionResult(array: false)
            }
            if bound.children.contains(where: { CanonicalSQLValueSemantics.isTemporal($0.type) }) { return try temporalResult() }
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .cast(_, let target) where selectedNumericCastTarget(target)
            && bound.type == .sql(target)
            && bound.children.first.map({ selectedNumericCastSource($0.type) }) == true:
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .function(let function) where selectedNumericFunction(function.name):
            let result = try CanonicalSQLNumericEvaluator.evaluate(bound,
                operands: children.map { $0.value }, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result),
                leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .cast where selectedScalarCastTarget(bound.type):
            guard let input = bound.children.first, let value = children.first?.value else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected CAST arity")
            }
            if CanonicalSQLScalarCast.usesPayload(bound.type) {
                return try payloadResult(CanonicalSQLScalarCast.evaluatePayload(value, from: input.type,
                    to: bound.type, context: bound.castContext, temporalEnvironment: temporalEnvironment,
                    limits: limits, workMeter: workMeter))
            }
            let result = try CanonicalSQLScalarCast.evaluateValue(value, from: input.type,
                to: bound.type, context: bound.castContext, temporalEnvironment: temporalEnvironment, workMeter: workMeter)
            return ProjectedQueryValue(value: try normalize(result), leases: children.flatMap { $0.leases }, payloadIsRetained: false)
        case .patternMatch: return try patternResult()
        case .concatenate:
            if case .sql(.array) = bound.type { return try collectionResult(array: true) }
            if bound.type == .sql(.jsonb) { return try collectionResult(array: false) }
            if bound.type == .sql(.blob) {
                return try payloadResult(CanonicalSQLBinaryEvaluator.evaluate(bound, operands: children.map { $0.value }, workMeter: workMeter))
            }
            return try payloadResult(CanonicalSQLStringEvaluator.evaluate(bound, operands: children.map { $0.value }, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        case .function(let function):
            let name = function.name
            if selectedArrayFunction(name) { return try collectionResult(array: true) }
            if selectedJSONFunction(name) { return try collectionResult(array: false) }
            if ["to_char", "to_date"].contains(name) || (name == "to_timestamp" && children.count == 2) {
                guard let temporalEnvironment else { throw DatabaseExpressionEvaluationError.missingTemporalEnvironment }
                let result = try SQLTemporalFormatting.evaluate(bound, operands: children.map { $0.value },
                    environment: temporalEnvironment, workMeter: workMeter, limits: limits)
                var leases = children.flatMap { $0.leases }
                if let lease = result.lease { leases.append(lease) }
                return ProjectedQueryValue(value: try normalize(result.value), leases: leases, payloadIsRetained: !leases.isEmpty)
            }
            if ["now", "transaction_timestamp", "statement_timestamp", "clock_timestamp", "extract", "date_part", "date_trunc", "date_bin", "age",
                "make_date", "make_time", "make_timestamp", "make_timestamptz", "make_interval", "timezone", "to_timestamp"].contains(name) {
                return try temporalResult()
            }
            if ["regexp_like", "regexp_match", "regexp_replace", "regexp_split_to_array"].contains(name)
                || (name == "substring" && bound.children.count > 1 && bound.children[1].type == .sql(.text)) {
                return try patternResult()
            }
            if ["encode", "decode", "get_byte", "set_byte", "get_bit", "set_bit"].contains(name)
                || (["length", "octet_length", "bit_length", "substr", "substring"].contains(name)
                    && bound.children.first?.type == .sql(.blob)) {
                return try payloadResult(CanonicalSQLBinaryEvaluator.evaluate(bound, operands: children.map { $0.value }, workMeter: workMeter))
            }
            return try payloadResult(CanonicalSQLStringEvaluator.evaluate(bound, operands: children.map { $0.value }, temporalEnvironment: temporalEnvironment, workMeter: workMeter))
        default: break
        }
        let operands = children.enumerated().map { index, value in
            CanonicalBoundExpression(expression: .literal(.queryValue(value.value, type: bound.children[index].type)),
                type: bound.children[index].type, isNullable: bound.children[index].isNullable)
        }
        let executable = CanonicalBoundExpression(expression: bound.expression, type: bound.type,
            isNullable: bound.isNullable, children: operands, castContext: bound.castContext,
            sourceAlignment: bound.sourceAlignment)
        let expression = try executable.materializing(in: row.values)
        // Only selected, already-evaluated operands reach the primitive kernel.
        // This call performs no column, parameter, aggregate, or subquery lookup.
        let value = try evaluateSelectedKernel(expression, on: row, workMeter: workMeter)
        let result = try value.withValue { try normalize(.primitive(copy $0)) }
        var leases = children.flatMap { $0.leases }
        if let reservation = value.retainedReservation { leases.append(DatabaseQueryPayloadLease(retaining: reservation)) }
        return ProjectedQueryValue(value: result, leases: leases, payloadIsRetained: !leases.isEmpty)
    }
    static func evaluateSelectedKernel(
        _ expression: DatabaseKit.Expression,
        on row: CanonicalSourceRow,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseQueryScopedFieldValue {
        var retainedLifetimes = try DatabaseRetainedArrayBuilder<DatabaseIntermediateReservation>(
            workMeter: workMeter, stage: .expressionEvaluation,
            layout: DatabaseRetainedArrayLayout.forElement(DatabaseIntermediateReservation.self))
        let admission = try preadmitExpressionPayload(expression, on: row, workMeter: workMeter,
            retainedLifetimes: &retainedLifetimes,
            forceRootOwnership: canonicalExpressionResultRequiresOwnedPayload(expression))
        let lifetimes = try retainedLifetimes.finish().moveToSharedOwnership(at: .expressionEvaluation)
        defer { withExtendedLifetime(lifetimes) {} }
        let evaluator = DatabaseExpressionEvaluator(fields: [:], workMeter: workMeter)
        if let admission {
            return try .producing(maximumFootprint: admission.maximumFootprint,
                reservation: admission.reservation, stage: .expressionEvaluation) {
                    try evaluator.evaluate(expression)
                }
        }
        return .borrowing(try evaluator.evaluate(expression))
    }


}

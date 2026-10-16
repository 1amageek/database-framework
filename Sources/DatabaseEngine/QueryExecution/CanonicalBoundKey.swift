import DatabaseKit

/// One selected key, optionally reusing a designated projection occurrence.
struct CanonicalBoundKey: Sendable {
    let expression: CanonicalBoundExpression
    let projectionIndex: Int?
    let sortKey: SortKey?
    let occurrence: QueryOccurrenceID?

    init(expression: CanonicalBoundExpression, projectionIndex: Int? = nil,
         sortKey: SortKey? = nil, occurrence: QueryOccurrenceID? = nil) {
        self.expression = expression
        self.projectionIndex = projectionIndex
        self.sortKey = sortKey
        self.occurrence = occurrence ?? expression.occurrence
    }
}

extension CanonicalBoundKey {
    /// Prefix compatibility never changes the designated projection cache key.
    func isSemanticallyEquivalent(to other: CanonicalBoundKey, workMeter: DatabaseWorkMeter?,
        isVolatileSubquery: (CanonicalBoundSelect) -> Bool) throws -> Bool {
        try workMeter?.consume(at: .bindingCandidate)
        if let projectionIndex, projectionIndex == other.projectionIndex { return true }
        let layout = try DatabaseRetainedArrayLayout.forElement(ExpressionPair.self)
        let scratch = try workMeter?.reserveIntermediate(bytes: layout.containerByteCount, at: .bindingCandidate)
        defer { scratch?.release() }
        var pending: [ExpressionPair] = []
        try CanonicalBindingStorage.append(ExpressionPair(left: expression, right: other.expression),
            to: &pending, reservation: scratch)
        while let pair = pending.popLast() {
            try workMeter?.consume(at: .bindingCandidate)
            let left = pair.left, right = pair.right
            guard left.type == right.type, left.collation == right.collation,
                  left.comparisonMode == right.comparisonMode, left.children.count == right.children.count else { return false }
            if left.columnSlot != nil || right.columnSlot != nil {
                guard left.columnSlot == right.columnSlot else { return false }
                continue
            }
            if left.wholeRow != nil || right.wholeRow != nil {
                guard let lhs = left.wholeRow, let rhs = right.wholeRow,
                      lhs.presenceIndex == rhs.presenceIndex, lhs.fieldSlots == rhs.fieldSlots else { return false }
                continue
            }
            if let plan = left.subqueryBinding, isVolatileSubquery(plan) { return false }
            if let plan = right.subqueryBinding, isVolatileSubquery(plan) { return false }
            guard try Self.sameOperation(left, right, workMeter: workMeter) else { return false }
            for index in left.children.indices.reversed() {
                try CanonicalBindingStorage.append(ExpressionPair(left: left.children[index], right: right.children[index]),
                    to: &pending, reservation: scratch)
            }
        }
        return true
    }

    private struct ExpressionPair {
        let left: CanonicalBoundExpression
        let right: CanonicalBoundExpression
    }

    private static func sameOperation(_ left: CanonicalBoundExpression, _ right: CanonicalBoundExpression,
        workMeter: DatabaseWorkMeter?) throws -> Bool {
        switch (left.expression, right.expression) {
        case (.column, .column): return false // A selected column must have a resolved slot.
        case (.add, .add), (.subtract, .subtract), (.multiply, .multiply), (.divide, .divide),
             (.modulo, .modulo), (.concatenate, .concatenate), (.negate, .negate),
             (.equal, .equal), (.notEqual, .notEqual), (.lessThan, .lessThan),
             (.lessThanOrEqual, .lessThanOrEqual), (.greaterThan, .greaterThan),
             (.greaterThanOrEqual, .greaterThanOrEqual), (.and, .and), (.or, .or),
             (.not, .not), (.isNull, .isNull), (.isNotNull, .isNotNull),
             (.between, .between), (.inList, .inList), (.notInList, .notInList),
             (.coalesce, .coalesce), (.nullIf, .nullIf), (.greatest, .greatest),
             (.least, .least), (.row, .row), (.array, .array), (.fieldExpansion, .fieldExpansion): return true
        case (.cast(_, let lhs), .cast(_, let rhs)):
            return lhs == rhs && left.castContext == right.castContext && left.sourceAlignment == right.sourceAlignment
        case (.collate(_, let lhs), .collate(_, let rhs)): return lhs == rhs
        case (.field, .field): return left.fieldOrdinal == right.fieldOrdinal
        case (.currentValue(let lhs, let lp), .currentValue(let rhs, let rp)): return lhs == rhs && lp == rp
        case (.function(let lhs), .function(let rhs)):
            // Separate volatile occurrences cannot share a prefix merely because their syntax matches.
            if lhs.schema == nil && lhs.name == "clock_timestamp" { return false }
            return try sameName(lhs.name, rhs.name, workMeter: workMeter)
                && sameOptionalName(lhs.schema, rhs.schema, workMeter: workMeter)
                && lhs.distinct == rhs.distinct && lhs.finalArrayIsVariadic == rhs.finalArrayIsVariadic
        case (.collectionOperation(let lhs, _, _), .collectionOperation(let rhs, _, _)): return lhs == rhs
        case (.patternMatch(_, _, let le, let lk), .patternMatch(_, _, let re, let rk)):
            return lk == rk && (le == nil) == (re == nil)
        case (.truthTest(_, let lv, let ln), .truthTest(_, let rv, let rn)): return lv == rv && ln == rn
        case (.isDistinctFrom(_, _, let lhs), .isDistinctFrom(_, _, let rhs)): return lhs == rhs
        case (.caseWhen(let lo, let lc, let lf), .caseWhen(let ro, let rc, let rf)):
            return (lo == nil) == (ro == nil) && lc.count == rc.count && (lf == nil) == (rf == nil)
        case (.arrayAccess(_, let lhs), .arrayAccess(_, let rhs)):
            guard lhs.count == rhs.count else { return false }
            for index in lhs.indices {
                try workMeter?.consume(at: .bindingCandidate)
                switch (lhs[index], rhs[index]) {
                case (.index, .index): break
                case (.slice(let ll, let lu), .slice(let rl, let ru)):
                    guard (ll == nil) == (rl == nil), (lu == nil) == (ru == nil) else { return false }
                default: return false
                }
            }
            return true
        case (.literal(let lhs), .literal(let rhs)):
            try charge(lhs, workMeter: workMeter)
            try charge(rhs, workMeter: workMeter)
            return lhs == rhs
        case (.subquery(let lhs), .subquery(let rhs)), (.exists(let lhs), .exists(let rhs)),
             (.arraySubquery(let lhs), .arraySubquery(let rhs)), (.inSubquery(_, let lhs), .inSubquery(_, let rhs)):
            return lhs == rhs
        case (.quantifiedComparison(_, let lo, let lq, let ls), .quantifiedComparison(_, let ro, let rq, let rs)):
            guard lo == ro, lq == rq else { return false }
            switch (ls, rs) {
            case (.array, .array): return true
            case (.subquery(let lhs), .subquery(let rhs)): return lhs == rhs
            default: return false
            }
        default:
            // Preserve exact QueryIR matching for the remaining non-SQL forms.
            return left.expression == right.expression
        }
    }

    private static func sameName(_ left: String, _ right: String, workMeter: DatabaseWorkMeter?) throws -> Bool {
        if let workMeter {
            try DatabaseByteProcessingMeter.consume(byteCount: UInt64(left.utf8.count), workMeter: workMeter, stage: .bindingCandidate)
            try DatabaseByteProcessingMeter.consume(byteCount: UInt64(right.utf8.count), workMeter: workMeter, stage: .bindingCandidate)
        }
        return left.utf8.elementsEqual(right.utf8)
    }

    private static func sameOptionalName(_ left: String?, _ right: String?, workMeter: DatabaseWorkMeter?) throws -> Bool {
        switch (left, right) {
        case (.none, .none): return true
        case (.some(let lhs), .some(let rhs)): return try sameName(lhs, rhs, workMeter: workMeter)
        default: return false
        }
    }

    private static func charge(_ literal: Literal, workMeter: DatabaseWorkMeter?) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Literal.self)
        let scratch = try workMeter?.reserveIntermediate(bytes: layout.containerByteCount, at: .bindingCandidate)
        defer { scratch?.release() }
        var pending: [Literal] = []
        try CanonicalBindingStorage.append(literal, to: &pending, reservation: scratch)
        while let value = pending.popLast() {
            try workMeter?.consume(at: .bindingCandidate)
            switch value {
            case .string(let text), .sqlUnknownString(let text), .iri(let text), .blankNode(let text):
                if let workMeter {
                    try DatabaseByteProcessingMeter.consume(byteCount: UInt64(text.utf8.count), workMeter: workMeter, stage: .bindingCandidate)
                }
            case .typedLiteral(let value, let datatype), .langLiteral(let value, let datatype):
                _ = try sameName(value, datatype, workMeter: workMeter)
            case .dirLangLiteral(let value, let language, let direction):
                _ = try sameName(value, language, workMeter: workMeter)
                _ = try sameName(direction, "", workMeter: workMeter)
            case .binary(let bytes):
                if let workMeter {
                    try DatabaseByteProcessingMeter.consume(byteCount: UInt64(bytes.count), workMeter: workMeter, stage: .bindingCandidate)
                }
            case .queryValue(let value, _):
                if let workMeter {
                    _ = try CanonicalRelationalFootprintMeter.valueFootprint(of: value, workMeter: workMeter, stage: .bindingCandidate)
                }
            case .array(let values):
                for value in values { try CanonicalBindingStorage.append(value, to: &pending, reservation: scratch) }
            case .rdfTerm(let term):
                if let workMeter {
                    _ = try CanonicalRelationalFootprintMeter.valueFootprint(of: .rdfTerm(term), workMeter: workMeter, stage: .bindingCandidate)
                }
            default: break
            }
        }
    }
}

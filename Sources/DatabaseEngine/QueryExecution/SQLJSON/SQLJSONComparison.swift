import DatabaseKit
import DatabaseTypes

/// SQL semantics; representation Hashable is deliberately not used.
enum SQLJSONComparison {
    private enum Visit {
        case pair(QueryJSONBNode, QueryJSONBNode, Bool)
        case arrays(QueryJSONBElements, QueryJSONBElements, Int)
        case objects(QueryJSONBMembers, QueryJSONBMembers, Int)
    }
    private enum Containment {
        case pair(QueryJSONBNode, QueryJSONBNode)
        case all(Int)
        case any(Int)
    }

    static func compare(_ left: QueryJSONBNode, _ right: QueryJSONBNode,
        memory: SQLJSONMemory) throws -> QueryComparison {
        var visits: [Visit] = []
        try memory.append(.pair(left, right, true), to: &visits)
        while let visit = visits.popLast() {
            try memory.work()
            switch visit {
            case .pair(let left, let right, let root):
                let l = rank(left, root: root), r = rank(right, root: root)
                if l != r { return l < r ? .lessThan : .greaterThan }
                switch (left, right) {
                case (.null, .null): break
                case (.bool(let a), .bool(let b)): if a != b { return a ? .greaterThan : .lessThan }
                case (.string(let a), .string(let b)):
                    let result = try strings(a, b, memory: memory)
                    if result != .equal { return result }
                case (.number(let a), .number(let b)):
                    if a.value != b.value { return a.value < b.value ? .lessThan : .greaterThan }
                case (.array(let a), .array(let b)):
                    if a.count != b.count { return a.count < b.count ? .lessThan : .greaterThan }
                    if !a.isEmpty { try memory.append(.arrays(a, b, 0), to: &visits) }
                case (.object(let a), .object(let b)):
                    if a.count != b.count { return a.count < b.count ? .lessThan : .greaterThan }
                    if !a.isEmpty { try memory.append(.objects(a, b, 0), to: &visits) }
                default: throw SQLJSONMemory.failure("JSONB comparison kinds")
                }
            case .arrays(let a, let b, let index):
                if index + 1 < a.count { try memory.append(.arrays(a, b, index + 1), to: &visits) }
                try memory.append(.pair(a[index], b[index], false), to: &visits)
            case .objects(let a, let b, let index):
                let result = try strings(a[index].key, b[index].key, memory: memory)
                if result != .equal { return result }
                if index + 1 < a.count { try memory.append(.objects(a, b, index + 1), to: &visits) }
                try memory.append(.pair(a[index].value, b[index].value, false), to: &visits)
            }
        }
        return .equal
    }

    static func contains(_ left: QueryJSONBNode, _ right: QueryJSONBNode,
        memory: SQLJSONMemory) throws -> Bool {
        var visits: [Containment] = [], results: [Bool] = []
        try memory.append(.pair(left, right), to: &visits)
        while let visit = visits.popLast() {
            try memory.work()
            switch visit {
            case .all(let count), .any(let count):
                let all: Bool
                if case .all = visit { all = true } else { all = false }
                var result = all
                for value in results.suffix(count) { try memory.work(); result = all ? result && value : result || value }
                results.removeLast(count)
                try memory.append(result, to: &results)
            case .pair(let left, let right):
                switch (left, right) {
                case (.object(let a), .object(let b)):
                    var pairs: [(QueryJSONBNode, QueryJSONBNode)] = []
                    var missing = false
                    for member in b {
                        var found: QueryJSONBNode?
                        for candidate in a {
                            if try strings(member.key, candidate.key, memory: memory) == .equal { found = candidate.value; break }
                        }
                        guard let found else { missing = true; break }
                        try memory.append((found, member.value), to: &pairs)
                    }
                    if missing { try memory.append(false, to: &results) }
                    else {
                        try memory.append(.all(pairs.count), to: &visits)
                        for pair in pairs.reversed() { try memory.append(.pair(pair.0, pair.1), to: &visits) }
                    }
                case (.array(let a), .array(let b)):
                    try memory.append(.all(b.count), to: &visits)
                    for child in b.reversed() {
                        var eligible: [QueryJSONBNode] = []
                        for candidate in a {
                            try memory.work()
                            if scalar(child) && !scalar(candidate) { continue }
                            try memory.append(candidate, to: &eligible)
                        }
                        try memory.append(.any(eligible.count), to: &visits)
                        for candidate in eligible.reversed() { try memory.append(.pair(candidate, child), to: &visits) }
                    }
                case (.array(let a), _):
                    if scalar(right) {
                        var found = false
                        for candidate in a where scalar(candidate) {
                            if try compare(candidate, right, memory: memory) == .equal { found = true; break }
                        }
                        try memory.append(found, to: &results)
                    } else { try memory.append(false, to: &results) }
                default:
                    try memory.append(try scalar(left) && scalar(right) && compare(left, right, memory: memory) == .equal, to: &results)
                }
            }
        }
        guard results.count == 1 else { throw SQLJSONMemory.failure("JSONB containment stack") }
        return results[0]
    }

    static func strings(_ left: String, _ right: String, memory: SQLJSONMemory) throws -> QueryComparison {
        try memory.work(try SQLJSONMemory.add(UInt64(left.utf8.count), UInt64(right.utf8.count)))
        if left.utf8.elementsEqual(right.utf8) { return .equal }
        return left.utf8.lexicographicallyPrecedes(right.utf8) ? .lessThan : .greaterThan
    }
    private static func rank(_ node: QueryJSONBNode, root: Bool) -> Int {
        switch node {
        case .null: return 0
        case .string: return 1
        case .number: return 2
        case .bool: return 3
        case .array(let array): return root && array.isEmpty ? -1 : 4
        case .object: return 5
        }
    }
    private static func scalar(_ node: QueryJSONBNode) -> Bool {
        switch node { case .array, .object: return false; default: return true }
    }
}

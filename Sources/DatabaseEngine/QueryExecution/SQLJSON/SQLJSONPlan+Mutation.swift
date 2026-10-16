import DatabaseKit
import DatabaseTypes

extension SQLJSONPlan {
    enum Mutation: Equatable { case set, insert, delete }

    mutating func concatenate(_ left: Int, _ right: Int) throws -> Int {
        if case .object(let a, _) = entries[left], case .object(let b, _) = entries[right] {
            var result = try copy(a)
            for member in b {
                if let index = try memberIndex(member.key, in: result) { result[index] = member }
                else { try memory.append(member, to: &result) }
            }
            try sort(&result)
            return try add(.object(result))
        }
        var result: [Int] = []
        for id in [left, right] {
            if case .array(let children) = entries[id] {
                try memory.prepare(&result, adding: children.count)
                result.append(contentsOf: children)
            } else { try memory.append(id, to: &result) }
        }
        return try add(.array(result))
    }

    mutating func delete(_ root: Int, key: String) throws {
        switch entries[root] {
        case .object(let members, _):
            var output: [Member] = []
            for member in members {
                if try SQLJSONComparison.strings(member.key, key, memory: memory) != .equal {
                    try memory.append(member, to: &output)
                }
            }
            entries[root] = .object(output)
        case .array(let children):
            var output: [Int] = []
            for child in children {
                if case .scalar(.string(let string)) = entries[child],
                   try SQLJSONComparison.strings(string, key, memory: memory) == .equal { continue }
                try memory.append(child, to: &output)
            }
            entries[root] = .array(output)
        default: throw collection(.jsonContainerRootRequired(operation: "jsonb - text"))
        }
    }

    mutating func delete(_ root: Int, index: Int32) throws {
        guard case .array(let children) = entries[root] else {
            throw collection(.jsonArrayRootRequired(function: "jsonb - integer"))
        }
        let position = index < 0 ? Int64(children.count) + Int64(index) : Int64(index)
        guard position >= 0, position < children.count else { return }
        var output = try copy(children)
        output.remove(at: Int(position))
        entries[root] = .array(output)
    }

    mutating func mutate(_ root: Int, path: QueryArrayValue, replacement: Int?,
        operation: Mutation, flag: Bool) throws {
        guard path.dimensions.count <= 1 else {
            throw collection(.multidimensionalJSONMutationPath(rank: path.dimensions.count))
        }
        switch entries[root] {
        case .array, .object: break
        default: throw collection(.jsonContainerRootRequired(operation: operation == .delete ? "#-" : (operation == .set ? "jsonb_set" : "jsonb_insert")))
        }
        var current = root
        for position in path.elements.indices {
            try memory.work()
            let cell = path.elements[position]
            guard !cell.isNull else { throw collection(.nullJSONMutationPathElement(position: position + 1)) }
            guard case .primitive(.string(let key)) = cell else { throw SQLJSONMemory.failure("selected JSON path element") }
            let final = position + 1 == path.elements.count
            switch entries[current] {
            case .object(let members, _):
                let existing = try memberIndex(key, in: members)
                if !final {
                    guard let existing else { return }
                    current = members[existing].value; continue
                }
                var output = try copy(members)
                switch operation {
                case .delete:
                    if let existing { output.remove(at: existing) }
                case .set:
                    guard let replacement else { throw SQLJSONMemory.failure("JSON replacement") }
                    if let existing { output[existing] = Member(key: key, value: replacement) }
                    else if flag { try memory.append(Member(key: key, value: replacement), to: &output); try sort(&output) }
                case .insert:
                    if existing != nil { throw collection(.existingJSONInsertKey(key: key)) }
                    guard let replacement else { throw SQLJSONMemory.failure("JSON replacement") }
                    try memory.bytes(UInt64(key.utf8.count))
                    try memory.append(Member(key: key, value: replacement), to: &output); try sort(&output)
                }
                entries[current] = .object(output)
            case .array(let children):
                guard let index = SQLJSONLexicalInput.index(key) else {
                    throw collection(.invalidJSONMutationPathIndex(position: position + 1, text: key))
                }
                let selected = index < 0 ? Int64(children.count) + Int64(index) : Int64(index)
                let inside = selected >= 0 && selected < children.count
                if !final {
                    guard inside else { return }; current = children[Int(selected)]; continue
                }
                var output = try copy(children)
                switch operation {
                case .delete: if inside { output.remove(at: Int(selected)) }
                case .set:
                    guard let replacement else { throw SQLJSONMemory.failure("JSON replacement") }
                    if inside { output[Int(selected)] = replacement }
                    else if flag {
                        try memory.prepare(&output)
                        output.insert(replacement, at: index < 0 ? 0 : output.count)
                    }
                case .insert:
                    guard let replacement else { throw SQLJSONMemory.failure("JSON replacement") }
                    let at = inside ? Int(selected) + (flag ? 1 : 0) : (index < 0 ? 0 : output.count)
                    try memory.prepare(&output); output.insert(replacement, at: at)
                }
                entries[current] = .array(output)
            default: return
            }
        }
    }

    mutating func stripNulls(_ root: Int, arrays: Bool) throws {
        var pending: [Int] = []
        try memory.append(root, to: &pending)
        while let id = pending.popLast() {
            try memory.work()
            switch entries[id] {
            case .array(let children):
                var output: [Int] = []
                for child in children {
                    if arrays, case .scalar(.null) = entries[child] { continue }
                    try memory.append(child, to: &output)
                    try memory.append(child, to: &pending)
                }
                entries[id] = .array(output)
            case .object(let members, _):
                var output: [Member] = []
                for member in members {
                    if case .scalar(.null) = entries[member.value] { continue }
                    try memory.append(member, to: &output)
                    try memory.append(member.value, to: &pending)
                }
                entries[id] = .object(output)
            default: break
            }
        }
    }

    private func copy<T>(_ source: [T]) throws -> [T] {
        var destination: [T] = []
        try memory.prepare(&destination, adding: source.count)
        // Only this container's ID/member slots are copied; subtrees remain flat references.
        destination.append(contentsOf: source)
        return destination
    }

    private func collection(_ cause: SQLCollectionError) -> DatabaseExpressionEvaluationError { .collection(cause) }
}

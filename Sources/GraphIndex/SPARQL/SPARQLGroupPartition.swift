import DatabaseEngine

struct SPARQLGroupMember: Sendable {
    let sourceIndex: Int
    let groupIdentifier: Int
    let expressionScopeIdentifier: UInt64
}

struct SPARQLGroupDescriptor: Sendable {
    let representativeKeyOffset: Int?
    let memberRange: Range<Int>
}

/// Linear ownership for a grouped solution relation.
///
/// Source rows stay in their original retained relation. Grouping owns only
/// flat key values, row-index metadata, and contiguous group ranges.
struct SPARQLGroupPartition: ~Copyable, Sendable {
    private let source: SPARQLRetainedBindings
    private let keyValues: DatabaseRetainedBuffer<GroupValue>
    private let members: DatabaseRetainedBuffer<SPARQLGroupMember>
    private let groups: DatabaseRetainedBuffer<SPARQLGroupDescriptor>
    let keyCount: Int

    init(
        source: consuming SPARQLRetainedBindings,
        keyValues: consuming DatabaseRetainedBuffer<GroupValue>,
        members: consuming DatabaseRetainedBuffer<SPARQLGroupMember>,
        groups: consuming DatabaseRetainedBuffer<SPARQLGroupDescriptor>,
        keyCount: Int
    ) {
        self.source = consume source
        self.keyValues = consume keyValues
        self.members = consume members
        self.groups = consume groups
        self.keyCount = keyCount
    }

    var groupCount: Int { groups.count }

    borrowing func memberRange(at groupIndex: Int) -> Range<Int> {
        groups.withElement(at: groupIndex) { descriptor in
            descriptor.memberRange
        }
    }

    borrowing func withMember<Result, Failure: Error>(
        at memberIndex: Int,
        _ body: (borrowing VariableBinding) async throws(Failure) -> Result
    ) async throws(Failure) -> Result {
        try await members.withElement(
            at: memberIndex
        ) { (member) async throws(Failure) in
            try await source.withElement(
                at: member.sourceIndex
            ) { (binding) async throws(Failure) in
                let scoped = binding.assigningExpressionScope(
                    member.expressionScopeIdentifier
                )
                return try await body(scoped)
            }
        }
    }

    borrowing func withGroupKeyValue<Result>(
        groupIndex: Int,
        keyIndex: Int,
        _ body: (borrowing GroupValue) throws -> Result
    ) throws -> Result {
        precondition(keyIndex >= 0 && keyIndex < keyCount)
        return try groups.withElement(at: groupIndex) { descriptor in
            guard let keyOffset = descriptor.representativeKeyOffset else {
                preconditionFailure(
                    "A keyed group must retain a representative key offset"
                )
            }
            return try keyValues.withElement(
                at: keyOffset + keyIndex,
                body
            )
        }
    }
}

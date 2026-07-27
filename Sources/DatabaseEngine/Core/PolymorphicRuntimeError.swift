public enum PolymorphicRuntimeError: Error, Sendable, Equatable {
    case missingGroup(identifier: String)
    case invalidStoredIdentifier
    case invalidRequestedIdentifier
    case unknownTypeCode(Int64)
    case nonPolymorphableMember(
        groupIdentifier: String,
        memberTypeName: String
    )
}

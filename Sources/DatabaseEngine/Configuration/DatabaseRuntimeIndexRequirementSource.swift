/// Schema location that requires a runtime index capability.
public enum DatabaseRuntimeIndexRequirementSource: Sendable, Equatable {
    case entity(String)
    case polymorphicGroup(String)
}

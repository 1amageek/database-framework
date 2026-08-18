#if DATABASE_MULTIPLE_BASES
import DatabaseKit

/// Deliberately opaque Composition access failures that do not reveal member
/// Base existence or authorization state.
public enum DatabaseCompositionAccessError: Error, Sendable, Equatable {
    case unavailable(CompositionSelection)
}

#endif

#if DATABASE_MULTI_BASE
import DatabaseKit

/// Explicit authorization boundary for all local database data operations.
public final class DatabaseSession: Sendable {
    package let container: DBContainer
    package let authorization: AuthorizationContext

    package init(
        container: DBContainer,
        authorization: AuthorizationContext
    ) {
        self.container = container
        self.authorization = authorization
    }

    public func base(_ id: Base.ID) -> BaseDataSource {
        BaseDataSource(
            id: id,
            container: container,
            authorization: authorization
        )
    }

    public func composition(
        _ id: Base.Composition.ID
    ) -> CompositionDataSource {
        composition(.named(id))
    }

    public func composition(
        _ selection: CompositionSelection
    ) -> CompositionDataSource {
        CompositionDataSource(
            selection: selection,
            container: container,
            authorization: authorization
        )
    }

    public func composition(
        bases: [Base.ID]
    ) throws -> CompositionDataSource {
        CompositionDataSource(
            selection: try .derived(bases),
            container: container,
            authorization: authorization
        )
    }
}

#endif

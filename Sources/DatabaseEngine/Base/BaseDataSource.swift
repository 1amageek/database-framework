import DatabaseKit

/// Read and mutation source fixed to one Base identity.
public struct BaseDataSource: Sendable {
    public let id: Base.ID
    private let container: DBContainer
    private let authorization: AuthorizationContext

    package init(
        id: Base.ID,
        container: DBContainer,
        authorization: AuthorizationContext
    ) {
        self.id = id
        self.container = container
        self.authorization = authorization
    }

    public func newContext(
        autosaveEnabled: Bool = false
    ) -> DatabaseContext {
        DatabaseContext(
            container: container,
            baseID: id,
            authorization: authorization,
            autosaveEnabled: autosaveEnabled
        )
    }

    public func query<Model: Persistable>(
        _ type: Model.Type
    ) -> QueryExecutor<Model> {
        newContext().fetch(type)
    }

    public func admin() -> AdminContext {
        AdminContext(context: newContext())
    }
}

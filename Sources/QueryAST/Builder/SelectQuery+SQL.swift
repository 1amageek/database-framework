import DatabaseKit

public extension SelectQuery {
    /// Serializes canonical IR through the existing bounded SQL owner.
    func toSQL() throws(SQLSerializationError) -> String {
        try SQLSerializer().serialize(self)
    }
}

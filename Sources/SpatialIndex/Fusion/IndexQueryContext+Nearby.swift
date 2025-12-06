// IndexQueryContext+Nearby.swift
// SpatialIndex - Factory method for Nearby query

import Foundation
import Core
import DatabaseEngine

extension IndexQueryContext {
    /// Create a Nearby query for spatial search
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(Store.self) {
    ///     context.indexQueryContext.nearby(Store.self, \.location)
    ///         .within(radiusKm: 5, of: userLocation)
    /// }
    /// .execute()
    /// ```
    public func nearby<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, GeoPoint>
    ) -> Nearby<T> {
        Nearby(keyPath, context: self)
    }

    /// Create a Nearby query for optional GeoPoint field
    public func nearby<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, GeoPoint?>
    ) -> Nearby<T> {
        Nearby(keyPath, context: self)
    }
}

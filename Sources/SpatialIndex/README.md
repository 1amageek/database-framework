# SpatialIndex

Geographic and spatial queries using S2 geometry or Morton codes.

## Overview

SpatialIndex provides efficient spatial indexing and querying for 2D/3D coordinates. It supports two encoding strategies: S2 (Google's spherical geometry) for geographic data and Morton codes (Z-order curves) for general spatial data.

**Algorithms**:
- **S2 Geometry**: Hierarchical spherical cells for precise geographic queries
- **Morton Code**: Z-order space-filling curve for 2D/3D data
- **Covering Cells**: Minimal cell set covering a search region

**Storage Layout**:
```
[indexSubspace][spatialCode][primaryKey] = ''
```

Where `spatialCode` is a UInt64 cell ID computed from coordinates.

## Use Cases

### 1. Store Locator (Points of Interest)

**Scenario**: Find nearby stores within a radius.

```swift
@Persistable
struct Store {
    var id: String = ULID().ulidString
    var name: String = ""
    var latitude: Double = 0
    var longitude: Double = 0
    var category: String = ""

    #Index<Store>(
        type: SpatialIndexKind(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: .s2,
            level: 15  // ~1km resolution
        )
    )
}

// Find stores within 5km of user location
let nearbyStores = try await context.nearby(Store.self)
    .coordinates(latitude: userLat, longitude: userLon)
    .radius(5000)  // meters
    .execute()
```

**Performance**: O(c + n) where c = covering cells, n = results per cell.

### 2. Delivery Zone Check (Bounding Box)

**Scenario**: Find orders within a delivery zone rectangle.

```swift
@Persistable
struct DeliveryOrder {
    var id: String = ULID().uuidString
    var address: String = ""
    var latitude: Double = 0
    var longitude: Double = 0
    var status: String = ""

    #Index<DeliveryOrder>(
        type: SpatialIndexKind(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: .s2,
            level: 13  // ~5km resolution
        )
    )
}

// Find orders in delivery zone
let orders = try await context.nearby(DeliveryOrder.self)
    .boundingBox(
        minLat: 35.65, minLon: 139.70,
        maxLat: 35.70, maxLon: 139.80
    )
    .execute()
```

### 3. Geofencing (Event Detection)

**Scenario**: Check if a location is within a geofence.

```swift
@Persistable
struct Geofence {
    var id: String = ULID().uuidString
    var name: String = ""
    var centerLat: Double = 0
    var centerLon: Double = 0
    var radiusMeters: Double = 0

    #Index<Geofence>(
        type: SpatialIndexKind(
            latitude: \.centerLat,
            longitude: \.centerLon,
            encoding: .s2,
            level: 12  // ~10km resolution
        )
    )
}

// Check if device location triggers any geofence
let nearbyFences = try await context.nearby(Geofence.self)
    .coordinates(latitude: deviceLat, longitude: deviceLon)
    .radius(10000)  // Check fences within 10km
    .execute()

// Post-filter by actual fence radius
let triggered = nearbyFences.filter { fence in
    haversineDistance(deviceLat, deviceLon, fence.centerLat, fence.centerLon) <= fence.radiusMeters
}
```

### 4. 3D Spatial Index (Altitude)

**Scenario**: Index aircraft positions with altitude.

```swift
@Persistable
struct AircraftPosition {
    var id: String = ULID().uuidString
    var callsign: String = ""
    var latitude: Double = 0
    var longitude: Double = 0
    var altitude: Double = 0  // meters

    #Index<AircraftPosition>(
        type: SpatialIndexKind(
            coordinates: [\.latitude, \.longitude, \.altitude],
            encoding: .morton,  // Morton supports 3D
            level: 16
        )
    )
}
```

### 5. Map Tile Query (Hierarchical)

**Scenario**: Fetch map data for visible tiles.

```swift
@Persistable
struct MapFeature {
    var id: String = ULID().ulidString
    var featureType: String = ""
    var latitude: Double = 0
    var longitude: Double = 0

    // Multiple levels for hierarchical queries
    #Index<MapFeature>(
        type: SpatialIndexKind(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: .s2,
            level: 10  // Coarse for zoomed out
        )
    )

    #Index<MapFeature>(
        type: SpatialIndexKind(
            latitude: \.latitude,
            longitude: \.longitude,
            encoding: .s2,
            level: 18  // Fine for zoomed in
        )
    )
}
```

## Design Patterns

### S2 Level Selection

| Level | Cell Size | Use Case |
|-------|-----------|----------|
| 6 | ~100km | Country/region |
| 10 | ~10km | City-level |
| 13 | ~1km | Neighborhood |
| 15 | ~300m | Street-level |
| 18 | ~30m | Building-level |
| 21 | ~3m | Room-level |

**Choosing Level**:
```swift
// Coarse level: fewer cells, faster index but less precise
SpatialIndexKind(latitude: \.lat, longitude: \.lon, level: 10)

// Fine level: more cells, slower index but more precise
SpatialIndexKind(latitude: \.lat, longitude: \.lon, level: 18)
```

**Trade-off**: Lower level = fewer cells = faster scan, but more false positives.

### Covering Cells Algorithm

For a search region (circle or rectangle), S2 computes a "covering" - a set of cells that completely covers the region:

```
Search Circle → Covering Cells → Index Scan → Post-filter

     ┌─────────────────────┐
     │   ┌───┬───┬───┐    │
     │   │ C │ C │   │    │   C = Covering cell
     │   ├───┼───┼───┤    │   P = Point in region
     │ ● │ C │ P │ C │    │   ● = Center
     │   ├───┼───┼───┤    │
     │   │ C │ C │   │    │
     │   └───┴───┴───┘    │
     └─────────────────────┘
```

**Covering Strategy**:
- More cells → Better precision, more index scans
- Fewer cells → Faster query, more post-filtering

### Sparse Index (Optional Coordinates)

SpatialIndex supports sparse index behavior for optional location fields:

```swift
@Persistable
struct Event {
    var id: String = ULID().uuidString
    var name: String = ""
    var latitude: Double? = nil  // Optional - not all events have location
    var longitude: Double? = nil

    #Index<Event>(
        type: SpatialIndexKind(
            latitude: \.latitude,
            longitude: \.longitude
        )
    )
}

// Events with nil coordinates are NOT indexed
// Only events with coordinates appear in spatial queries
```

### S2 vs Morton Comparison

| Feature | S2 | Morton |
|---------|-----|--------|
| Coordinate system | Spherical | Cartesian |
| Dimensions | 2D (lat/lon) | 2D or 3D |
| Earth curvature | Handled | Not handled |
| Distance accuracy | High | Approximate |
| Implementation | Complex | Simple |
| Use case | Geographic | General spatial |

**Recommendation**:
- Geographic data (GPS) → Use S2
- Game/simulation (x,y,z) → Use Morton

## Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| S2 encoding | ✅ Complete | All levels (0-30) |
| Morton 2D encoding | ✅ Complete | Up to 32-bit precision |
| Morton 3D encoding | ✅ Complete | Up to 21-bit per axis |
| Radius search | ✅ Complete | Via covering cells |
| Bounding box search | ✅ Complete | Via covering cells |
| Distance calculation | ⚠️ Post-filter | Haversine not built-in |
| Sparse index (nil) | ✅ Complete | nil coordinates not indexed |
| K-nearest neighbors | ❌ Not implemented | Requires distance sorting |
| Polygon queries | ❌ Not implemented | Complex region support |

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| Insert | O(1) | Single cell encoding |
| Delete | O(1) | Direct key clear |
| Radius search | O(c × n) | c = cells, n = results/cell |
| Bounding box | O(c × n) | c = cells, n = results/cell |

### Cell Count vs Precision

| Search Type | Radius | Level 10 | Level 15 | Level 18 |
|-------------|--------|----------|----------|----------|
| Circle | 1km | ~4 cells | ~20 cells | ~100 cells |
| Circle | 5km | ~10 cells | ~50 cells | ~300 cells |
| Circle | 10km | ~20 cells | ~100 cells | ~600 cells |

**Observation**: Higher levels = more cells = slower query but fewer false positives.

### FDB Considerations

- **Key size**: S2 cell ID = 8 bytes, primary key additional
- **Transaction limit**: 10MB writes, batch large imports
- **Range scan**: Each covering cell requires one range scan

## Benchmark Results

Run with: `swift test --filter SpatialIndexPerformanceTests`

### Indexing

| Locations | Insert Time | Throughput |
|-----------|-------------|------------|
| 100 | ~50ms | ~2,000/s |
| 1,000 | ~500ms | ~2,000/s |
| 10,000 | ~5s | ~2,000/s |

### Radius Search

| Locations | Radius | Level | Latency (p50) |
|-----------|--------|-------|---------------|
| 1,000 | 1km | 15 | ~10ms |
| 1,000 | 5km | 15 | ~30ms |
| 10,000 | 1km | 15 | ~15ms |
| 10,000 | 5km | 15 | ~50ms |

### Bounding Box Search

| Locations | Box Size | Level | Latency (p50) |
|-----------|----------|-------|---------------|
| 1,000 | 1km² | 15 | ~5ms |
| 1,000 | 10km² | 15 | ~20ms |
| 10,000 | 1km² | 15 | ~10ms |

*Benchmarks run on M1 Mac with local FoundationDB cluster.*

## References

- [S2 Geometry Library](https://s2geometry.io/) - Google's spherical geometry
- [S2 Cell Hierarchy](https://s2geometry.io/devguide/s2cell_hierarchy) - Cell levels explained
- [Morton Code](https://en.wikipedia.org/wiki/Z-order_curve) - Z-order space-filling curve
- [Geohash](https://en.wikipedia.org/wiki/Geohash) - Related encoding (not used)
- [Haversine Formula](https://en.wikipedia.org/wiki/Haversine_formula) - Great-circle distance

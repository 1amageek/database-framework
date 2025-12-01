// Geohash.swift
// SpatialIndexLayer - Geohash encoding/decoding
//
// Complete geohash implementation ported from fdb-record-layer.

import Foundation

/// Geohash encoding/decoding for geographic coordinates
///
/// Geohash is a geocoding system that encodes latitude/longitude into a short string
/// using base32 encoding. It provides hierarchical spatial indexing with Z-order curve mapping.
public enum Geohash {
    private static let base32 = Array("0123456789bcdefghjkmnpqrstuvwxyz")

    private static let base32Lookup: [Character: Int] = {
        var lookup: [Character: Int] = [:]
        for (index, char) in base32.enumerated() {
            lookup[char] = index
        }
        return lookup
    }()

    public static func encode(latitude: Double, longitude: Double, precision: Int = 12) -> String {
        precondition(latitude >= -90 && latitude <= 90, "Latitude must be in [-90, 90]")
        precondition(longitude >= -180 && longitude <= 180, "Longitude must be in [-180, 180]")
        precondition(precision >= 1 && precision <= 12, "Precision must be in [1, 12]")

        var geohash = ""
        var minLat = -90.0
        var maxLat = 90.0
        var minLon = -180.0
        var maxLon = 180.0
        var bit = 0
        var ch = 0
        var even = true

        while geohash.count < precision {
            if even {
                let mid = (minLon + maxLon) / 2
                if longitude > mid {
                    ch |= (1 << (4 - bit))
                    minLon = mid
                } else {
                    maxLon = mid
                }
            } else {
                let mid = (minLat + maxLat) / 2
                if latitude > mid {
                    ch |= (1 << (4 - bit))
                    minLat = mid
                } else {
                    maxLat = mid
                }
            }

            even = !even

            if bit < 4 {
                bit += 1
            } else {
                geohash.append(base32[ch])
                bit = 0
                ch = 0
            }
        }

        return geohash
    }

    public static func decode(_ geohash: String) -> (minLat: Double, minLon: Double, maxLat: Double, maxLon: Double) {
        var minLat = -90.0
        var maxLat = 90.0
        var minLon = -180.0
        var maxLon = 180.0
        var even = true

        for char in geohash.lowercased() {
            guard let idx = base32Lookup[char] else {
                continue
            }

            for bit in (0..<5).reversed() {
                let mask = 1 << bit

                if even {
                    let mid = (minLon + maxLon) / 2
                    if (idx & mask) != 0 {
                        minLon = mid
                    } else {
                        maxLon = mid
                    }
                } else {
                    let mid = (minLat + maxLat) / 2
                    if (idx & mask) != 0 {
                        minLat = mid
                    } else {
                        maxLat = mid
                    }
                }

                even = !even
            }
        }

        return (minLat, minLon, maxLat, maxLon)
    }

    public static func decodeCenter(_ geohash: String) -> (latitude: Double, longitude: Double) {
        let bounds = decode(geohash)
        let lat = (bounds.minLat + bounds.maxLat) / 2
        let lon = (bounds.minLon + bounds.maxLon) / 2
        return (lat, lon)
    }

    public static func neighbors(_ geohash: String) -> [String] {
        return [
            neighbor(geohash, direction: .north),
            neighbor(geohash, direction: .northEast),
            neighbor(geohash, direction: .east),
            neighbor(geohash, direction: .southEast),
            neighbor(geohash, direction: .south),
            neighbor(geohash, direction: .southWest),
            neighbor(geohash, direction: .west),
            neighbor(geohash, direction: .northWest)
        ].compactMap { $0 }
    }

    public static func neighbor(_ geohash: String, direction: Direction) -> String? {
        guard !geohash.isEmpty else { return nil }

        switch direction {
        case .northEast:
            guard let n = neighbor(geohash, direction: .north) else { return nil }
            return neighbor(n, direction: .east)
        case .northWest:
            guard let n = neighbor(geohash, direction: .north) else { return nil }
            return neighbor(n, direction: .west)
        case .southEast:
            guard let s = neighbor(geohash, direction: .south) else { return nil }
            return neighbor(s, direction: .east)
        case .southWest:
            guard let s = neighbor(geohash, direction: .south) else { return nil }
            return neighbor(s, direction: .west)
        default:
            break
        }

        let lastChar = geohash.last!
        var base = String(geohash.dropLast())

        let neighborMap = direction.neighborMap(even: geohash.count % 2 == 0)
        let borderMap = direction.borderMap(even: geohash.count % 2 == 0)

        if borderMap.contains(lastChar) {
            guard let parentNeighbor = neighbor(base, direction: direction) else {
                return nil
            }
            base = parentNeighbor
        }

        guard let base32Index = base32.firstIndex(of: lastChar) else {
            return base + String(lastChar)
        }

        let charIndex = base32.distance(from: base32.startIndex, to: base32Index)
        let neighborChar = neighborMap[neighborMap.index(neighborMap.startIndex, offsetBy: charIndex)]
        return base + String(neighborChar)
    }

    public enum Direction {
        case north, south, east, west
        case northEast, northWest, southEast, southWest

        fileprivate func neighborMap(even: Bool) -> String {
            switch self {
            case .north: return even ? "p0r21436x8zb9dcf5h7kjnmqesgutwvy" : "bc01fg45238967deuvhjyznpkmstqrwx"
            case .south: return even ? "14365h7k9dcfesgujnmqp0r2twvyx8zb" : "238967debc01fg45kmstqrwxuvhjyznp"
            case .east: return even ? "bc01fg45238967deuvhjyznpkmstqrwx" : "p0r21436x8zb9dcf5h7kjnmqesgutwvy"
            case .west: return even ? "238967debc01fg45kmstqrwxuvhjyznp" : "14365h7k9dcfesgujnmqp0r2twvyx8zb"
            default: return ""
            }
        }

        fileprivate func borderMap(even: Bool) -> String {
            switch self {
            case .north: return even ? "prxz" : "bcfguvyz"
            case .south: return even ? "028b" : "0145hjnp"
            case .east: return even ? "bcfguvyz" : "prxz"
            case .west: return even ? "0145hjnp" : "028b"
            default: return ""
            }
        }
    }

    public static func optimalPrecision(boundingBoxSizeKm: Double) -> Int {
        let cellSizes: [Double] = [
            5000, 1250, 156, 39, 4.9, 1.2, 0.15, 0.019, 0.0048, 0.0012, 0.00015, 0.000038
        ]

        for (precision, cellSize) in cellSizes.enumerated() {
            if cellSize < boundingBoxSizeKm {
                return min(precision + 1, 12)
            }
        }

        return 12
    }

    public static func boundingBoxSizeKm(minLat: Double, maxLat: Double, minLon: Double, maxLon: Double) -> Double {
        let centerLat = (minLat + maxLat) / 2
        let latDiff = maxLat - minLat
        let lonDiff = maxLon - minLon

        let latDistanceKm = latDiff * 111.0
        let lonDistanceKm = lonDiff * 111.0 * cos(centerLat * .pi / 180.0)

        return max(latDistanceKm, lonDistanceKm)
    }

    public static func coveringGeohashes(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        precision: Int
    ) -> [String] {
        var geohashes = Set<String>()

        if minLon > maxLon {
            let westHashes = coveringGeohashesSimple(
                minLat: minLat, minLon: minLon,
                maxLat: maxLat, maxLon: 180.0,
                precision: precision
            )
            let eastHashes = coveringGeohashesSimple(
                minLat: minLat, minLon: -180.0,
                maxLat: maxLat, maxLon: maxLon,
                precision: precision
            )
            geohashes.formUnion(westHashes)
            geohashes.formUnion(eastHashes)
        } else {
            geohashes = coveringGeohashesSimple(
                minLat: minLat, minLon: minLon,
                maxLat: maxLat, maxLon: maxLon,
                precision: precision
            )
        }

        return Array(geohashes).sorted()
    }

    private static func coveringGeohashesSimple(
        minLat: Double,
        minLon: Double,
        maxLat: Double,
        maxLon: Double,
        precision: Int
    ) -> Set<String> {
        var geohashes = Set<String>()

        let clampedMinLat = max(-90.0, minLat)
        let clampedMaxLat = min(90.0, maxLat)

        let latStep = (clampedMaxLat - clampedMinLat) / 10.0
        let lonStep = (maxLon - minLon) / 10.0

        var lat = clampedMinLat
        while lat <= clampedMaxLat {
            var lon = minLon
            while lon <= maxLon {
                let hash = encode(latitude: lat, longitude: lon, precision: precision)
                geohashes.insert(hash)

                for neighbor in neighbors(hash) {
                    geohashes.insert(neighbor)
                }

                lon += Swift.max(lonStep, 0.001)
            }
            lat += Swift.max(latStep, 0.001)
        }

        let corners = [
            (clampedMinLat, minLon),
            (clampedMinLat, maxLon),
            (clampedMaxLat, minLon),
            (clampedMaxLat, maxLon)
        ]

        for (lat, lon) in corners {
            let hash = encode(latitude: lat, longitude: lon, precision: precision)
            geohashes.insert(hash)
            for neighbor in neighbors(hash) {
                geohashes.insert(neighbor)
            }
        }

        return geohashes
    }
}

import DatabaseKit
import DatabaseTypes

enum SpatialCodeEncoder {
    static func encode(
        _ coordinate: SpatialIndexStoredCoordinate,
        encoding: SpatialEncoding,
        level: Int
    ) -> UInt64 {
        switch encoding {
        case .s2:
            return S2Geometry.encode(
                latitude: coordinate.point.latitude,
                longitude: coordinate.point.longitude,
                level: level
            )
        case .morton:
            let x = MortonCode.normalize(
                coordinate.point.longitude,
                min: -180,
                max: 180
            )
            let y = MortonCode.normalize(
                coordinate.point.latitude,
                min: -90,
                max: 90
            )
            if let height = coordinate.height {
                let z = MortonCode.normalize(
                    height,
                    min: -1_000,
                    max: 10_000
                )
                return MortonCode.encode3D(
                    x: x,
                    y: y,
                    z: z,
                    level: level
                )
            }
            return MortonCode.encode2D(x: x, y: y, level: level)
        }
    }
}

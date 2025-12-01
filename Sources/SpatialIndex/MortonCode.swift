// MortonCode.swift
// SpatialIndexLayer - Morton Code (Z-order curve) encoding
//
// Complete Morton Code implementation ported from fdb-record-layer.

import Foundation

/// Morton Code (Z-order curve) encoding for Cartesian coordinates
public enum MortonCode {

    public static func encode2D(x: Double, y: Double, level: Int = 18) -> UInt64 {
        precondition(x >= 0.0 && x <= 1.0, "x must be in [0, 1]")
        precondition(y >= 0.0 && y <= 1.0, "y must be in [0, 1]")
        precondition(level >= 0 && level <= 30, "level must be 0-30")

        let maxValue = Double((1 << level) - 1)
        let xi = UInt32(x * maxValue)
        let yi = UInt32(y * maxValue)

        let code = interleave2D(xi, yi)

        let shift = (30 - level) * 2
        return code << shift
    }

    public static func decode2D(_ code: UInt64, level: Int = 18) -> (x: Double, y: Double) {
        precondition(level >= 0 && level <= 30, "level must be 0-30")

        let shift = (30 - level) * 2
        let unshiftedCode = code >> shift

        let (xi, yi) = deinterleave2D(unshiftedCode)

        let maxValue = Double((1 << level) - 1)
        let x = Double(xi) / maxValue
        let y = Double(yi) / maxValue
        return (x, y)
    }

    private static func interleave2D(_ x: UInt32, _ y: UInt32) -> UInt64 {
        var xx = UInt64(x)
        var yy = UInt64(y)

        xx = (xx | (xx << 16)) & 0x0000FFFF0000FFFF
        xx = (xx | (xx << 8))  & 0x00FF00FF00FF00FF
        xx = (xx | (xx << 4))  & 0x0F0F0F0F0F0F0F0F
        xx = (xx | (xx << 2))  & 0x3333333333333333
        xx = (xx | (xx << 1))  & 0x5555555555555555

        yy = (yy | (yy << 16)) & 0x0000FFFF0000FFFF
        yy = (yy | (yy << 8))  & 0x00FF00FF00FF00FF
        yy = (yy | (yy << 4))  & 0x0F0F0F0F0F0F0F0F
        yy = (yy | (yy << 2))  & 0x3333333333333333
        yy = (yy | (yy << 1))  & 0x5555555555555555

        return xx | (yy << 1)
    }

    private static func deinterleave2D(_ code: UInt64) -> (x: UInt32, y: UInt32) {
        var x = code & 0x5555555555555555
        var y = (code >> 1) & 0x5555555555555555

        x = (x | (x >> 1))  & 0x3333333333333333
        x = (x | (x >> 2))  & 0x0F0F0F0F0F0F0F0F
        x = (x | (x >> 4))  & 0x00FF00FF00FF00FF
        x = (x | (x >> 8))  & 0x0000FFFF0000FFFF
        x = (x | (x >> 16)) & 0x00000000FFFFFFFF

        y = (y | (y >> 1))  & 0x3333333333333333
        y = (y | (y >> 2))  & 0x0F0F0F0F0F0F0F0F
        y = (y | (y >> 4))  & 0x00FF00FF00FF00FF
        y = (y | (y >> 8))  & 0x0000FFFF0000FFFF
        y = (y | (y >> 16)) & 0x00000000FFFFFFFF

        return (UInt32(x), UInt32(y))
    }

    public static func encode3D(x: Double, y: Double, z: Double, level: Int = 16) -> UInt64 {
        precondition(x >= 0.0 && x <= 1.0, "x must be in [0, 1]")
        precondition(y >= 0.0 && y <= 1.0, "y must be in [0, 1]")
        precondition(z >= 0.0 && z <= 1.0, "z must be in [0, 1]")
        precondition(level >= 0 && level <= 20, "level must be 0-20")

        let maxValue = Double((1 << level) - 1)
        let xi = UInt32(x * maxValue)
        let yi = UInt32(y * maxValue)
        let zi = UInt32(z * maxValue)

        let code = interleave3D(xi, yi, zi)

        let shift = (20 - level) * 3
        return code << shift
    }

    public static func decode3D(_ code: UInt64, level: Int = 16) -> (x: Double, y: Double, z: Double) {
        precondition(level >= 0 && level <= 20, "level must be 0-20")

        let shift = (20 - level) * 3
        let unshiftedCode = code >> shift

        let (xi, yi, zi) = deinterleave3D(unshiftedCode)

        let maxValue = Double((1 << level) - 1)
        let x = Double(xi) / maxValue
        let y = Double(yi) / maxValue
        let z = Double(zi) / maxValue
        return (x, y, z)
    }

    private static func interleave3D(_ x: UInt32, _ y: UInt32, _ z: UInt32) -> UInt64 {
        var xx = UInt64(x) & 0x1FFFFF
        var yy = UInt64(y) & 0x1FFFFF
        var zz = UInt64(z) & 0x1FFFFF

        xx = (xx | (xx << 32)) & 0x1F00000000FFFF
        xx = (xx | (xx << 16)) & 0x1F0000FF0000FF
        xx = (xx | (xx << 8))  & 0x100F00F00F00F00F
        xx = (xx | (xx << 4))  & 0x10C30C30C30C30C3
        xx = (xx | (xx << 2))  & 0x1249249249249249

        yy = (yy | (yy << 32)) & 0x1F00000000FFFF
        yy = (yy | (yy << 16)) & 0x1F0000FF0000FF
        yy = (yy | (yy << 8))  & 0x100F00F00F00F00F
        yy = (yy | (yy << 4))  & 0x10C30C30C30C30C3
        yy = (yy | (yy << 2))  & 0x1249249249249249

        zz = (zz | (zz << 32)) & 0x1F00000000FFFF
        zz = (zz | (zz << 16)) & 0x1F0000FF0000FF
        zz = (zz | (zz << 8))  & 0x100F00F00F00F00F
        zz = (zz | (zz << 4))  & 0x10C30C30C30C30C3
        zz = (zz | (zz << 2))  & 0x1249249249249249

        return xx | (yy << 1) | (zz << 2)
    }

    private static func deinterleave3D(_ code: UInt64) -> (x: UInt32, y: UInt32, z: UInt32) {
        var x = code & 0x1249249249249249
        var y = (code >> 1) & 0x1249249249249249
        var z = (code >> 2) & 0x1249249249249249

        x = (x | (x >> 2))  & 0x10C30C30C30C30C3
        x = (x | (x >> 4))  & 0x100F00F00F00F00F
        x = (x | (x >> 8))  & 0x1F0000FF0000FF
        x = (x | (x >> 16)) & 0x1F00000000FFFF
        x = (x | (x >> 32)) & 0x1FFFFF

        y = (y | (y >> 2))  & 0x10C30C30C30C30C3
        y = (y | (y >> 4))  & 0x100F00F00F00F00F
        y = (y | (y >> 8))  & 0x1F0000FF0000FF
        y = (y | (y >> 16)) & 0x1F00000000FFFF
        y = (y | (y >> 32)) & 0x1FFFFF

        z = (z | (z >> 2))  & 0x10C30C30C30C30C3
        z = (z | (z >> 4))  & 0x100F00F00F00F00F
        z = (z | (z >> 8))  & 0x1F0000FF0000FF
        z = (z | (z >> 16)) & 0x1F00000000FFFF
        z = (z | (z >> 32)) & 0x1FFFFF

        return (UInt32(x), UInt32(y), UInt32(z))
    }

    public static func normalize(_ value: Double, min: Double, max: Double) -> Double {
        precondition(max > min, "max must be greater than min")
        let clamped = Swift.max(min, Swift.min(max, value))
        return (clamped - min) / (max - min)
    }

    public static func denormalize(_ normalized: Double, min: Double, max: Double) -> Double {
        precondition(max > min, "max must be greater than min")
        return min + normalized * (max - min)
    }

    public static func boundingBox2D(
        minX: Double,
        minY: Double,
        maxX: Double,
        maxY: Double
    ) -> (minCode: UInt64, maxCode: UInt64) {
        let minCode = encode2D(x: minX, y: minY)
        let maxCode = encode2D(x: maxX, y: maxY)
        return (minCode, maxCode)
    }

    public static func boundingBox3D(
        minX: Double,
        minY: Double,
        minZ: Double,
        maxX: Double,
        maxY: Double,
        maxZ: Double
    ) -> (minCode: UInt64, maxCode: UInt64) {
        let minCode = encode3D(x: minX, y: minY, z: minZ)
        let maxCode = encode3D(x: maxX, y: maxY, z: maxZ)
        return (minCode, maxCode)
    }
}

import DatabaseTypes

/// Canonical physical value stored beside every spatial index key.
enum SpatialIndexValueCodec {
    private static let magic: [UInt8] = [0x44, 0x53, 0x49, 0x56]
    private static let version: UInt8 = 1
    private static let pointDimensions: UInt8 = 2
    private static let positionDimensions: UInt8 = 3
    private static let dimensionsOffset = 5
    private static let latitudeOffset = 6
    private static let longitudeOffset = 14
    private static let heightOffset = 22
    private static let byteCount = 30

    static func encode(_ coordinate: SpatialIndexStoredCoordinate) -> ByteString {
        // ByteString owns and deallocates one exact 30-byte allocation. This
        // synchronous borrow initializes every byte with UInt8 alignment and
        // neither the buffer nor a derived pointer escapes the closure.
        ByteString.copying(count: byteCount) { output in
            for index in magic.indices {
                output[index] = magic[index]
            }
            output[magic.count] = version
            output[dimensionsOffset] = coordinate.height == nil
                ? pointDimensions
                : positionDimensions
            write(
                coordinate.point.latitude.bitPattern,
                to: output,
                at: latitudeOffset
            )
            write(
                coordinate.point.longitude.bitPattern,
                to: output,
                at: longitudeOffset
            )
            write(
                (coordinate.height ?? 0).bitPattern,
                to: output,
                at: heightOffset
            )
        }
    }

    static func decode(
        _ bytes: ByteString
    ) throws -> SpatialIndexStoredCoordinate {
        guard bytes.count == byteCount else {
            throw SpatialIndexValueCodecError.invalidByteCount(bytes.count)
        }
        for index in magic.indices where bytes[bytes.startIndex + index]
            != magic[index] {
            throw SpatialIndexValueCodecError.invalidMagic
        }
        let storedVersion = bytes[bytes.startIndex + magic.count]
        guard storedVersion == version else {
            throw SpatialIndexValueCodecError.unsupportedVersion(storedVersion)
        }
        let dimensions = bytes[bytes.startIndex + dimensionsOffset]
        guard dimensions == pointDimensions
            || dimensions == positionDimensions else {
            throw SpatialIndexValueCodecError.invalidDimensions(dimensions)
        }
        let latitude = Double(
            bitPattern: readUInt64(from: bytes, at: latitudeOffset)
        )
        let longitude = Double(
            bitPattern: readUInt64(from: bytes, at: longitudeOffset)
        )
        let encodedHeight = Double(
            bitPattern: readUInt64(from: bytes, at: heightOffset)
        )
        guard encodedHeight.isFinite,
              !(encodedHeight == 0 && encodedHeight.bitPattern != 0),
              dimensions != pointDimensions || encodedHeight == 0 else {
            throw SpatialIndexValueCodecError.invalidHeight
        }
        do {
            return SpatialIndexStoredCoordinate(
                point: try GeographicPoint(
                    latitude: latitude,
                    longitude: longitude
                ),
                height: dimensions == positionDimensions
                    ? encodedHeight
                    : nil
            )
        } catch {
            throw SpatialIndexValueCodecError.invalidPoint
        }
    }

    private static func write(
        _ value: UInt64,
        to output: UnsafeMutableRawBufferPointer,
        at offset: Int
    ) {
        for index in 0..<MemoryLayout<UInt64>.size {
            let shift = UInt64((MemoryLayout<UInt64>.size - index - 1) * 8)
            output[offset + index] = UInt8(
                truncatingIfNeeded: value >> shift
            )
        }
    }

    private static func readUInt64(
        from bytes: ByteString,
        at offset: Int
    ) -> UInt64 {
        var value: UInt64 = 0
        for index in 0..<MemoryLayout<UInt64>.size {
            value = (value << 8)
                | UInt64(bytes[bytes.startIndex + offset + index])
        }
        return value
    }
}

enum SpatialIndexValueCodecError: Error, Sendable, Equatable {
    case invalidByteCount(Int)
    case invalidMagic
    case unsupportedVersion(UInt8)
    case invalidDimensions(UInt8)
    case invalidPoint
    case invalidHeight
}

struct SpatialIndexStoredCoordinate: Sendable, Equatable {
    let point: GeographicPoint
    let height: Double?

    init(point: GeographicPoint, height: Double?) {
        precondition(height?.isFinite ?? true)
        self.point = point
        self.height = height.map { $0 == 0 ? 0 : $0 }
    }
}

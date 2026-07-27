import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

internal protocol HashBytesConvertible {
    func appendHashBytes(to stream: inout MurmurHash3.Stream)
}

extension String: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.update(utf8)
    }
}

extension Int: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.updateLittleEndian(Int64(self))
    }
}

extension Int64: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.updateLittleEndian(self)
    }
}

extension UInt64: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.updateLittleEndian(self)
    }
}

extension Double: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.updateLittleEndian(bitPattern)
    }
}

extension Bool: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.update(byte: self ? 1 : 0)
    }
}

extension Data: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.update(self)
    }
}

extension ByteString: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.update(self)
    }
}

extension Array: HashBytesConvertible where Element: HashBytesConvertible {
    @usableFromInline
    func appendHashBytes(to stream: inout MurmurHash3.Stream) {
        stream.updateLittleEndian(Int64(count))
        for element in self {
            var measuringStream = MurmurHash3.Stream()
            element.appendHashBytes(to: &measuringStream)
            stream.updateLittleEndian(Int64(measuringStream.byteCount))
            element.appendHashBytes(to: &stream)
        }
    }
}

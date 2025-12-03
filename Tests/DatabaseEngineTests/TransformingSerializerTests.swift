// TransformingSerializerTests.swift
// DatabaseEngine Tests - TransformingSerializer compression tests

import Testing
import Foundation
@testable import DatabaseEngine

@Suite("TransformingSerializer Tests")
struct TransformingSerializerTests {

    // MARK: - Basic Compression Tests

    @Test("Compress and decompress data")
    func compressAndDecompress() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Create compressible data (repeated pattern)
        let original = Data(repeating: 0x41, count: 1000)

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
    }

    @Test("Compression reduces size for compressible data")
    func compressionReducesSize() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Highly compressible data (repeated pattern)
        let original = Data(repeating: 0x41, count: 10000)

        let compressed = try serializer.serializeSync(original)

        // Should be significantly smaller
        #expect(compressed.count < original.count / 2)
    }

    @Test("Small data skips compression")
    func smallDataSkipsCompression() throws {
        let config = TransformConfiguration(
            compressionEnabled: true,
            compressionMinSize: 100
        )
        let serializer = TransformingSerializer(configuration: config)

        // Small data (under threshold)
        let original = Data([0x01, 0x02, 0x03, 0x04, 0x05])

        let result = try serializer.serializeSync(original)

        // First byte is flags (0x00 = no transformation)
        #expect(result[0] == 0x00)
        // Rest is original data
        #expect(Data(result[1...]) == original)
    }

    @Test("Incompressible data skips compression")
    func incompressibleDataSkipsCompression() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Random-like data (incompressible)
        var original = Data(count: 1000)
        for i in 0..<original.count {
            original[i] = UInt8(truncatingIfNeeded: i * 7 + 13)
        }

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        // Should still round-trip correctly
        #expect(decompressed == original)
    }

    // MARK: - Header Tests

    @Test("Compressed data has correct header")
    func compressedDataHeader() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Compressible data
        let original = Data(repeating: 0x41, count: 1000)

        let compressed = try serializer.serializeSync(original)

        // First byte: flags (0x01 = compressed)
        #expect(compressed[0] == 0x01)
        // Second byte: algorithm (0x01 = zlib)
        #expect(compressed[1] == 0x01)
    }

    @Test("Uncompressed data has correct header")
    func uncompressedDataHeader() throws {
        let config = TransformConfiguration(
            compressionEnabled: false
        )
        let serializer = TransformingSerializer(configuration: config)

        let original = Data([0x01, 0x02, 0x03])

        let result = try serializer.serializeSync(original)

        // First byte: flags (0x00 = no transformation)
        #expect(result[0] == 0x00)
    }

    // MARK: - Empty Data Tests

    @Test("Empty data handling")
    func emptyDataHandling() throws {
        let serializer = TransformingSerializer(configuration: .default)

        let original = Data()

        let result = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(result)

        #expect(decompressed == original)
    }

    @Test("Deserialize empty data returns empty")
    func deserializeEmptyData() throws {
        let serializer = TransformingSerializer(configuration: .default)

        let result = try serializer.deserializeSync(Data())

        #expect(result.isEmpty)
    }

    // MARK: - Different Compression Algorithms

    @Test("LZ4 compression works")
    func lz4Compression() throws {
        let config = TransformConfiguration(
            compressionEnabled: true,
            compressionAlgorithm: .lz4
        )
        let serializer = TransformingSerializer(configuration: config)

        let original = Data(repeating: 0x42, count: 1000)

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
        #expect(compressed[1] == 0x00) // LZ4 algorithm byte
    }

    @Test("LZFSE compression works")
    func lzfseCompression() throws {
        let config = TransformConfiguration(
            compressionEnabled: true,
            compressionAlgorithm: .lzfse
        )
        let serializer = TransformingSerializer(configuration: config)

        let original = Data(repeating: 0x43, count: 1000)

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
        #expect(compressed[1] == 0x03) // LZFSE algorithm byte
    }

    // MARK: - Large Data Tests

    @Test("Large data compression")
    func largeDataCompression() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // 100KB of compressible data
        let original = Data(repeating: 0x44, count: 100_000)

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
        #expect(compressed.count < original.count)
    }

    // MARK: - Configuration Tests

    @Test("None configuration disables compression")
    func noneConfigurationDisablesCompression() throws {
        let serializer = TransformingSerializer(configuration: .none)

        let original = Data(repeating: 0x45, count: 1000)

        let result = try serializer.serializeSync(original)

        // First byte: flags (0x00 = no transformation)
        #expect(result[0] == 0x00)
        // Size should be original + 1 (header byte)
        #expect(result.count == original.count + 1)
    }

    @Test("Custom min size threshold")
    func customMinSizeThreshold() throws {
        let config = TransformConfiguration(
            compressionEnabled: true,
            compressionMinSize: 500
        )
        let serializer = TransformingSerializer(configuration: config)

        // Data smaller than threshold
        let smallData = Data(repeating: 0x46, count: 200)
        let smallResult = try serializer.serializeSync(smallData)
        #expect(smallResult[0] == 0x00) // No compression

        // Data larger than threshold
        let largeData = Data(repeating: 0x46, count: 600)
        let largeResult = try serializer.serializeSync(largeData)
        #expect(largeResult[0] == 0x01) // Compressed
    }

    // MARK: - Real-world Data Tests

    @Test("JSON-like data compression")
    func jsonLikeDataCompression() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Simulate JSON-like data with repetitive structure
        let jsonLike = """
        {"users":[{"id":1,"name":"Alice","email":"alice@example.com"},{"id":2,"name":"Bob","email":"bob@example.com"},{"id":3,"name":"Charlie","email":"charlie@example.com"}]}
        """
        let original = Data(jsonLike.utf8)

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
    }

    @Test("Protobuf-like binary data compression")
    func protobufLikeBinaryCompression() throws {
        let serializer = TransformingSerializer(configuration: .default)

        // Simulate Protobuf-like binary data
        var original = Data()
        for i in 0..<100 {
            // Field tag + value pattern
            original.append(contentsOf: [0x08, UInt8(i % 128)])
            original.append(contentsOf: [0x12, 0x05])
            original.append(contentsOf: Array("hello".utf8))
        }

        let compressed = try serializer.serializeSync(original)
        let decompressed = try serializer.deserializeSync(compressed)

        #expect(decompressed == original)
    }
}

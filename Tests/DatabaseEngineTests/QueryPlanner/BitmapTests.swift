// BitmapTests.swift
// Tests for Bitmap data structure and operations

import Testing
import Foundation
@testable import DatabaseEngine
@testable import Core

@Suite("Bitmap Tests")
struct BitmapTests {

    // MARK: - Basic Operations Tests

    @Test("Bitmap initialization creates correct size")
    func testBitmapInitialization() {
        let bitmap = Bitmap(bitCount: 100)

        #expect(bitmap.bitCount == 100)
        #expect(bitmap.popCount == 0) // All bits initially clear
    }

    @Test("Set and check single bit")
    func testSetAndCheckBit() {
        var bitmap = Bitmap(bitCount: 100)

        bitmap.set(42)

        #expect(bitmap.isSet(42) == true)
        #expect(bitmap.isSet(41) == false)
        #expect(bitmap.isSet(43) == false)
    }

    @Test("Set multiple bits")
    func testSetMultipleBits() {
        var bitmap = Bitmap(bitCount: 100)

        bitmap.set(0)
        bitmap.set(10)
        bitmap.set(50)
        bitmap.set(99)

        #expect(bitmap.isSet(0) == true)
        #expect(bitmap.isSet(10) == true)
        #expect(bitmap.isSet(50) == true)
        #expect(bitmap.isSet(99) == true)
        #expect(bitmap.popCount == 4)
    }

    @Test("Clear bit")
    func testClearBit() {
        var bitmap = Bitmap(bitCount: 100)

        bitmap.set(42)
        #expect(bitmap.isSet(42) == true)

        bitmap.clear(42)
        #expect(bitmap.isSet(42) == false)
    }

    @Test("Out of bounds access is safe")
    func testOutOfBoundsAccess() {
        var bitmap = Bitmap(bitCount: 10)

        // Setting out of bounds should be ignored
        bitmap.set(100)
        #expect(bitmap.popCount == 0)

        // Checking out of bounds should return false
        #expect(bitmap.isSet(100) == false)
    }

    @Test("Get set positions")
    func testSetPositions() {
        var bitmap = Bitmap(bitCount: 100)

        bitmap.set(5)
        bitmap.set(10)
        bitmap.set(75)

        let positions = bitmap.setPositions
        #expect(positions.count == 3)
        #expect(positions.contains(5))
        #expect(positions.contains(10))
        #expect(positions.contains(75))
    }

    // MARK: - Population Count Tests

    @Test("Population count is accurate")
    func testPopCount() {
        var bitmap = Bitmap(bitCount: 1000)

        for i in stride(from: 0, to: 1000, by: 10) {
            bitmap.set(i)
        }

        #expect(bitmap.popCount == 100) // 0, 10, 20, ..., 990
    }

    @Test("Population count for empty bitmap")
    func testPopCountEmpty() {
        let bitmap = Bitmap(bitCount: 1000)
        #expect(bitmap.popCount == 0)
    }

    @Test("Population count for full bitmap")
    func testPopCountFull() {
        var bitmap = Bitmap(bitCount: 64)

        for i in 0..<64 {
            bitmap.set(i)
        }

        #expect(bitmap.popCount == 64)
    }

    // MARK: - AND Operation Tests

    @Test("AND operation on identical bitmaps")
    func testAndIdentical() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        for i in [10, 20, 30, 40, 50] {
            bitmap1.set(i)
            bitmap2.set(i)
        }

        let result = bitmap1.and(bitmap2)

        #expect(result.popCount == 5)
        #expect(result.isSet(10))
        #expect(result.isSet(30))
        #expect(result.isSet(50))
    }

    @Test("AND operation on disjoint bitmaps")
    func testAndDisjoint() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap2.set(30)
        bitmap2.set(40)

        let result = bitmap1.and(bitmap2)

        #expect(result.popCount == 0)
    }

    @Test("AND operation on overlapping bitmaps")
    func testAndOverlapping() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap1.set(30)

        bitmap2.set(20)
        bitmap2.set(30)
        bitmap2.set(40)

        let result = bitmap1.and(bitmap2)

        #expect(result.popCount == 2)
        #expect(result.isSet(20))
        #expect(result.isSet(30))
        #expect(!result.isSet(10))
        #expect(!result.isSet(40))
    }

    // MARK: - OR Operation Tests

    @Test("OR operation on disjoint bitmaps")
    func testOrDisjoint() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap2.set(30)
        bitmap2.set(40)

        let result = bitmap1.or(bitmap2)

        #expect(result.popCount == 4)
        #expect(result.isSet(10))
        #expect(result.isSet(20))
        #expect(result.isSet(30))
        #expect(result.isSet(40))
    }

    @Test("OR operation on overlapping bitmaps")
    func testOrOverlapping() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap2.set(20)
        bitmap2.set(30)

        let result = bitmap1.or(bitmap2)

        #expect(result.popCount == 3)
        #expect(result.isSet(10))
        #expect(result.isSet(20))
        #expect(result.isSet(30))
    }

    // MARK: - NOT Operation Tests

    @Test("NOT operation inverts bitmap")
    func testNot() {
        var bitmap = Bitmap(bitCount: 8)

        bitmap.set(0)
        bitmap.set(2)
        bitmap.set(4)
        bitmap.set(6)

        let inverted = bitmap.not()

        #expect(inverted.isSet(0) == false)
        #expect(inverted.isSet(1) == true)
        #expect(inverted.isSet(2) == false)
        #expect(inverted.isSet(3) == true)
        #expect(inverted.isSet(4) == false)
        #expect(inverted.isSet(5) == true)
        #expect(inverted.isSet(6) == false)
        #expect(inverted.isSet(7) == true)
    }

    @Test("Double NOT returns original")
    func testDoubleNot() {
        var bitmap = Bitmap(bitCount: 100)
        bitmap.set(10)
        bitmap.set(50)
        bitmap.set(90)

        let doubleInverted = bitmap.not().not()

        #expect(doubleInverted.isSet(10))
        #expect(doubleInverted.isSet(50))
        #expect(doubleInverted.isSet(90))
        #expect(doubleInverted.popCount == bitmap.popCount)
    }

    // MARK: - XOR Operation Tests

    @Test("XOR operation on disjoint bitmaps")
    func testXorDisjoint() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap2.set(30)
        bitmap2.set(40)

        let result = bitmap1.xor(bitmap2)

        #expect(result.popCount == 4)
    }

    @Test("XOR operation removes common bits")
    func testXorRemovesCommon() {
        var bitmap1 = Bitmap(bitCount: 100)
        var bitmap2 = Bitmap(bitCount: 100)

        bitmap1.set(10)
        bitmap1.set(20)
        bitmap1.set(30)

        bitmap2.set(20)
        bitmap2.set(30)
        bitmap2.set(40)

        let result = bitmap1.xor(bitmap2)

        #expect(result.popCount == 2) // Only 10 and 40 remain
        #expect(result.isSet(10))
        #expect(!result.isSet(20))
        #expect(!result.isSet(30))
        #expect(result.isSet(40))
    }

    @Test("XOR with self produces empty bitmap")
    func testXorWithSelf() {
        var bitmap = Bitmap(bitCount: 100)
        bitmap.set(10)
        bitmap.set(50)

        let result = bitmap.xor(bitmap)

        #expect(result.popCount == 0)
    }

    // MARK: - Complex Operation Tests

    @Test("Complex AND-OR combination")
    func testComplexAndOr() {
        // Simulate: (status = 'active' OR status = 'pending') AND department = 'Engineering'
        var active = Bitmap(bitCount: 100)
        var pending = Bitmap(bitCount: 100)
        var engineering = Bitmap(bitCount: 100)

        // Active users: 0, 10, 20, 30, 40
        for i in stride(from: 0, to: 50, by: 10) { active.set(i) }

        // Pending users: 5, 15, 25, 35
        for i in stride(from: 5, to: 40, by: 10) { pending.set(i) }

        // Engineering department: 10, 15, 30
        engineering.set(10)
        engineering.set(15)
        engineering.set(30)

        // (active OR pending) AND engineering
        let result = active.or(pending).and(engineering)

        // Should be: 10, 15, 30 (active or pending AND in engineering)
        #expect(result.popCount == 3)
        #expect(result.isSet(10)) // active AND engineering
        #expect(result.isSet(15)) // pending AND engineering
        #expect(result.isSet(30)) // active AND engineering
    }

    // MARK: - Edge Cases

    @Test("Single bit bitmap")
    func testSingleBitBitmap() {
        var bitmap = Bitmap(bitCount: 1)

        #expect(bitmap.isSet(0) == false)

        bitmap.set(0)
        #expect(bitmap.isSet(0) == true)
        #expect(bitmap.popCount == 1)

        bitmap.clear(0)
        #expect(bitmap.isSet(0) == false)
        #expect(bitmap.popCount == 0)
    }

    @Test("Bitmap at word boundary")
    func testWordBoundary() {
        var bitmap = Bitmap(bitCount: 64)

        bitmap.set(63) // Last bit of first word
        #expect(bitmap.isSet(63) == true)
        #expect(bitmap.popCount == 1)
    }

    @Test("Bitmap spanning multiple words")
    func testMultipleWords() {
        var bitmap = Bitmap(bitCount: 200)

        // Set bits in different words
        bitmap.set(0)   // Word 0
        bitmap.set(63)  // Word 0 (last bit)
        bitmap.set(64)  // Word 1 (first bit)
        bitmap.set(128) // Word 2
        bitmap.set(199) // Word 3 (last used bit)

        #expect(bitmap.popCount == 5)

        let positions = bitmap.setPositions
        #expect(positions == [0, 63, 64, 128, 199])
    }
}

// MARK: - BitmapIndexKind Tests

@Suite("BitmapIndexKind Tests")
struct BitmapIndexKindTests {

    @Test("BitmapIndexKind initialization")
    func testBitmapIndexKindInit() {
        let kind = BitmapIndexKind(
            name: "idx_status",
            fieldName: "status",
            expectedCardinality: 3,
            compression: .roaring
        )

        #expect(kind.name == "idx_status")
        #expect(kind.fieldName == "status")
        #expect(kind.expectedCardinality == 3)
        #expect(kind.compression == .roaring)
    }

    @Test("BitmapIndexKind default compression")
    func testBitmapIndexKindDefaultCompression() {
        let kind = BitmapIndexKind(
            name: "idx_status",
            fieldName: "status"
        )

        #expect(kind.compression == .runLength)
    }

    @Test("BitmapIndexKind equality")
    func testBitmapIndexKindEquality() {
        let kind1 = BitmapIndexKind(name: "idx_status", fieldName: "status")
        let kind2 = BitmapIndexKind(name: "idx_status", fieldName: "status")
        let kind3 = BitmapIndexKind(name: "idx_other", fieldName: "other")

        #expect(kind1 == kind2)
        #expect(kind1 != kind3)
    }
}

// MARK: - BitmapCompression Tests

@Suite("BitmapCompression Tests")
struct BitmapCompressionTests {

    @Test("All compression types are distinct")
    func testCompressionTypes() {
        #expect(BitmapCompression.none != BitmapCompression.runLength)
        #expect(BitmapCompression.runLength != BitmapCompression.wordAligned)
        #expect(BitmapCompression.wordAligned != BitmapCompression.roaring)
    }
}

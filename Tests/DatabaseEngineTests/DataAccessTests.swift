// DataAccessTests.swift
// FDBIndexing Tests - DataAccess tests for nested field extraction

import Testing
import Foundation
import Core
@testable import DatabaseEngine
@testable import Core

// MARK: - Test Structures

/// Test address structure (nested type)
struct TestAddress: Sendable, Codable {
    var street: String
    var city: String
    var zipCode: String
}

/// Test profile structure (deeply nested)
struct TestProfile: Sendable, Codable {
    var bio: String
    var website: String
}

/// Test user with nested address
@Persistable
struct TestUserWithAddress {
    var email: String
    var name: String
    var address: TestAddress
}

/// Test user with deeply nested profile
@Persistable
struct TestUserWithProfile {
    var email: String
    var name: String
    var profile: TestProfile
    var address: TestAddress
}

/// Simple test user without nested fields
@Persistable
struct TestSimpleUser {
    var email: String
    var name: String
    var age: Int64
}

// MARK: - DataAccess Tests

@Suite("DataAccess Tests")
struct DataAccessTests {

    // MARK: - Simple Field Extraction Tests

    @Test("extractField extracts simple string field")
    func testExtractSimpleStringField() throws {
        let user = TestSimpleUser(email: "test@example.com", name: "Test User", age: 30)

        let values = try DataAccess.extractField(from: user, keyPath: "email")

        #expect(values.count == 1)
        #expect((values[0] as? String) == "test@example.com")
    }

    @Test("extractField extracts simple integer field")
    func testExtractSimpleIntegerField() throws {
        let user = TestSimpleUser(email: "test@example.com", name: "Test User", age: 30)

        let values = try DataAccess.extractField(from: user, keyPath: "age")

        #expect(values.count == 1)
        #expect((values[0] as? Int64) == 30)
    }

    @Test("extractField throws for non-existent field")
    func testExtractNonExistentField() throws {
        let user = TestSimpleUser(email: "test@example.com", name: "Test User", age: 30)

        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "nonExistent")
        }
    }

    // MARK: - Nested Field Extraction Tests

    @Test("extractField extracts nested field with dot notation")
    func testExtractNestedField() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        let values = try DataAccess.extractField(from: user, keyPath: "address.city")

        #expect(values.count == 1)
        #expect((values[0] as? String) == "San Francisco")
    }

    @Test("extractField extracts all nested fields")
    func testExtractAllNestedFields() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        let streetValues = try DataAccess.extractField(from: user, keyPath: "address.street")
        let cityValues = try DataAccess.extractField(from: user, keyPath: "address.city")
        let zipValues = try DataAccess.extractField(from: user, keyPath: "address.zipCode")

        #expect((streetValues[0] as? String) == "123 Main St")
        #expect((cityValues[0] as? String) == "San Francisco")
        #expect((zipValues[0] as? String) == "94102")
    }

    @Test("extractField throws for non-existent nested field")
    func testExtractNonExistentNestedField() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "address.nonExistent")
        }
    }

    @Test("extractField throws for invalid nested path")
    func testExtractInvalidNestedPath() throws {
        let user = TestSimpleUser(email: "test@example.com", name: "Test User", age: 30)

        // email is not a struct, so email.something should fail
        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "email.something")
        }
    }

    // MARK: - KeyExpression Evaluation Tests

    @Test("evaluate simple FieldKeyExpression")
    func testEvaluateSimpleFieldExpression() throws {
        let user = TestSimpleUser(email: "test@example.com", name: "Test User", age: 30)
        let expr = FieldKeyExpression(fieldName: "email")

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 1)
        #expect((values[0] as? String) == "test@example.com")
    }

    @Test("evaluate NestExpression for nested field")
    func testEvaluateNestExpression() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build NestExpression: address.city
        let childExpr = FieldKeyExpression(fieldName: "city")
        let nestExpr = NestExpression(parentField: "address", child: childExpr)

        let values = try DataAccess.evaluate(item: user, expression: nestExpr)

        #expect(values.count == 1)
        #expect((values[0] as? String) == "San Francisco")
    }

    @Test("evaluate ConcatenateKeyExpression with nested field")
    func testEvaluateConcatenateWithNested() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build: [email, address.city]
        let emailExpr = FieldKeyExpression(fieldName: "email")
        let cityExpr = NestExpression(
            parentField: "address",
            child: FieldKeyExpression(fieldName: "city")
        )
        let concatExpr = ConcatenateKeyExpression(children: [emailExpr, cityExpr])

        let values = try DataAccess.evaluate(item: user, expression: concatExpr)

        #expect(values.count == 2)
        #expect((values[0] as? String) == "test@example.com")
        #expect((values[1] as? String) == "San Francisco")
    }

    @Test("evaluate KeyExpression created from factory")
    func testEvaluateFactoryCreatedExpression() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Use factory to create expression from dot notation
        let expr = KeyExpressionFactory.from(dotNotation: "address.city")

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 1)
        #expect((values[0] as? String) == "San Francisco")
    }

    @Test("evaluate composite index expression with nested fields")
    func testEvaluateCompositeIndexExpression() throws {
        let address = TestAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = TestUserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build composite index: [address.city, address.zipCode]
        let expr = KeyExpressionFactory.from(keyPaths: ["address.city", "address.zipCode"])

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 2)
        #expect((values[0] as? String) == "San Francisco")
        #expect((values[1] as? String) == "94102")
    }
}

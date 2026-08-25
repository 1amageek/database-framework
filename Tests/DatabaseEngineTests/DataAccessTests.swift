#if !os(WASI)
// DataAccessTests.swift
// FDBIndexing Tests - DataAccess tests for nested field extraction

import Testing
import TestHeartbeat
import Foundation
import DatabaseKit
import DatabaseTypes
@testable import DatabaseEngine
@testable import DatabaseKit

// MARK: - Test Structures

/// Test address structure (nested type)
struct MailingAddress:
    Sendable,
    FieldValueEncodable,
    FieldValueDecodable
{
    var street: String
    var city: String
    var zipCode: String

    static var fieldSchemaType: FieldSchemaType { .nested }

    func encodeFieldValue() throws(PersistableEncodingError) -> FieldValue {
        .object(
            try nestedFieldObject(
                entity: "MailingAddress",
                fields: [
                    (key: "street", value: .string(street)),
                    (key: "city", value: .string(city)),
                    (key: "zipCode", value: .string(zipCode)),
                ]
            )
        )
    }

    static func decodeFieldValue(
        _ value: FieldValue,
        field: String
    ) throws(PersistableDecodingError) -> MailingAddress {
        guard case .object(let object) = value,
              case .string(let street) = object["street"],
              case .string(let city) = object["city"],
              case .string(let zipCode) = object["zipCode"] else {
            throw .invalidValue(
                field: field,
                expected: "a mailing address object"
            )
        }
        return MailingAddress(
            street: street,
            city: city,
            zipCode: zipCode
        )
    }
}

/// Test profile structure (deeply nested)
struct UserProfile:
    Sendable,
    FieldValueEncodable,
    FieldValueDecodable
{
    var bio: String
    var website: String

    static var fieldSchemaType: FieldSchemaType { .nested }

    func encodeFieldValue() throws(PersistableEncodingError) -> FieldValue {
        .object(
            try nestedFieldObject(
                entity: "UserProfile",
                fields: [
                    (key: "bio", value: .string(bio)),
                    (key: "website", value: .string(website)),
                ]
            )
        )
    }

    static func decodeFieldValue(
        _ value: FieldValue,
        field: String
    ) throws(PersistableDecodingError) -> UserProfile {
        guard case .object(let object) = value,
              case .string(let bio) = object["bio"],
              case .string(let website) = object["website"] else {
            throw .invalidValue(
                field: field,
                expected: "a user profile object"
            )
        }
        return UserProfile(bio: bio, website: website)
    }
}

private func nestedFieldObject(
    entity: String,
    fields: consuming [(key: String, value: FieldValue)]
) throws(PersistableEncodingError) -> FieldObject {
    do {
        return try FieldObject(fields)
    } catch let error {
        switch error {
        case .duplicateKey(let name):
            throw .invalidSchema(
                entity: entity,
                reason: "field '\(name)' is declared more than once"
            )
        }
    }
}

/// Test user with nested address
@Persistable
struct UserWithAddress {
    var id: String = ""
    var email: String
    var name: String
    var address: MailingAddress
}

/// Test user with deeply nested profile
@Persistable
struct UserWithProfile {
    var id: String = ""
    var email: String
    var name: String
    var profile: UserProfile
    var address: MailingAddress
}

/// Simple test user without nested fields
@Persistable
struct SimpleUser {
    var id: String = ""
    var email: String
    var name: String
    var age: Int64
}

// MARK: - DataAccess Tests

@Suite("DataAccess Tests", .heartbeat)
struct DataAccessTests {

    // MARK: - Simple Field Extraction Tests

    @Test("extractField extracts simple string field")
    func testExtractSimpleStringField() throws {
        let user = SimpleUser(email: "test@example.com", name: "Test User", age: 30)

        let values = try DataAccess.extractField(from: user, keyPath: "email")

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeString(values[0]) == "test@example.com")
    }

    @Test("extractField extracts simple integer field")
    func testExtractSimpleIntegerField() throws {
        let user = SimpleUser(email: "test@example.com", name: "Test User", age: 30)

        let values = try DataAccess.extractField(from: user, keyPath: "age")

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeInt64(values[0]) == 30)
    }

    @Test("extractField throws for non-existent field")
    func testExtractNonExistentField() throws {
        let user = SimpleUser(email: "test@example.com", name: "Test User", age: 30)

        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "nonExistent")
        }
    }

    // MARK: - Nested Field Extraction Tests

    @Test("extractField extracts nested field with dot notation")
    func testExtractNestedField() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        let values = try DataAccess.extractField(from: user, keyPath: "address.city")

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeString(values[0]) == "San Francisco")
    }

    @Test("extractField extracts all nested fields")
    func testExtractAllNestedFields() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        let streetValues = try DataAccess.extractField(from: user, keyPath: "address.street")
        let cityValues = try DataAccess.extractField(from: user, keyPath: "address.city")
        let zipValues = try DataAccess.extractField(from: user, keyPath: "address.zipCode")

        #expect(try TupleDecoder.decodeString(streetValues[0]) == "123 Main St")
        #expect(try TupleDecoder.decodeString(cityValues[0]) == "San Francisco")
        #expect(try TupleDecoder.decodeString(zipValues[0]) == "94102")
    }

    @Test("extractField throws for non-existent nested field")
    func testExtractNonExistentNestedField() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "address.nonExistent")
        }
    }

    @Test("extractField throws for invalid nested path")
    func testExtractInvalidNestedPath() throws {
        let user = SimpleUser(email: "test@example.com", name: "Test User", age: 30)

        // email is not a struct, so email.something should fail
        #expect(throws: DataAccessError.self) {
            _ = try DataAccess.extractField(from: user, keyPath: "email.something")
        }
    }

    // MARK: - KeyExpression Evaluation Tests

    @Test("evaluate simple FieldKeyExpression")
    func testEvaluateSimpleFieldExpression() throws {
        let user = SimpleUser(email: "test@example.com", name: "Test User", age: 30)
        let expr = FieldKeyExpression(fieldName: "email")

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeString(values[0]) == "test@example.com")
    }

    @Test("evaluate NestExpression for nested field")
    func testEvaluateNestExpression() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build NestExpression: address.city
        let childExpr = FieldKeyExpression(fieldName: "city")
        let nestExpr = NestExpression(parentField: "address", child: childExpr)

        let values = try DataAccess.evaluate(item: user, expression: nestExpr)

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeString(values[0]) == "San Francisco")
    }

    @Test("evaluate ConcatenateKeyExpression with nested field")
    func testEvaluateConcatenateWithNested() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build: [email, address.city]
        let emailExpr = FieldKeyExpression(fieldName: "email")
        let cityExpr = NestExpression(
            parentField: "address",
            child: FieldKeyExpression(fieldName: "city")
        )
        let concatExpr = ConcatenateKeyExpression(children: [emailExpr, cityExpr])

        let values = try DataAccess.evaluate(item: user, expression: concatExpr)

        #expect(values.count == 2)
        #expect(try TupleDecoder.decodeString(values[0]) == "test@example.com")
        #expect(try TupleDecoder.decodeString(values[1]) == "San Francisco")
    }

    @Test("evaluate KeyExpression created from factory")
    func testEvaluateFactoryCreatedExpression() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Use factory to create expression from dot notation
        let expr = KeyExpressionFactory.from(dotNotation: "address.city")

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 1)
        #expect(try TupleDecoder.decodeString(values[0]) == "San Francisco")
    }

    @Test("evaluate composite index expression with nested fields")
    func testEvaluateCompositeIndexExpression() throws {
        let address = MailingAddress(street: "123 Main St", city: "San Francisco", zipCode: "94102")
        let user = UserWithAddress(email: "test@example.com", name: "Test User", address: address)

        // Build composite index: [address.city, address.zipCode]
        let expr = KeyExpressionFactory.from(keyPaths: ["address.city", "address.zipCode"])

        let values = try DataAccess.evaluate(item: user, expression: expr)

        #expect(values.count == 2)
        #expect(try TupleDecoder.decodeString(values[0]) == "San Francisco")
        #expect(try TupleDecoder.decodeString(values[1]) == "94102")
    }
}
#endif

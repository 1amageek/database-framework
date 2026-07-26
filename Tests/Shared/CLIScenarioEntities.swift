// CLIScenarioEntities.swift
// Persistable entities and index metadata for DatabaseCLI scenarios.

import Foundation
import DatabaseKit

/// User model for CLI testing with a unique email index.
@Persistable
public struct CLIUser {
    #Directory<CLIUser>("test", "cli", "users")
    #Index(
        .scalar,
        fields: [\CLIUser.email],
        unique: true,
        name: "CLIUser_email"
    )

    public var id: String = UUID().uuidString
    public var name: String = ""
    public var email: String = ""
    public var age: Int64 = 0
}

/// Order model for CLI testing with a user identifier index.
@Persistable
public struct CLIOrder {
    #Directory<CLIOrder>("test", "cli", "orders")
    #Index(
        .scalar,
        fields: [\CLIOrder.userId],
        name: "CLIOrder_userId"
    )

    public var id: String = UUID().uuidString
    public var userId: String = ""
    public var total: Double = 0.0
    public var status: String = "pending"
}

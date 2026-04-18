// TestModels.swift
// Shared test models conforming to Persistable for all test targets

import Foundation
import Core
import DatabaseEngine
import StorageKit

// MARK: - Player Model (for Rank Index / OnlineIndexer / Partitioned Directory tests)

@Persistable
public struct Player {
    #Directory<Player>("test", "players")
    public var id: String = UUID().uuidString
    public var name: String = ""
    public var score: Int64 = 0
    public var level: Int = 0
}

// MARK: - TenantOrder Model (for Partitioned Directory tests)

@Persistable
public struct TenantOrder {
    #Directory<TenantOrder>("test", "tenants", Field<TenantOrder>(\.tenantID), "orders", layer: .partition)
    public var id: String = UUID().uuidString
    public var tenantID: String = ""
    public var status: String = "pending"
    public var total: Double = 0
}

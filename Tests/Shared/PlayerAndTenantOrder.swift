// PlayerAndTenantOrder.swift
// Persistable entities shared by ranking, indexing, and partition scenarios.

import Foundation
import Core
import DatabaseValue
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

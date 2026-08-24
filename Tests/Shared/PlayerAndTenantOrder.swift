// PlayerAndTenantOrder.swift
// Persistable entities shared by ranking, indexing, and partition scenarios.

import Foundation
import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

// MARK: - Player Model (for Rank Index / OnlineIndexer / Partitioned Directory tests)

@Persistable
public struct Player {
    #Directory<Player>("test", "players")
    public var id: String = UUID().uuidString
    public var name: String = ""
    public var score: Int64 = 0
    public var level: Int64 = 0

    public static func fixture(
        id: String = UUID().uuidString,
        name: String = "",
        score: Int64 = 0,
        level: Int64 = 0
    ) -> Self {
        Self(id: id, name: name, score: score, level: level)
    }
}

// MARK: - TenantOrder Model (for Partitioned Directory tests)

@Persistable
public struct TenantOrder {
    #Directory<TenantOrder>(
        "test",
        "tenants",
        \TenantOrder.tenantID,
        "orders",
        layer: .partition
    )
    public var id: String = UUID().uuidString
    public var tenantID: String = ""
    public var status: String = "pending"
    public var total: Double = 0

    public static func fixture(
        id: String = UUID().uuidString,
        tenantID: String = "",
        status: String = "pending",
        total: Double = 0
    ) -> Self {
        Self(
            id: id,
            tenantID: tenantID,
            status: status,
            total: total
        )
    }
}

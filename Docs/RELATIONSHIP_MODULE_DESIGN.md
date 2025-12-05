# RelationshipIndex モジュール分離設計

## 背景

### 問題
Relationship 機能は RDB 以外には不要な機能であり、DatabaseEngine コアから分離すべき。
しかし、Relationship は他のインデックスと異なり、**アイテム自体の変更**が必要：

- **Cascade Delete**: 関連アイテムを削除
- **Nullify**: 関連アイテムの FK を null に変更

他のインデックス（Vector, FullText, Spatial 等）は「インデックスエントリの読み書き」のみで、アイテム自体は変更しない。

### 現状の問題点
```swift
// FDBContext に内部メソッドを追加して対応 ← 設計ミス
internal func saveModelWithIndexes(...)
internal func deleteModelWithIndexes(...)
```

## 設計方針

### Protocol による抽象化

```
┌─────────────────────────────────────────────────────────────┐
│                     DatabaseEngine                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         ModelPersistenceHandler (Protocol)          │   │
│  │  - save(_ model:, transaction:)                     │   │
│  │  - delete(_ model:, transaction:)                   │   │
│  │  - load(_ typeName:, id:, transaction:)             │   │
│  └─────────────────────────────────────────────────────┘   │
│                            ▲                                │
│                            │ conforms to                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │         FDBPersistenceHandler (Implementation)      │   │
│  │  - context: FDBContext                              │   │
│  │  - Uses FDBContext internal methods                 │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    FDBContext                        │   │
│  │  + makePersistenceHandler() -> ModelPersistenceHandler│  │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   RelationshipIndex                         │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              RelationshipMaintainer                  │   │
│  │  - enforceDeleteRules(handler: ModelPersistenceHandler)│ │
│  │  - Protocol にのみ依存、実装に依存しない              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │           FDBContext+Relationship (Extension)        │   │
│  │  - deleteEnforcingRelationshipRules()               │   │
│  │  - Uses context.makePersistenceHandler()            │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## インターフェース定義

### ModelPersistenceHandler Protocol

```swift
// Sources/DatabaseEngine/ModelPersistenceHandler.swift

/// モデル永続化操作の抽象化プロトコル
///
/// 外部モジュール（RelationshipIndex 等）がトランザクション内で
/// モデルの save/delete/load を行うための契約。
public protocol ModelPersistenceHandler: Sendable {
    /// モデルを保存（インデックス更新含む）
    func save(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws

    /// モデルを削除（インデックスクリーンアップ含む）
    func delete(
        _ model: any Persistable,
        transaction: any TransactionProtocol
    ) async throws

    /// 型名と ID でモデルを読み込み
    func load(
        _ typeName: String,
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> (any Persistable)?
}
```

### FDBPersistenceHandler Implementation

```swift
// Sources/DatabaseEngine/FDBPersistenceHandler.swift

/// FDBContext を使用した ModelPersistenceHandler 実装
internal struct FDBPersistenceHandler: ModelPersistenceHandler {
    let context: FDBContext

    func save(_ model: any Persistable, transaction: any TransactionProtocol) async throws {
        let modelType = type(of: model)
        let subspace = try await context.container.resolveDirectory(for: modelType)
        let store = FDBDataStore(
            database: context.container.database,
            subspace: subspace,
            schema: context.container.schema
        )
        let encoder = ProtobufEncoder()
        try await context.saveModel(model, store: store, transaction: transaction, encoder: encoder)
    }

    func delete(_ model: any Persistable, transaction: any TransactionProtocol) async throws {
        let modelType = type(of: model)
        let subspace = try await context.container.resolveDirectory(for: modelType)
        let store = FDBDataStore(
            database: context.container.database,
            subspace: subspace,
            schema: context.container.schema
        )
        try await context.deleteModel(model, store: store, transaction: transaction)
    }

    func load(_ typeName: String, id: Tuple, transaction: any TransactionProtocol) async throws -> (any Persistable)? {
        guard let entity = context.container.schema.entities.first(where: { $0.name == typeName }) else {
            return nil
        }
        let subspace = try await context.container.resolveDirectory(for: entity.persistableType)
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let typeSubspace = itemSubspace.subspace(typeName)
        let key = typeSubspace.pack(id)

        guard let data = try await transaction.getValue(for: key, snapshot: false) else {
            return nil
        }

        let decoder = ProtobufDecoder()
        return try decoder.decode(entity.persistableType, from: Data(data))
    }
}
```

### FDBContext Factory Method

```swift
// Sources/DatabaseEngine/FDBContext.swift に追加

extension FDBContext {
    /// ModelPersistenceHandler を生成
    ///
    /// 外部モジュールがトランザクション内でモデル操作を行うための
    /// ハンドラを提供。
    public func makePersistenceHandler() -> ModelPersistenceHandler {
        FDBPersistenceHandler(context: self)
    }
}
```

## モジュール構成

### Package.swift

```swift
.target(
    name: "RelationshipIndex",
    dependencies: [
        "DatabaseEngine",
        "Core",
        "ScalarIndex",  // FK インデックス用
    ]
),
```

### ファイル構成

```
Sources/
├── DatabaseEngine/
│   ├── ModelPersistenceHandler.swift    # Protocol 定義
│   ├── FDBPersistenceHandler.swift      # 実装
│   └── FDBContext.swift                 # +makePersistenceHandler()
│
└── RelationshipIndex/
    ├── RelationshipMaintainer.swift     # 移動元: DatabaseEngine/Relationship/
    └── FDBContext+Relationship.swift    # 移動元: DatabaseEngine/Relationship/
```

## RelationshipIndex モジュール

### RelationshipMaintainer

```swift
// Sources/RelationshipIndex/RelationshipMaintainer.swift

import DatabaseEngine
import Core
import FoundationDB

/// Relationship の削除ルールを適用
public struct RelationshipMaintainer: Sendable {
    let schema: Schema
    let indexSubspace: Subspace
    let itemSubspace: Subspace

    public init(schema: Schema, indexSubspace: Subspace, itemSubspace: Subspace) {
        self.schema = schema
        self.indexSubspace = indexSubspace
        self.itemSubspace = itemSubspace
    }

    /// 削除ルールを適用
    ///
    /// - Parameters:
    ///   - model: 削除対象のモデル
    ///   - transaction: トランザクション
    ///   - handler: モデル操作ハンドラ（Protocol）
    public func enforceDeleteRules(
        for model: any Persistable,
        transaction: any TransactionProtocol,
        handler: ModelPersistenceHandler
    ) async throws {
        // handler.save(), handler.delete(), handler.load() を使用
        // FDBContext の内部実装に依存しない
    }
}
```

### FDBContext+Relationship Extension

```swift
// Sources/RelationshipIndex/FDBContext+Relationship.swift

import DatabaseEngine
import Core

extension FDBContext {
    /// Relationship ルールを適用して削除
    public func deleteEnforcingRelationshipRules<T: Persistable>(_ model: T) async throws {
        try await container.database.withTransaction { transaction in
            let subspace = try await self.container.resolveDirectory(for: T.self)
            let indexSubspace = subspace.subspace(SubspaceKey.indexes)
            let itemSubspace = subspace.subspace(SubspaceKey.items)

            let maintainer = RelationshipMaintainer(
                schema: self.container.schema,
                indexSubspace: indexSubspace,
                itemSubspace: itemSubspace
            )

            // Handler を生成（Protocol 経由）
            let handler = self.makePersistenceHandler()

            // 削除ルール適用
            try await maintainer.enforceDeleteRules(
                for: model,
                transaction: transaction,
                handler: handler
            )

            // モデル自体を削除
            try await handler.delete(model, transaction: transaction)
        }
    }
}
```

## 依存関係

```
RelationshipIndex
    ↓ depends on
DatabaseEngine (Protocol: ModelPersistenceHandler)
    ↓ depends on
Core, FoundationDB
```

- RelationshipIndex は Protocol にのみ依存
- FDBContext の内部実装に依存しない
- 他の IndexModule と同じパターン

## メリット

1. **モジュール分離**: RDB 機能が不要なユーザーは RelationshipIndex を import しない
2. **クリーンな依存**: Protocol による抽象化で内部実装を隠蔽
3. **拡張性**: 将来、他のモジュールも同じ Pattern を使用可能
4. **テスト容易性**: Mock Handler でテスト可能

## 移行手順

1. `ModelPersistenceHandler` protocol を DatabaseEngine に追加
2. `FDBPersistenceHandler` 実装を DatabaseEngine に追加
3. `FDBContext.makePersistenceHandler()` を追加
4. `RelationshipIndex` モジュールを Package.swift に追加
5. `RelationshipMaintainer` を移動・リファクタリング
6. `FDBContext+Relationship` を移動・リファクタリング
7. `saveModelWithIndexes` / `deleteModelWithIndexes` を削除
8. テスト更新

## 既存コードへの影響

### 削除するコード (FDBContext.swift)
- `saveModelWithIndexes()` - Handler に置き換え
- `deleteModelWithIndexes()` - Handler に置き換え

### 変更なし
- `clearAll()` - テスト用ユーティリティ、維持
- `buildIndexKeys()` - 配列フィールド対応、維持

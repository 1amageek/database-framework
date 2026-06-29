import Foundation
import Core

/// キー変更監視イベント
///
/// FoundationDB固有のWatch機能で検出された変更を表すイベント。
///
/// **Usage**:
/// ```swift
/// for await event in admin.watch(User.self, id: userId) {
///     switch event {
///     case .changed(let user):
///         print("User updated: \(user.name)")
///     case .deleted(let id):
///         print("User deleted: \(id)")
///     case .error(let error):
///         print("Watch error: \(error)")
///     }
/// }
/// ```
public enum WatchEvent<T: Persistable>: Sendable {
    /// ドキュメントが変更された（新規作成または更新）
    case changed(T)

    /// ドキュメントが削除された
    case deleted(T.ID)

    /// 監視中にエラーが発生した
    case error(WatchError)
}

/// Watch機能のエラー
public enum WatchError: Error, Sendable {
    /// トランザクションがキャンセルされた
    case cancelled

    /// タイムアウト
    case timeout

    /// 接続が失われた
    case connectionLost

    /// その他のエラー
    case other(String)
}

extension WatchError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case .cancelled:
            return "Watch was cancelled"
        case .timeout:
            return "Watch timed out"
        case .connectionLost:
            return "Connection to database was lost"
        case .other(let message):
            return message
        }
    }
}

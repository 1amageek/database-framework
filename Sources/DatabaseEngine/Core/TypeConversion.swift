// TypeConversion.swift
// DatabaseEngine - Unified type conversion utilities
//
// This file provides a single entry point for all type conversions
// between Swift native types, FieldValue, and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from Filter.swift,
// AggregationPlanExecutor.swift, AggregationExecution.swift, etc.

import Foundation
import FoundationDB
import Core

/// 統一型変換ユーティリティ
///
/// **必須使用**: 全モジュールはこのユーティリティを使用すること。
/// 独自の型変換実装は禁止。
///
/// ## 型マッピング仕様
///
/// | Swift Type       | Int64 | Double | String | FieldValue      |
/// |------------------|-------|--------|--------|-----------------|
/// | Int, Int8-64     | ✓     | ✓      | -      | .int64          |
/// | UInt, UInt8-64   | ✓*    | ✓      | -      | .int64          |
/// | Double           | -     | ✓      | -      | .double         |
/// | Float            | -     | ✓      | -      | .double         |
/// | String           | -     | -      | ✓      | .string         |
/// | Bool             | ✓**   | -      | -      | .bool           |
/// | Date             | -     | ✓***   | -      | .double***      |
/// | UUID             | -     | -      | ✓****  | .string****     |
///
/// * UInt64 > Int64.max はオーバーフロー（nil を返す）
/// ** Bool → Int64: true=1, false=0
/// *** Date → Double: timeIntervalSince1970
/// **** UUID → String: uuidString
///
public struct TypeConversion: Sendable {

    private init() {}

    // MARK: - 値抽出 (比較・計算用)

    /// Int64 として値を抽出（比較・計算用）
    ///
    /// - 用途: 範囲比較、ソート、集計
    /// - 戻り値: nil = 変換不可（比較スキップ）
    public static func asInt64(_ value: Any) -> Int64? {
        switch value {
        case let v as Int64: return v
        case let v as Int: return Int64(v)
        case let v as Int32: return Int64(v)
        case let v as Int16: return Int64(v)
        case let v as Int8: return Int64(v)
        case let v as UInt: return v <= UInt(Int64.max) ? Int64(v) : nil
        case let v as UInt64: return v <= UInt64(Int64.max) ? Int64(v) : nil
        case let v as UInt32: return Int64(v)
        case let v as UInt16: return Int64(v)
        case let v as UInt8: return Int64(v)
        case let v as Bool: return v ? 1 : 0
        default: return nil
        }
    }

    /// Double として値を抽出（比較・計算用）
    ///
    /// - 用途: 範囲比較、集計 (SUM, AVG)、数値演算
    /// - 戻り値: nil = 変換不可
    public static func asDouble(_ value: Any) -> Double? {
        switch value {
        case let v as Double: return v
        case let v as Float: return Double(v)
        case let v as Int64: return Double(v)
        case let v as Int: return Double(v)
        case let v as Int32: return Double(v)
        case let v as Int16: return Double(v)
        case let v as Int8: return Double(v)
        case let v as UInt64: return Double(v)
        case let v as UInt: return Double(v)
        case let v as UInt32: return Double(v)
        case let v as UInt16: return Double(v)
        case let v as UInt8: return Double(v)
        case let v as Date: return v.timeIntervalSince1970
        default: return nil
        }
    }

    /// String として値を抽出（比較用）
    ///
    /// - 用途: 文字列比較、辞書順ソート
    /// - 戻り値: nil = 変換不可
    public static func asString(_ value: Any) -> String? {
        switch value {
        case let v as String: return v
        case let v as UUID: return v.uuidString
        default: return nil
        }
    }

    // MARK: - 型変換 (ストレージ用)

    /// FieldValue への変換
    ///
    /// - 用途: クエリ実行、統計、HyperLogLog
    /// - 戻り値: 常に FieldValue を返す（未知の型は .string(description)）
    public static func toFieldValue(_ value: Any) -> FieldValue {
        switch value {
        case let v as Bool: return .bool(v)
        case let v as Int: return .int64(Int64(v))
        case let v as Int8: return .int64(Int64(v))
        case let v as Int16: return .int64(Int64(v))
        case let v as Int32: return .int64(Int64(v))
        case let v as Int64: return .int64(v)
        case let v as UInt: return .int64(Int64(v))
        case let v as UInt8: return .int64(Int64(v))
        case let v as UInt16: return .int64(Int64(v))
        case let v as UInt32: return .int64(Int64(v))
        case let v as UInt64: return .int64(Int64(bitPattern: v))
        case let v as Float: return .double(Double(v))
        case let v as Double: return .double(v)
        case let v as String: return .string(v)
        case let v as Data: return .data(v)
        case let v as UUID: return .string(v.uuidString)
        case let v as Date: return .double(v.timeIntervalSince1970)
        case let v as FieldValue: return v
        default:
            return .string(String(describing: value))
        }
    }

    /// TupleElement への変換
    ///
    /// - 用途: FDBインデックスキー構築
    /// - エラー: 変換不可の型は TupleEncodingError をスロー
    public static func toTupleElement(_ value: Any) throws -> any TupleElement {
        return try TupleEncoder.encode(value)
    }

    /// TupleElement への変換（nil許容版）
    public static func toTupleElementOrNil(_ value: Any) -> (any TupleElement)? {
        return TupleEncoder.encodeOrNil(value)
    }

    // MARK: - TupleElement からの抽出

    /// TupleElement から Int64 を抽出
    public static func int64(from element: any TupleElement) throws -> Int64 {
        return try TupleDecoder.decodeInt64(element)
    }

    /// TupleElement から Double を抽出
    public static func double(from element: any TupleElement) throws -> Double {
        return try TupleDecoder.decodeDouble(element)
    }

    /// TupleElement から String を抽出
    public static func string(from element: any TupleElement) throws -> String {
        return try TupleDecoder.decodeString(element)
    }

    /// TupleElement から指定型を抽出
    public static func value<T>(from element: any TupleElement, as type: T.Type) throws -> T {
        return try TupleDecoder.decode(element, as: type)
    }

    /// TupleElement から指定型を抽出（nil許容版）
    public static func valueOrNil<T>(from element: any TupleElement, as type: T.Type) -> T? {
        return TupleDecoder.decodeOrNil(element, as: type)
    }
}

# 回文生成エンジン改修セッション記録

## 発端

論文（鈴木・佐藤 2010「文節結合による回文の自動生成」JSAI 3D4-3）の実装 `script/execute_palindrome.py` にバグがあり、論文で示されている「タヌキ」シードからの回文「夜起きぬ狸おるよ」「軽く起きぬ狸送るか」が見つからないという問題。

## バグの原因

### 1. `CANDIDATE_LIMIT` による候補の取りこぼし（主原因）

`get_candidates()` 内の `query_by_prefix()` が `LIMIT` 付きで候補を取得していた。不足文字列（deficit）が短い場合（例: `R="お"`）、該当する文節数が膨大になる:

- juman DB で「お」始まりの文節: **54,321件**
- 「おるよ」（居るよ）のソート順位置: **50,041番目**
- `LIMIT 2000` では到達不可能

論文のアルゴリズム（§2.2.2）はデータベース D の全文節を候補として探索するが、実装は `LIMIT` で打ち切っていた。

### 2. 辞書カバレッジの問題（副次的）

- 「おくるか」（送るか）と「かるく」（軽く）が juman 辞書ビルドに含まれていなかった
- そのため「軽く起きぬ狸送るか」は辞書の問題で生成不可能

## 解決アプローチ: 事前計算テーブルによる探索効率化

論文 §3 の「先読み」「マクロオペレータ」の考え方を DB テーブルとして実装。

### 新規作成ファイル

#### `database/schema.sql`
全テーブル DDL を一元管理。各スクリプトに散在していた CREATE TABLE/INDEX を集約。

テーブル一覧:
- `bunsetsu` — 文節データベース
- `extend_candidates` — 短い不足文字列（≤2文字）の事前計算遷移
- `reachable` — 不足文字列の到達可能性（最終状態に至る最小ステップ数）
- `palindrome_cache` — 生成された回文のキャッシュ

#### `database/__init__.py`
DDL 操作のユーティリティ:
- `apply_table(conn, table_name, skip_indexes=False)` — schema.sql からテーブルを作成
- `apply_indexes(conn, table_name)` — インデックスのみ作成
- `ensure_table(conn, table_name)` — テーブルが未存在の場合のみ作成

#### `script/build_search_tables.py`
`extend_candidates` と `reachable` テーブルを構築するビルドスクリプト。

処理内容:
1. **extend_candidates 構築**: 各文節について、短い不足文字列（≤2文字）に対する ExtendLeft/ExtendRight case 2 の遷移を列挙（juman DB で約538万行）
2. **reachable 構築**: BFS で到達可能性を計算（Level 1: kana 完全一致、Level 2+: 遷移先の到達可能性から逆算）
3. **到達不能行の削除**: extend_candidates の `reach_steps` を設定し、到達不能行を削除（538万→295万行）

### 変更ファイル

#### `script/execute_palindrome.py`

主な変更:

1. **事前計算テーブルの利用**: 不足文字列が2文字以下のとき、`extend_candidates` テーブルから `reach_steps` でフィルタして候補取得。`ORDER BY reach_steps LIMIT` で到達が近い候補を優先。

2. **反復深化**: BFS を深さ別に分離。depth=2, 3, 4... と段階的に探索し、各段階で `reach_steps` フィルタが最大限に効くようにした。深い探索では完全一致シードのみ使用。

3. **結果キャッシュ**: `palindrome_cache` テーブルに探索結果を全件保存。2回目以降は DB から直接読み込み（初回 ~5秒 → 2回目 0.09秒）。

4. **除外ワード**: `EXCLUDE_WORDS` 配列で display に特定の語を含む結果を除外。

5. **文節数制御**: `MIN_BUNSETSU` / `MAX_BUNSETSU` 変数で探索する文節数の範囲を制御。

6. **進捗ログ**: `search_at_depth` 内で5秒ごとに states数、キュー残量、候補取得時間、extend演算時間を出力。

#### `script/juman/build_db.py`, `script/bccwj/build_db.py`

- インライン DDL を `database.apply_table()` に置き換え
- `import sys` を追加（`sys.path.insert` のため）

## パフォーマンス

juman DB（134万文節）、シード「たぬき」での計測:

| 条件 | 時間 | 結果数 |
|---|---|---|
| 変更前（LIMIT 2000） | ~19秒 | 目標回文なし |
| 事前計算テーブル + depth=4 + キャッシュなし | ~5秒 | 29,269件（目標含む） |
| キャッシュヒット時 | 0.09秒 | 29,269件 |

### depth と探索コストの関係

| depth | 状態数 | 時間 | 備考 |
|---|---|---|---|
| 2 | 少数 | ~0秒 | |
| 3 | 少数 | ~0.2秒 | |
| 4 | ~90万 | ~5秒 | 実用限界 |
| 5 | ~5,500万 | ~840秒 | extend演算が支配的 |

depth=5 以上は状態数が指数的に増加し実用的でない（論文でも4文節までの実験）。

### `reach_steps` フィルタの効果（deficit="お", side="R"）

| フィルタ条件 | 候補数 |
|---|---|
| フィルタなし | 54,321 |
| 到達不能削除後 | 25,839 |
| reach_steps ≤ 1 | 1,633 |
| reach_steps ≤ 2 | 9,674 |

## 残課題

- depth=5 以上の高速化（論文のマクロオペレータの完全実装が必要）
- juman 辞書の活用形カバレッジ改善（「送るか」「軽く」等の欠落）
- `docs/build_db.py`, `script/mecab/build_db.py` の DDL 集約（未対応）

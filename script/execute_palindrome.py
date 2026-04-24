"""
回文検索 CLI
bunsetsu.db + palindrome_engine を使ってキーワードから回文を探す
"""

import argparse
import sqlite3
import sys
import re
import time
from pathlib import Path
from collections import deque

from palindrome_engine import (
    generate_initial_states, extend_left, extend_right,
)

DEFAULT_DB_PATH = Path(__file__).parent / "bunsetsu.db"
# 200 -> 18.56s, 500 -> 19.06s, 1000-> 19.04s
MAX_SEEDS = 200

# 表示文字列に日本語（ひらがな・カタカナ・漢字・長音符）以外が含まれるものを除外
_JP_RE = re.compile(r'^[ぁ-んァ-ヶー\u4E00-\u9FFF々〇]+$')
# 5000 -> 53s, 2000 -> 18.56s
CANDIDATE_LIMIT = 1000
MIN_BUNSETSU = 2
MAX_BUNSETSU = 4
MAX_RESULTS = 5000
DEFICIT_THRESHOLD = 2  # この長さ以下の不足文字列は事前計算テーブルを使う

# display にこれらの語を含む結果を除外する
EXCLUDE_WORDS = ["悔過"]


def prefix_range(prefix):
    """前方一致を範囲クエリに変換（LIKE はインデックスが効かないため）"""
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    return prefix, upper


def query_by_prefix(cur, column, prefix, limit=CANDIDATE_LIMIT):
    """前方一致をインデックスが効く範囲クエリで実行"""
    lo, hi = prefix_range(prefix)
    return cur.execute(
        f"SELECT kana, display FROM bunsetsu WHERE {column} >= ? AND {column} < ? LIMIT ?",
        (lo, hi, limit),
    ).fetchall()


def _has_search_tables(cur):
    """extend_candidates / reachable テーブルが存在するか"""
    row = cur.execute(
        "SELECT COUNT(*) FROM sqlite_master "
        "WHERE type='table' AND name IN ('extend_candidates','reachable')"
    ).fetchone()
    return row[0] == 2


_HIRAGANA_RE = re.compile(r'^[ぁ-んー]+$')

def find_seeds(cur, keyword):
    """キーワードに一致・前方一致する文節をDBから取得
    ひらがな入力 → kana列で検索、それ以外 → display列で検索
    """
    if _HIRAGANA_RE.match(keyword):
        return query_by_prefix(cur, "kana", keyword, limit=MAX_SEEDS)
    else:
        return query_by_prefix(cur, "display", keyword, limit=MAX_SEEDS)


def get_candidates(cur, state, use_tables=False, remaining_steps=99):
    """状態の L / R に基づいて、拡張可能な文節候補をDBから取得"""
    candidates = []

    if state.L:
        L = state.L
        ll = len(L)
        # case 1: word が L の末尾と一致するケース（word = L の接尾辞）
        for length in range(1, ll + 1):
            suffix = L[ll - length:]
            rows = cur.execute(
                "SELECT kana, display FROM bunsetsu WHERE kana = ?", (suffix,)
            ).fetchall()
            candidates.extend(rows)
        # case 2: L が word の末尾と一致するケース
        if use_tables and ll <= DEFICIT_THRESHOLD:
            rows = cur.execute(
                "SELECT DISTINCT w_kana, w_display "
                "FROM extend_candidates "
                "WHERE deficit = ? AND side = 'L' AND reach_steps <= ? "
                "ORDER BY reach_steps LIMIT ?",
                (L, remaining_steps - 1, CANDIDATE_LIMIT),
            ).fetchall()
            candidates.extend(rows)
        else:
            rev_L = L[::-1]
            candidates.extend(query_by_prefix(cur, "kana_rev", rev_L))

    if state.R:
        R = state.R
        rl = len(R)
        # case 1: word が R の先頭と一致するケース（word = R の接頭辞）
        for length in range(1, rl + 1):
            prefix = R[:length]
            rows = cur.execute(
                "SELECT kana, display FROM bunsetsu WHERE kana = ?", (prefix,)
            ).fetchall()
            candidates.extend(rows)
        # case 2: R が word の先頭と一致するケース
        if use_tables and rl <= DEFICIT_THRESHOLD:
            rows = cur.execute(
                "SELECT DISTINCT w_kana, w_display "
                "FROM extend_candidates "
                "WHERE deficit = ? AND side = 'R' AND reach_steps <= ? "
                "ORDER BY reach_steps LIMIT ?",
                (R, remaining_steps - 1, CANDIDATE_LIMIT),
            ).fetchall()
            candidates.extend(rows)
        else:
            candidates.extend(query_by_prefix(cur, "kana", R))

    return candidates


def search_at_depth(cur, seed_kana, seed_display, target_depth,
                    use_tables=False):
    """指定した文節数で回文を探索（反復深化の1段階分）"""
    results = []
    queue = deque(generate_initial_states(seed_kana, seed_display))
    visited = set()
    n_states = 0
    n_candidates = 0
    t_candidates = 0.0
    t_extend = 0.0
    t_start = time.perf_counter()
    LOG_INTERVAL = 5.0  # 秒ごとにログ出力
    next_log = t_start + LOG_INTERVAL

    while queue:
        state = queue.popleft()
        key = (state.L, state.H, state.R)
        if key in visited:
            continue
        visited.add(key)
        n_states += 1

        if state.is_palindrome_state():
            if state.bunsetsu_count >= MIN_BUNSETSU:
                results.append(state)
            continue

        if state.bunsetsu_count >= target_depth:
            continue

        tc0 = time.perf_counter()
        remaining = target_depth - state.bunsetsu_count
        cands = get_candidates(cur, state, use_tables=use_tables,
                               remaining_steps=remaining)
        tc1 = time.perf_counter()
        t_candidates += tc1 - tc0
        n_candidates += len(cands)

        te0 = time.perf_counter()
        for w_kana, w_display in cands:
            if state.L:
                ns = extend_left(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited:
                    queue.append(ns)
            if state.R:
                ns = extend_right(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited:
                    queue.append(ns)
        t_extend += time.perf_counter() - te0

        now = time.perf_counter()
        if now >= next_log:
            elapsed = now - t_start
            print(f"    [depth={target_depth} seed={seed_display} {elapsed:.1f}s] "
                  f"states={n_states:,} queue={len(queue):,} results={len(results):,} "
                  f"candidates={n_candidates:,} "
                  f"t_cand={t_candidates:.1f}s t_ext={t_extend:.1f}s",
                  flush=True)
            next_log = now + LOG_INTERVAL

    elapsed = time.perf_counter() - t_start
    if elapsed > 1.0:
        print(f"    [depth={target_depth} seed={seed_display} done {elapsed:.1f}s] "
              f"states={n_states:,} results={len(results):,} "
              f"candidates={n_candidates:,} "
              f"t_cand={t_candidates:.1f}s t_ext={t_extend:.1f}s",
              flush=True)

    return results


def main():
    parser = argparse.ArgumentParser(description="回文検索エンジン")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                        help=f"bunsetsu.db のパス（デフォルト: {DEFAULT_DB_PATH}）")
    args = parser.parse_args()
    db_path = args.db

    if not db_path.exists():
        print(f"エラー: {db_path} が見つかりません。先に build_db.py を実行してください。")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    use_tables = _has_search_tables(cur)
    if use_tables:
        print("  (事前計算テーブル検出 — 高速モード)")
    else:
        print("  (事前計算テーブルなし — build_search_tables.py で構築可能)")

    print("=" * 50)
    print("  回文検索エンジン")
    print("  Ctrl+C または空入力で終了")
    print("=" * 50)

    try:
        while True:
            print()
            keyword = input("キーワードを入力してください > ").strip()
            if not keyword:
                break

            seeds = find_seeds(cur, keyword)
            if not seeds:
                print("  該当する語彙が見つかりませんでした。")
                continue

            print(f"  {len(seeds)} 件の語彙にマッチ。回文を探索中...")

            t0 = time.perf_counter()
            all_results = []
            seen_h = set()
            # 完全一致シードと前方一致シードを分ける
            exact_seeds = [(k, d) for k, d in seeds if k == keyword or d == keyword]
            if not exact_seeds:
                exact_seeds = seeds[:1]  # 最低1つ
            for depth in range(MIN_BUNSETSU, MAX_BUNSETSU + 1):
                # 深い探索では完全一致シードのみ使う（高速化）
                depth_seeds = seeds if depth <= 3 else exact_seeds
                dt0 = time.perf_counter()
                print(f"  -- depth={depth} seeds={len(depth_seeds)} 開始", flush=True)
                depth_results = []
                for seed_kana, seed_display in depth_seeds:
                    for state in search_at_depth(cur, seed_kana, seed_display,
                                                 target_depth=depth,
                                                 use_tables=use_tables):
                        if state.H not in seen_h and _JP_RE.match(state.display) \
                                and not any(w in state.display for w in EXCLUDE_WORDS):
                            seen_h.add(state.H)
                            depth_results.append(state)
                dt_elapsed = time.perf_counter() - dt0
                print(f"  -- depth={depth} 完了: {len(depth_results):,} 件 ({dt_elapsed:.2f}秒)",
                      flush=True)
                all_results.extend(depth_results)
            elapsed = time.perf_counter() - t0

            if not all_results:
                print(f"  回文が見つかりませんでした。（{elapsed:.2f}秒）")
            else:
                # 読みが長い順（長い回文優先）
                all_results.sort(key=lambda s: -len(s.H))
                shown = all_results[:MAX_RESULTS]
                print(f"\n  {len(all_results)} 件中 上位 {len(shown)} 件を表示（{elapsed:.2f}秒）:\n")
                for i, state in enumerate(shown, 1):
                    print(f"    {i:2d}. [{state.bunsetsu_count}文節] {state.display}（{state.H}）")

    except (KeyboardInterrupt, EOFError):
        print()

    conn.close()


if __name__ == "__main__":
    main()

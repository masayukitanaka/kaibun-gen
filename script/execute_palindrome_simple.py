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
MAX_SEEDS = 500

# 表示文字列に日本語（ひらがな・カタカナ・漢字・長音符）以外が含まれるものを除外
_JP_RE = re.compile(r'^[ぁ-んァ-ヶー\u4E00-\u9FFF々〇]+$')
# 5000 -> 53s, 2000 -> 18.56s
CANDIDATE_LIMIT = 2000
MAX_BUNSETSU = 8
MAX_RESULTS = 50


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


_HIRAGANA_RE = re.compile(r'^[ぁ-んー]+$')

def find_seeds(cur, keyword):
    """キーワードに一致・前方一致する文節をDBから取得
    ひらがな入力 → kana列で検索、それ以外 → display列で検索
    """
    if _HIRAGANA_RE.match(keyword):
        return query_by_prefix(cur, "kana", keyword, limit=MAX_SEEDS)
    else:
        return query_by_prefix(cur, "display", keyword, limit=MAX_SEEDS)


def get_candidates(cur, state):
    """状態の L / R に基づいて、拡張可能な文節候補をDBから取得"""
    candidates = []

    if state.L:
        L = state.L
        ll = len(L)
        # word が L の末尾と一致するケース（word = L の接尾辞）
        for length in range(1, ll + 1):
            suffix = L[ll - length:]
            rows = cur.execute(
                "SELECT kana, display FROM bunsetsu WHERE kana = ?", (suffix,)
            ).fetchall()
            candidates.extend(rows)
        # L が word の末尾と一致するケース（kana_rev が reverse(L) で始まる）
        rev_L = L[::-1]
        candidates.extend(query_by_prefix(cur, "kana_rev", rev_L))

    if state.R:
        R = state.R
        rl = len(R)
        # word が R の先頭と一致するケース（word = R の接頭辞）
        for length in range(1, rl + 1):
            prefix = R[:length]
            rows = cur.execute(
                "SELECT kana, display FROM bunsetsu WHERE kana = ?", (prefix,)
            ).fetchall()
            candidates.extend(rows)
        # R が word の先頭と一致するケース（kana が R で始まる）
        candidates.extend(query_by_prefix(cur, "kana", R))

    return candidates


def search(cur, seed_kana, seed_display, max_bunsetsu=MAX_BUNSETSU, max_results=MAX_RESULTS):
    """BFS で回文を探索（DB から候補を都度取得）"""
    results = []
    queue = deque(generate_initial_states(seed_kana, seed_display))
    visited = set()

    while queue and len(results) < max_results:
        state = queue.popleft()
        key = (state.L, state.H, state.R)
        if key in visited:
            continue
        visited.add(key)

        if state.is_palindrome_state():
            if state.bunsetsu_count >= 2 and state.is_valid_palindrome():
                results.append(state)
            continue

        if state.bunsetsu_count >= max_bunsetsu:
            continue

        for w_kana, w_display in get_candidates(cur, state):
            if state.L:
                ns = extend_left(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited:
                    queue.append(ns)
            if state.R:
                ns = extend_right(state, w_kana, w_display)
                if ns and (ns.L, ns.H, ns.R) not in visited:
                    queue.append(ns)

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
            for seed_kana, seed_display in seeds:
                for state in search(cur, seed_kana, seed_display):
                    if state.H not in seen_h and _JP_RE.match(state.display):
                        seen_h.add(state.H)
                        all_results.append(state)
                if len(all_results) >= MAX_RESULTS:
                    break
            elapsed = time.perf_counter() - t0

            if not all_results:
                print(f"  回文が見つかりませんでした。（{elapsed:.2f}秒）")
            else:
                print(f"\n  {len(all_results)} 件の回文が見つかりました（{elapsed:.2f}秒）:\n")
                for i, state in enumerate(all_results, 1):
                    print(f"    {i:2d}. {state.display}（{state.H}）")

    except (KeyboardInterrupt, EOFError):
        print()

    conn.close()


if __name__ == "__main__":
    main()

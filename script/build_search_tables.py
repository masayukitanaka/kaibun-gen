"""
extend_candidates / reachable テーブルを構築する。
bunsetsu.db に対して1回実行すれば、以後の探索が高速化される。

Usage:
    python build_search_tables.py --db juman/bunsetsu.db
"""

import argparse
import sqlite3
import sys
import time
from pathlib import Path

THRESHOLD = 2       # 事前計算する不足文字列の最大長
MAX_REACH_LEVEL = 6 # reachable テーブルの最大探索深さ


# ── helpers ──────────────────────────────────────────────

def prefix_range(prefix):
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
    return prefix, upper


# ── extend_candidates 構築 ───────────────────────────────

def build_extend_candidates(cur):
    """case 2 の遷移を全文節から列挙し extend_candidates に格納する"""
    cur.execute("DROP TABLE IF EXISTS extend_candidates")
    cur.execute("""
        CREATE TABLE extend_candidates (
            deficit     TEXT NOT NULL,
            side        TEXT NOT NULL,
            w_kana      TEXT NOT NULL,
            w_display   TEXT NOT NULL,
            new_deficit TEXT NOT NULL,
            new_side    TEXT NOT NULL
        )
    """)

    all_bunsetsu = cur.execute("SELECT kana, display FROM bunsetsu").fetchall()
    total = len(all_bunsetsu)
    batch = []
    for i, (kana, display) in enumerate(all_bunsetsu):
        n = len(kana)
        for k in range(1, min(n + 1, THRESHOLD + 1)):
            # ExtendRight case 2: deficit R = kana[:k]
            deficit = kana[:k]
            remaining = kana[k:]
            new_def = remaining[::-1] if remaining else ""
            new_s = "L" if new_def else ""
            batch.append((deficit, "R", kana, display, new_def, new_s))

            # ExtendLeft case 2: deficit L = kana[-k:]
            deficit = kana[-k:]
            remaining = kana[:-k] if k < n else ""
            new_def = remaining[::-1] if remaining else ""
            new_s = "R" if new_def else ""
            batch.append((deficit, "L", kana, display, new_def, new_s))

        if len(batch) >= 80_000:
            cur.executemany(
                "INSERT INTO extend_candidates VALUES(?,?,?,?,?,?)", batch
            )
            batch.clear()

        if (i + 1) % 200_000 == 0:
            print(f"    {i+1}/{total} ...", flush=True)

    if batch:
        cur.executemany(
            "INSERT INTO extend_candidates VALUES(?,?,?,?,?,?)", batch
        )

    cur.execute(
        "CREATE INDEX idx_ec_lookup ON extend_candidates(deficit, side)"
    )
    cur.execute(
        "CREATE INDEX idx_ec_newdef ON extend_candidates(new_deficit, new_side)"
    )

    cnt = cur.execute("SELECT COUNT(*) FROM extend_candidates").fetchone()[0]
    print(f"  extend_candidates: {cnt:,} 行")


# ── reachable 構築 ───────────────────────────────────────

def build_reachable(cur):
    """不足文字列の到達可能性を BFS で計算し reachable に格納する"""
    cur.execute("DROP TABLE IF EXISTS reachable")
    cur.execute("""
        CREATE TABLE reachable (
            deficit   TEXT NOT NULL,
            side      TEXT NOT NULL,
            min_steps INTEGER NOT NULL,
            PRIMARY KEY (deficit, side)
        )
    """)

    # ── 到達可能な不足文字列を収集していく ──
    reachable: dict[tuple[str, str], int] = {}

    # Level 1: DB に kana として存在する文字列 → 1ステップで解消可能
    kana_set: set[str] = set()
    for (kana,) in cur.execute("SELECT DISTINCT kana FROM bunsetsu"):
        kana_set.add(kana)
        reachable[(kana, "L")] = 1
        reachable[(kana, "R")] = 1

    print(f"  reachable level 1: {len(reachable):,}")

    # Level 2+: extend_candidates を使って拡張
    #   (deficit, side) → (new_deficit, new_side) で
    #   new_deficit が既に到達可能なら deficit も到達可能
    ec_transitions = cur.execute(
        "SELECT DISTINCT deficit, side, new_deficit, new_side "
        "FROM extend_candidates"
    ).fetchall()

    for level in range(2, MAX_REACH_LEVEL + 1):
        added = 0
        for deficit, side, new_deficit, new_side in ec_transitions:
            key = (deficit, side)
            if key in reachable:
                continue
            if not new_deficit:      # new_deficit="" → level 1 で登録済みのはず
                continue
            target = (new_deficit, new_side)
            if target in reachable and reachable[target] < level:
                reachable[key] = level
                added += 1

        # case 1 遷移: 短い不足文字列を kana で部分消化
        #   L 側: deficit の末尾が kana → 残りが到達可能なら OK
        #   R 側: deficit の先頭が kana → 残りが到達可能なら OK
        all_deficits = {
            (d, s)
            for d, s, _, _ in ec_transitions
            if (d, s) not in reachable
        }
        for deficit, side in all_deficits:
            if (deficit, side) in reachable:
                continue
            dl = len(deficit)
            if side == "L":
                for k in range(1, dl):
                    suffix = deficit[dl - k :]
                    if suffix in kana_set:
                        rem = deficit[: dl - k]
                        if not rem or (
                            (rem, "L") in reachable
                            and reachable[(rem, "L")] < level
                        ):
                            reachable[(deficit, side)] = level
                            added += 1
                            break
            else:
                for k in range(1, dl):
                    prefix = deficit[:k]
                    if prefix in kana_set:
                        rem = deficit[k:]
                        if not rem or (
                            (rem, "R") in reachable
                            and reachable[(rem, "R")] < level
                        ):
                            reachable[(deficit, side)] = level
                            added += 1
                            break

        print(f"  reachable level {level}: +{added:,} (計 {len(reachable):,})")
        if added == 0:
            break

    # ── extend_candidates.new_deficit のうち長いものの到達可能性 ──
    long_new = set()
    for (nd, ns) in cur.execute(
        "SELECT DISTINCT new_deficit, new_side FROM extend_candidates "
        "WHERE new_deficit != ''"
    ):
        if (nd, ns) not in reachable:
            long_new.add((nd, ns))

    print(f"  長い new_deficit の到達可能性チェック: {len(long_new):,} 件")

    resolved = 0
    for deficit, side in long_new:
        if deficit in kana_set:
            reachable[(deficit, side)] = 1
            resolved += 1
            continue
        dl = len(deficit)
        # case 1 分解
        found = False
        if side == "L":
            for k in range(1, dl):
                suffix = deficit[dl - k :]
                if suffix in kana_set:
                    rem = deficit[: dl - k]
                    if not rem or rem in kana_set or (rem, "L") in reachable:
                        steps = 1 + (reachable.get((rem, "L"), 0) if rem else 0)
                        reachable[(deficit, side)] = steps
                        found = True
                        resolved += 1
                        break
        else:
            for k in range(1, dl):
                prefix = deficit[:k]
                if prefix in kana_set:
                    rem = deficit[k:]
                    if not rem or rem in kana_set or (rem, "R") in reachable:
                        steps = 1 + (reachable.get((rem, "R"), 0) if rem else 0)
                        reachable[(deficit, side)] = steps
                        found = True
                        resolved += 1
                        break

        if not found:
            # case 2: DB で接頭辞/接尾辞検索して1件でも遷移可能か確認
            if side == "L":
                rev = deficit[::-1]
                lo, hi = prefix_range(rev)
                row = cur.execute(
                    "SELECT kana FROM bunsetsu "
                    "WHERE kana_rev >= ? AND kana_rev < ? LIMIT 1",
                    (lo, hi),
                ).fetchone()
            else:
                lo, hi = prefix_range(deficit)
                row = cur.execute(
                    "SELECT kana FROM bunsetsu "
                    "WHERE kana >= ? AND kana < ? LIMIT 1",
                    (lo, hi),
                ).fetchone()
            if row:
                # 遷移先が存在 → 到達可能とみなす (保守的に大きめの step)
                reachable[(deficit, side)] = MAX_REACH_LEVEL
                resolved += 1

    print(f"  長い new_deficit 解決: {resolved:,} / {len(long_new):,}")

    # ── DB に書き込み ──
    batch = [(d, s, steps) for (d, s), steps in reachable.items()]
    cur.executemany("INSERT OR IGNORE INTO reachable VALUES(?,?,?)", batch)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_reach ON reachable(deficit, side)")

    cnt = cur.execute("SELECT COUNT(*) FROM reachable").fetchone()[0]
    print(f"  reachable: {cnt:,} 行")


# ── メイン ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="回文探索用の事前計算テーブルを構築する"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).parent / "bunsetsu.db",
        help="bunsetsu.db のパス",
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"エラー: {args.db} が見つかりません。")
        sys.exit(1)

    conn = sqlite3.connect(str(args.db))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cur = conn.cursor()

    t0 = time.perf_counter()

    print("1/3  extend_candidates を構築中...")
    build_extend_candidates(cur)
    conn.commit()

    print("2/3  reachable を構築中...")
    build_reachable(cur)
    conn.commit()

    print("3/3  extend_candidates に reach_steps を付与し到達不能行を削除中...")
    before = cur.execute("SELECT COUNT(*) FROM extend_candidates").fetchone()[0]
    # reach_steps 列を追加: new_deficit の到達に必要な最小ステップ数
    cur.execute("ALTER TABLE extend_candidates ADD COLUMN reach_steps INTEGER")
    cur.execute("UPDATE extend_candidates SET reach_steps = 0 WHERE new_deficit = ''")
    cur.execute(
        "UPDATE extend_candidates SET reach_steps = ("
        "  SELECT r.min_steps FROM reachable r "
        "  WHERE r.deficit = extend_candidates.new_deficit "
        "    AND r.side = extend_candidates.new_side"
        ") WHERE new_deficit != ''"
    )
    # reach_steps IS NULL → 到達不能 → 削除
    cur.execute("DELETE FROM extend_candidates WHERE reach_steps IS NULL")
    after = cur.execute("SELECT COUNT(*) FROM extend_candidates").fetchone()[0]
    print(f"  {before:,} → {after:,} 行 ({before - after:,} 行削除)")
    # reach_steps でフィルタできるよう複合インデックスを再構築
    cur.execute("DROP INDEX IF EXISTS idx_ec_lookup")
    cur.execute(
        "CREATE INDEX idx_ec_lookup ON extend_candidates(deficit, side, reach_steps)"
    )
    conn.commit()
    conn.execute("VACUUM")

    elapsed = time.perf_counter() - t0
    print(f"\n完了 ({elapsed:.1f}秒)")
    conn.close()


if __name__ == "__main__":
    main()

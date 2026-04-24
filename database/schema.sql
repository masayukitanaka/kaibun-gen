-- ============================================================
-- kaibun-gen データベーススキーマ
-- 全テーブル定義をここに集約する。
-- 各スクリプトは database.apply_schema() 経由でこれを利用する。
-- ============================================================

-- ── bunsetsu: 文節データベース ──────────────────────────────

DROP TABLE IF EXISTS bunsetsu;
CREATE TABLE bunsetsu (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    kana     TEXT NOT NULL,
    kana_rev TEXT NOT NULL,
    display  TEXT NOT NULL,
    type     TEXT,
    freq     INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_kana     ON bunsetsu(kana);
CREATE INDEX IF NOT EXISTS idx_kana_rev ON bunsetsu(kana_rev);
CREATE INDEX IF NOT EXISTS idx_display  ON bunsetsu(display);


-- ── extend_candidates: 短い不足文字列の事前計算遷移 ─────────

DROP TABLE IF EXISTS extend_candidates;
CREATE TABLE extend_candidates (
    deficit     TEXT NOT NULL,
    side        TEXT NOT NULL,
    w_kana      TEXT NOT NULL,
    w_display   TEXT NOT NULL,
    new_deficit TEXT NOT NULL,
    new_side    TEXT NOT NULL,
    reach_steps INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ec_lookup ON extend_candidates(deficit, side, reach_steps);
CREATE INDEX IF NOT EXISTS idx_ec_newdef ON extend_candidates(new_deficit, new_side);


-- ── reachable: 不足文字列の到達可能性 ──────────────────────

DROP TABLE IF EXISTS reachable;
CREATE TABLE reachable (
    deficit   TEXT NOT NULL,
    side      TEXT NOT NULL,
    min_steps INTEGER NOT NULL,
    PRIMARY KEY (deficit, side)
);
CREATE INDEX IF NOT EXISTS idx_reach ON reachable(deficit, side);


-- ── palindrome_cache: 生成された回文のキャッシュ ────────────

DROP TABLE IF EXISTS palindrome_cache;
CREATE TABLE palindrome_cache (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    seed_kana      TEXT NOT NULL,
    seed_display   TEXT NOT NULL,
    target_depth   INTEGER NOT NULL,
    kana           TEXT NOT NULL,
    display        TEXT NOT NULL,
    bunsetsu_count INTEGER NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(seed_kana, target_depth, kana)
);
CREATE INDEX IF NOT EXISTS idx_pc_seed ON palindrome_cache(seed_kana, target_depth);

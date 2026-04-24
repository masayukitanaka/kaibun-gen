"""
文節データベース構築スクリプト（BCCWJ版）
BCCWJ_frequencylist_luw2_ver1_0.tsv から語彙を読み取り
文節を生成して SQLite に格納する

テーブル設計:
  bunsetsu(id, kana, kana_rev, display, type, freq)
  INDEX ON kana, kana_rev, display
"""

import csv
import sqlite3
import sys
import re
from pathlib import Path

# ─── wType ごとの最低出現回数（これ未満は除外）──────────────
# 必要に応じて調整してください
MIN_FREQ = {
    "和":   1,     # 和語: 全て採用
    "漢":   1,     # 漢語: 全て採用
    "外":   5,     # 外来語: 5回以上
    "混":   5,     # 混種語: 5回以上
    "固":   5,     # 固有名詞的: 5回以上
    "記号": 0,     # 記号: 除外
    "不明": 0,     # 不明: 除外
    "※":   0,     # 除外
}

TSV_FILE = "BCCWJ_frequencylist_luw2_ver1_0.tsv"


def kata2hira(s: str) -> str:
    return "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ン" else c for c in s)


# ─── 動詞: 辞書形末尾から活用型を推定 ─────────────────────
def guess_conj_type(reading, lemma):
    """辞書形（読み・表記）から活用型を推定"""
    if lemma == "する" or reading.endswith("する"):
        return "サ行変格活用"
    if lemma == "来る" or reading == "くる":
        return "カ行変格活用"
    if reading.endswith("る"):
        # 一段 vs 五段・ラ行 の判定
        # 語幹末尾がイ段・エ段なら一段の可能性が高い
        if len(reading) >= 2 and reading[-2] in "いきしちにひみりぎじびぴえけせてねへめれげぜべぺ":
            return "一段"
        return "五段・ラ行"
    if reading.endswith("う"):
        return "五段・ワ行ウ音便"
    if reading.endswith("く"):
        # 「行く」は特殊（イ音便）
        if lemma == "行く":
            return "五段・カ行イ音便"
        return "五段・カ行イ音便"
    if reading.endswith("ぐ"):
        return "五段・ガ行"
    if reading.endswith("す"):
        return "五段・サ行"
    if reading.endswith("つ"):
        return "五段・タ行"
    if reading.endswith("ぬ"):
        return "五段・ナ行"
    if reading.endswith("ぶ"):
        return "五段・バ行"
    if reading.endswith("む"):
        return "五段・マ行"
    return None


# ─── 動詞: 活用形ごとの (語尾, 表記, 後続可能助詞リスト) ──────────
VERB_FORMS = {
    "五段・カ行イ音便": [
        ("く",   "く",   [("よ","よ"),("な","な"),("か","か")]),
        ("かない","かない",[]),
        ("き",   "き",   [("たい","たい"),("ながら","ながら")]),
        ("いた", "いた", [("",""),("よ","よ"),("ら","ら"),("り","り"),("の","の")]),
        ("いて", "いて", [("",""),("も","も"),("は","は")]),
        ("いたい","いたい",[("",""),("よ","よ")]),
    ],
    "五段・ガ行": [
        ("ぐ",   "ぐ",   [("よ","よ"),("な","な"),("か","か")]),
        ("がない","がない",[]),
        ("ぎ",   "ぎ",   [("たい","たい"),("ながら","ながら")]),
        ("いだ", "いだ", [("",""),("よ","よ"),("の","の")]),
        ("いで", "いで", [("",""),("も","も")]),
    ],
    "五段・サ行": [
        ("す",   "す",   [("よ","よ"),("な","な"),("か","か")]),
        ("さない","さない",[]),
        ("し",   "し",   [("たい","たい"),("ながら","ながら")]),
        ("した", "した", [("",""),("よ","よ"),("の","の"),("ら","ら"),("り","り")]),
        ("して", "して", [("",""),("も","も"),("は","は")]),
        ("したい","したい",[]),
    ],
    "五段・タ行": [
        ("つ",   "つ",   [("よ","よ"),("な","な"),("か","か")]),
        ("たない","たない",[]),
        ("ち",   "ち",   [("たい","たい"),("ながら","ながら")]),
        ("った", "った", [("",""),("よ","よ"),("の","の"),("ら","ら"),("り","り")]),
        ("って", "って", [("",""),("も","も"),("は","は")]),
    ],
    "五段・ナ行": [
        ("ぬ",   "ぬ",   [("よ","よ"),("か","か")]),
        ("なない","なない",[]),
        ("に",   "に",   [("たい","たい"),("ながら","ながら")]),
        ("んだ", "んだ", [("",""),("よ","よ"),("の","の")]),
        ("んで", "んで", [("",""),("も","も")]),
    ],
    "五段・バ行": [
        ("ぶ",   "ぶ",   [("よ","よ"),("な","な"),("か","か")]),
        ("ばない","ばない",[]),
        ("び",   "び",   [("たい","たい"),("ながら","ながら")]),
        ("んだ", "んだ", [("",""),("よ","よ"),("の","の")]),
        ("んで", "んで", [("",""),("も","も")]),
    ],
    "五段・マ行": [
        ("む",   "む",   [("よ","よ"),("な","な"),("か","か")]),
        ("まない","まない",[]),
        ("み",   "み",   [("たい","たい"),("ながら","ながら")]),
        ("んだ", "んだ", [("",""),("よ","よ"),("の","の")]),
        ("んで", "んで", [("",""),("も","も")]),
    ],
    "五段・ラ行": [
        ("る",   "る",   [("よ","よ"),("な","な"),("か","か"),("の","の")]),
        ("らない","らない",[]),
        ("り",   "り",   [("たい","たい"),("ながら","ながら"),("に","に")]),
        ("った", "った", [("",""),("よ","よ"),("の","の"),("ら","ら"),("り","り")]),
        ("って", "って", [("",""),("も","も"),("は","は")]),
        ("るよ", "るよ", []),
        ("るか", "るか", []),
        ("るな", "るな", []),
    ],
    "五段・ワ行ウ音便": [
        ("う",   "う",   [("よ","よ"),("か","か"),("な","な")]),
        ("わない","わない",[]),
        ("い",   "い",   [("たい","たい"),("ながら","ながら")]),
        ("った", "った", [("",""),("よ","よ"),("の","の")]),
        ("って", "って", [("",""),("も","も")]),
    ],
    "一段": [
        ("る",   "る",   [("よ","よ"),("な","な"),("か","か"),("の","の")]),
        ("ない", "ない", []),
        ("",     "",     [("たい","たい"),("ながら","ながら")]),
        ("た",   "た",   [("",""),("よ","よ"),("の","の"),("ら","ら"),("り","り")]),
        ("て",   "て",   [("",""),("も","も"),("は","は")]),
        ("るよ", "るよ", []),
        ("るか", "るか", []),
        ("ぬ",   "ぬ",   []),
    ],
    "カ行変格活用": [
        ("くる", "くる", [("よ","よ"),("な","な"),("か","か")]),
        ("こない","こない",[]),
        ("き",   "き",   [("たい","たい"),("ながら","ながら")]),
        ("きた", "きた", [("",""),("よ","よ"),("の","の")]),
        ("きて", "きて", [("",""),("も","も")]),
    ],
    "サ行変格活用": [
        ("する", "する", [("よ","よ"),("な","な"),("か","か"),("の","の")]),
        ("しない","しない",[]),
        ("し",   "し",   [("たい","たい"),("ながら","ながら")]),
        ("した", "した", [("",""),("よ","よ"),("の","の"),("ら","ら"),("り","り")]),
        ("して", "して", [("",""),("も","も"),("は","は")]),
        ("するよ","するよ",[]),
        ("するか","するか",[]),
    ],
}

DICT_FORM_ENDINGS = {
    "五段・カ行イ音便":"く","五段・ガ行":"ぐ","五段・サ行":"す",
    "五段・タ行":"つ","五段・ナ行":"ぬ","五段・バ行":"ぶ",
    "五段・マ行":"む","五段・ラ行":"る","五段・ワ行ウ音便":"う",
    "一段":"る","カ行変格活用":"くる","サ行変格活用":"する",
}

# 名詞に付く助詞
NOUN_PARTICLES = [
    ("", ""), ("が","が"), ("の","の"), ("に","に"), ("を","を"),
    ("は","は"), ("も","も"), ("で","で"), ("と","と"), ("へ","へ"),
    ("から","から"), ("まで","まで"), ("より","より"),
    ("には","には"), ("とは","とは"), ("でも","でも"),
    ("だ","だ"), ("な","な"),
]

# 形容詞活用形
ADJ_FORMS = [
    ("い", "い", [("",""),("よ","よ"),("な","な"),("ね","ね")]),
    ("く", "く", [("",""),("て","て"),("も","も"),("は","は"),("ない","ない")]),
    ("さ", "さ", [("",""),("が","が"),("に","に"),("も","も")]),
    ("かった","かった",[("",""),("よ","よ"),("の","の")]),
    ("くて","くて",[("",""),("も","も")]),
]


def get_verb_stem(base_form, reading, conj_type):
    """語幹と語幹読みを返す"""
    end = DICT_FORM_ENDINGS.get(conj_type, "")
    if end and base_form.endswith(end):
        stem_disp = base_form[:-len(end)]
    else:
        stem_disp = base_form
    if end and reading.endswith(end):
        stem_kana = reading[:-len(end)]
    else:
        stem_kana = reading
    return stem_disp, stem_kana


def get_adj_stem(base_form, reading):
    """形容詞語幹"""
    if base_form.endswith("い") and reading.endswith("い"):
        return base_form[:-1], reading[:-1]
    return base_form, reading


def collect_vocab(tsv_path, min_freq):
    """BCCWJ頻度リストTSVから語彙を収集"""
    vocab = {}
    skipped_freq = 0
    skipped_other = 0

    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            lform = row["lForm"]       # カタカナ読み
            lemma = row["lemma"]       # 表記
            pos = row["pos"]           # 品詞（名詞-普通名詞-一般 など）
            wtype = row["wType"]       # 語種（和・漢・外・混・固）
            try:
                freq = int(row["frequency"])
            except (ValueError, KeyError):
                skipped_other += 1
                continue

            # wType ごとの最低出現回数チェック
            threshold = min_freq.get(wtype, 0)
            if threshold <= 0 or freq < threshold:
                skipped_freq += 1
                continue

            # 読みをひらがなに変換
            reading = kata2hira(lform)
            if not reading or not lemma:
                skipped_other += 1
                continue

            # 品詞の正規化（BCCWJ形式 → 内部形式）
            if pos.startswith("動詞"):
                ipos, ipos1 = "動詞", ""
                conj_type = guess_conj_type(reading, lemma)
                if not conj_type:
                    skipped_other += 1
                    continue
            elif pos.startswith("形容詞"):
                ipos, ipos1 = "形容詞", ""
                if not reading.endswith("い"):
                    skipped_other += 1
                    continue
            elif pos.startswith("名詞-固有名詞"):
                ipos, ipos1 = "名詞", "固有名詞"
            elif pos.startswith("名詞"):
                ipos, ipos1 = "名詞", ""
            elif pos.startswith("副詞") or pos.startswith("感動詞"):
                ipos, ipos1 = "副詞", ""
            elif pos.startswith("形状詞"):
                ipos, ipos1 = "名詞", ""
            elif pos.startswith("代名詞"):
                ipos, ipos1 = "名詞", ""
            else:
                skipped_other += 1
                continue

            conj_type = conj_type if ipos == "動詞" else ""

            key = (lemma, reading)
            if key not in vocab:
                vocab[key] = (lemma, reading, ipos, ipos1, conj_type, lemma, freq)

    print(f"  頻度フィルタで除外: {skipped_freq:,} 件")
    print(f"  その他除外: {skipped_other:,} 件")
    return list(vocab.values())


def generate_bunsetsu(tsv_path, min_freq):
    results = []
    seen = set()

    def add(kana, display, btype, freq):
        kana = kana.strip()
        display = display.strip()
        if not kana or not display:
            return
        if not re.match(r'^[ぁ-んー]+$', kana):
            return
        if len(kana) < 1 or len(kana) > 12:
            return
        if kana not in seen:
            seen.add(kana)
            results.append((kana, display, btype, freq))

    print("  TSV読み取り中...")
    vocab = collect_vocab(tsv_path, min_freq)
    print(f"  語彙数: {len(vocab):,} 件")

    print("  文節展開中...")
    for surface, reading, pos, pos1, conj_type, base_form, freq in vocab:
        if not reading:
            continue

        # ── 動詞 ──────────────────────────────────────
        if pos == "動詞" and conj_type in VERB_FORMS:
            stem_disp, stem_kana = get_verb_stem(base_form, reading, conj_type)
            if len(stem_disp) != len(stem_kana):
                continue
            for form_kana, form_disp, follow in VERB_FORMS[conj_type]:
                full_kana = stem_kana + form_kana
                full_disp = stem_disp + form_disp
                add(full_kana, full_disp, "動詞節", freq)
                for fk, fd in follow:
                    if fk:
                        add(full_kana + fk, full_disp + fd, "動詞節", freq)

        # ── 形容詞 ────────────────────────────────────
        elif pos == "形容詞":
            stem_disp, stem_kana = get_adj_stem(base_form, reading)
            if len(stem_disp) != len(stem_kana):
                continue
            for form_kana, form_disp, follow in ADJ_FORMS:
                full_kana = stem_kana + form_kana
                full_disp = stem_disp + form_disp
                add(full_kana, full_disp, "形容詞節", freq)
                for fk, fd in follow:
                    if fk:
                        add(full_kana + fk, full_disp + fd, "形容詞節", freq)

        # ── 名詞 ──────────────────────────────────────
        elif pos == "名詞":
            btype = "固有名詞節" if pos1 == "固有名詞" else "名詞節"
            for fk, fd in NOUN_PARTICLES:
                add(reading + fk, surface + fd, btype, freq)

        # ── 副詞・その他 ──────────────────────────────
        elif pos in ("副詞",):
            add(reading, surface, "副詞節", freq)

    return results


def build_sqlite(bunsetsu_list, db_path):
    print(f"  SQLiteに書き込み中: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from database import apply_table
    apply_table(conn, "bunsetsu")

    rows = [(k, k[::-1], d, t, fr) for k, d, t, fr in bunsetsu_list]
    cur.executemany(
        "INSERT INTO bunsetsu (kana, kana_rev, display, type, freq) VALUES (?,?,?,?,?)",
        rows
    )
    conn.commit()

    total = cur.execute("SELECT COUNT(*) FROM bunsetsu").fetchone()[0]
    by_type = cur.execute(
        "SELECT type, COUNT(*) FROM bunsetsu GROUP BY type ORDER BY COUNT(*) DESC"
    ).fetchall()
    conn.close()

    print(f"  登録完了: {total:,} 件")
    for t, c in by_type:
        print(f"    {t or '不明'}: {c:,}")
    return total


def test_query(db_path):
    import time
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tests = [
        ("前方一致 'よ%'",       "SELECT kana,display FROM bunsetsu WHERE kana LIKE 'よ%' LIMIT 8"),
        ("後方一致 '%ぬ'(逆順)", "SELECT kana,display FROM bunsetsu WHERE kana_rev LIKE 'ぬ%' LIMIT 8"),
        ("後方一致 '%きぬ'(逆順)","SELECT kana,display FROM bunsetsu WHERE kana_rev LIKE 'ぬき%' LIMIT 8"),
    ]
    for label, sql in tests:
        t0 = time.perf_counter()
        rows = cur.execute(sql).fetchall()
        ms = (time.perf_counter() - t0) * 1000
        print(f"  [{label}] {ms:.2f}ms → {len(rows)} 件")
        for kana, display in rows:
            print(f"    {kana} ({display})")
    conn.close()


def main():
    db_path = Path(__file__).parent / "bunsetsu.db"
    tsv_path = Path(__file__).parent / TSV_FILE

    if not tsv_path.exists():
        print(f"エラー: {tsv_path} が見つかりません。")
        return

    print("=" * 55)
    print("  文節データベース構築（BCCWJ版）")
    print("  BCCWJ 頻度リスト → SQLite")
    print("=" * 55)

    print("\n  wType別 最低出現回数:")
    for wtype, threshold in sorted(MIN_FREQ.items()):
        label = "除外" if threshold <= 0 else f"{threshold}回以上"
        print(f"    {wtype}: {label}")

    print("\n[1/3] 文節生成中...")
    bunsetsu_list = generate_bunsetsu(str(tsv_path), MIN_FREQ)
    print(f"  生成完了: {len(bunsetsu_list):,} 文節")

    print("\n[2/3] SQLite構築中...")
    build_sqlite(bunsetsu_list, str(db_path))

    print("\n[3/3] クエリ性能テスト...")
    test_query(str(db_path))

    print(f"\n✅ 完了: {db_path}")


if __name__ == "__main__":
    main()

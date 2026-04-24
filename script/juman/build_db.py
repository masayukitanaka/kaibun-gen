"""
文節データベース構築スクリプト（JUMANPP版）
jumanpp.dic（テキスト形式）から語彙を読み取り
文節を生成して SQLite に格納する

jumanpp.dic フォーマット（CSV）:
  表層形,left_id,right_id,cost,品詞,品詞細分類,活用形,活用型,原形,読み,代表表記/代表読み,その他情報

テーブル設計:
  bunsetsu(id, kana, kana_rev, display, type, freq)
  INDEX ON kana, kana_rev, display
"""

import sqlite3
import sys
import re
from pathlib import Path

DIC_FILE = "jumanpp.dic"


def kata2hira(s: str) -> str:
    return "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ン" else c for c in s)


# ─── JUMANPP活用型 → 内部活用型のマッピング ────────────────
JUMANPP_CONJ_MAP = {
    "母音動詞":           "一段",
    "子音動詞カ行":       "五段・カ行イ音便",
    "子音動詞カ行促音便形": "五段・カ行イ音便",
    "子音動詞ガ行":       "五段・ガ行",
    "子音動詞サ行":       "五段・サ行",
    "子音動詞タ行":       "五段・タ行",
    "子音動詞ナ行":       "五段・ナ行",
    "子音動詞バ行":       "五段・バ行",
    "子音動詞マ行":       "五段・マ行",
    "子音動詞ラ行":       "五段・ラ行",
    "子音動詞ラ行イ形":   "五段・ラ行",
    "子音動詞ワ行":       "五段・ワ行ウ音便",
    "子音動詞ワ行文語音便形": "五段・ワ行ウ音便",
    "カ変動詞":           "カ行変格活用",
    "カ変動詞来":         "カ行変格活用",
    "サ変動詞":           "サ行変格活用",
    "ザ変動詞":           "サ行変格活用",
}


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

# 形容詞活用形（イ形容詞のみ）
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


def parse_dic_line(line):
    """jumanpp.dic の1行をパースして辞書エントリを返す"""
    # CSV形式だが、表層形にカンマを含む場合があるので最低限のフィールド数で判定
    parts = line.split(",")
    if len(parts) < 11:
        return None
    # 後ろから確定フィールドを取る（表層形にカンマが含まれる場合に対応）
    # フィールド数が多い場合、最初のN個が表層形に結合される
    # 固定フィールド: ...,pos,pos1,infl_form,conj_type,base,reading,repr,...
    # 末尾から11フィールド目以降が表層形
    # ただし、大半はフィールド12個なので簡易判定
    try:
        # 末尾の情報フィールドはカンマを含む可能性があるので、
        # 先頭から必要なフィールドを取得
        # fields[0]=surface, [1..3]=ids, [4]=pos, [5]=pos1, [6]=infl_form,
        # [7]=conj_type, [8]=base, [9]=reading, [10]=repr, [11..]=info
        pos = parts[4]
        pos1 = parts[5]
        infl_form = parts[6]
        conj_type = parts[7]
        base = parts[8]
        reading = parts[9]
        repr_field = parts[10]  # "代表表記/代表読み"
    except IndexError:
        return None

    return {
        "pos": pos,
        "pos1": pos1,
        "infl_form": infl_form,
        "conj_type": conj_type,
        "base": base,
        "reading": reading,
        "repr": repr_field,
    }


def collect_vocab(dic_path):
    """jumanpp.dic から語彙を収集"""
    vocab = {}
    skipped = 0

    with open(dic_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            entry = parse_dic_line(line)
            if not entry:
                skipped += 1
                continue

            pos = entry["pos"]
            pos1 = entry["pos1"]
            reading = entry["reading"]
            base = entry["base"]
            conj_type = entry["conj_type"]
            repr_field = entry["repr"]

            # UNK や特殊エントリを除外
            if reading in ("UNK", "\\t") or base == "UNK":
                continue

            # 代表表記から表示用の表記と読みを取得
            if "/" in repr_field:
                disp_surface, disp_reading = repr_field.split("/", 1)
            else:
                disp_surface = base
                disp_reading = reading

            # ── 動詞（基本形のみ）──────────────────────
            if pos == "動詞" and entry["infl_form"] == "基本形":
                mapped_conj = JUMANPP_CONJ_MAP.get(conj_type)
                if not mapped_conj:
                    skipped += 1
                    continue
                key = (disp_surface, disp_reading)
                if key not in vocab:
                    vocab[key] = (disp_surface, disp_reading, "動詞", "", mapped_conj, disp_surface)

            # ── イ形容詞（基本形のみ）────────────────────
            elif pos == "形容詞" and conj_type.startswith("イ形容詞") and entry["infl_form"] == "基本形":
                if not disp_reading.endswith("い"):
                    skipped += 1
                    continue
                key = (disp_surface, disp_reading)
                if key not in vocab:
                    vocab[key] = (disp_surface, disp_reading, "形容詞", "", "", disp_surface)

            # ── ナ形容詞 → 名詞扱い（語幹部分）─────────────
            elif pos == "形容詞" and conj_type.startswith("ナ形容詞") and entry["infl_form"] == "語幹":
                key = (disp_surface, disp_reading)
                if key not in vocab:
                    vocab[key] = (disp_surface, disp_reading, "名詞", "", "", disp_surface)

            # ── 名詞 ─────────────────────────────────
            elif pos == "名詞":
                is_proper = pos1 in ("人名", "地名", "組織名", "固有名詞")
                ipos1 = "固有名詞" if is_proper else ""
                key = (disp_surface, disp_reading)
                if key not in vocab:
                    vocab[key] = (disp_surface, disp_reading, "名詞", ipos1, "", disp_surface)

            # ── 副詞・感動詞 ──────────────────────────
            elif pos in ("副詞", "感動詞"):
                key = (disp_surface, disp_reading)
                if key not in vocab:
                    vocab[key] = (disp_surface, disp_reading, "副詞", "", "", disp_surface)

            else:
                skipped += 1

    print(f"  除外: {skipped:,} 件")
    return list(vocab.values())


def generate_bunsetsu(dic_path):
    results = []
    seen = set()

    def add(kana, display, btype):
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
            results.append((kana, display, btype))

    print("  辞書読み取り中...")
    vocab = collect_vocab(dic_path)
    print(f"  語彙数: {len(vocab):,} 件")

    print("  文節展開中...")
    for surface, reading, pos, pos1, conj_type, base_form in vocab:
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
                add(full_kana, full_disp, "動詞節")
                for fk, fd in follow:
                    if fk:
                        add(full_kana + fk, full_disp + fd, "動詞節")

        # ── 形容詞 ────────────────────────────────────
        elif pos == "形容詞":
            stem_disp, stem_kana = get_adj_stem(base_form, reading)
            if len(stem_disp) != len(stem_kana):
                continue
            for form_kana, form_disp, follow in ADJ_FORMS:
                full_kana = stem_kana + form_kana
                full_disp = stem_disp + form_disp
                add(full_kana, full_disp, "形容詞節")
                for fk, fd in follow:
                    if fk:
                        add(full_kana + fk, full_disp + fd, "形容詞節")

        # ── 名詞 ──────────────────────────────────────
        elif pos == "名詞":
            btype = "固有名詞節" if pos1 == "固有名詞" else "名詞節"
            for fk, fd in NOUN_PARTICLES:
                add(reading + fk, surface + fd, btype)

        # ── 副詞・その他 ──────────────────────────────
        elif pos == "副詞":
            add(reading, surface, "副詞節")

    return results


def build_sqlite(bunsetsu_list, db_path):
    print(f"  SQLiteに書き込み中: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from database import apply_table
    apply_table(conn, "bunsetsu")

    rows = [(k, k[::-1], d, t, 1) for k, d, t in bunsetsu_list]
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
    dic_path = Path(__file__).parent / DIC_FILE

    if not dic_path.exists():
        print(f"エラー: {dic_path} が見つかりません。")
        return

    print("=" * 55)
    print("  文節データベース構築（JUMANPP版）")
    print("  jumanpp.dic → SQLite")
    print("=" * 55)

    print("\n[1/3] 文節生成中...")
    bunsetsu_list = generate_bunsetsu(str(dic_path))
    print(f"  生成完了: {len(bunsetsu_list):,} 文節")

    print("\n[2/3] SQLite構築中...")
    build_sqlite(bunsetsu_list, str(db_path))

    print("\n[3/3] クエリ性能テスト...")
    test_query(str(db_path))

    print(f"\n✅ 完了: {db_path}")


if __name__ == "__main__":
    main()

"""
文節データベース構築スクリプト（改訂版）
MeCab + ipadic の辞書から文節を生成し SQLite に格納する

修正点:
- 活用形ごとに後続できる付属語を制限（「泳いだた」などを排除）
- 活用済みの動詞形（た形・て形など）には付属語を付けない
- 語幹+活用語尾+助詞の組み合わせを自然なものに限定

テーブル設計:
  bunsetsu(id, kana, kana_rev, display, type, freq)
  INDEX ON kana, kana_rev
"""

import sqlite3
import re
from pathlib import Path

def kata2hira(s: str) -> str:
    return "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ン" else c for c in s)

def parse_node(surface, feature):
    parts = feature.split(",")
    if len(parts) < 9:
        return None
    return {
        "surface":   surface,
        "pos":       parts[0],
        "pos1":      parts[1],
        "conj_type": parts[4],
        "base":      parts[6],
        "reading":   kata2hira(parts[7]) if parts[7] != "*" else None,
    }

# ─── 動詞: 活用形ごとの (語尾, 表記, 後続可能助詞リスト) ──────────
# 後続助詞が空リストの場合はその形単独で文節として扱う
VERB_FORMS = {
    "五段・カ行イ音便": [
        # (語幹への付加読み, 語幹への付加表記, [後続助詞(読み,表記)])
        ("く",   "く",   [("よ","よ"),("な","な"),("か","か")]),   # 連体形
        ("かない","かない",[]),
        ("き",   "き",   [("たい","たい"),("ながら","ながら")]),   # 連用形
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
        ("",     "",     [("たい","たい"),("ながら","ながら")]),  # 連用形=語幹
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

# 名詞に付く助詞
NOUN_PARTICLES = [
    ("", ""), ("が","が"), ("の","の"), ("に","に"), ("を","を"),
    ("は","は"), ("も","も"), ("で","で"), ("と","と"), ("へ","へ"),
    ("から","から"), ("まで","まで"), ("より","より"),
    ("には","には"), ("とは","とは"), ("でも","でも"),
    ("だ","だ"), ("な","な"), ("の","の"),
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
    endings = {
        "五段・カ行イ音便":"く","五段・ガ行":"ぐ","五段・サ行":"す",
        "五段・タ行":"つ","五段・ナ行":"ぬ","五段・バ行":"ぶ",
        "五段・マ行":"む","五段・ラ行":"る","五段・ワ行ウ音便":"う",
        "一段":"る","カ行変格活用":"くる","サ行変格活用":"する",
    }
    end = endings.get(conj_type, "")
    if end and base_form.endswith(end):
        stem_disp = base_form[:-len(end)] if end else base_form
    else:
        stem_disp = base_form
    if end and reading.endswith(end):
        stem_kana = reading[:-len(end)] if end else reading
    else:
        stem_kana = reading
    return stem_disp, stem_kana

def get_adj_stem(base_form, reading):
    """形容詞語幹"""
    if base_form.endswith("い") and reading.endswith("い"):
        return base_form[:-1], reading[:-1]
    return base_form, reading

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
            for form_kana, form_disp, follow in VERB_FORMS[conj_type]:
                full_kana = stem_kana + form_kana
                full_disp = stem_disp + form_disp
                # 形単独
                add(full_kana, full_disp, "動詞節")
                # 後続助詞
                for fk, fd in follow:
                    if fk:
                        add(full_kana + fk, full_disp + fd, "動詞節")

        # ── 形容詞 ────────────────────────────────────
        elif pos == "形容詞":
            stem_disp, stem_kana = get_adj_stem(base_form, reading)
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
        elif pos in ("副詞", "感動詞"):
            add(reading, surface, "副詞節")

    return results


def collect_vocab(dic_path):
    """MeCab辞書バイナリ(sys.dic)を直接読み取り語彙を収集"""
    import struct

    HEADER_SIZE = 72  # 10*uint32(40) + charset(32)
    dic_file = Path(dic_path) / "sys.dic"

    with open(dic_file, "rb") as f:
        f.seek(24)
        dsize = struct.unpack("<I", f.read(4))[0]
        tsize = struct.unpack("<I", f.read(4))[0]
        fsize = struct.unpack("<I", f.read(4))[0]

        f.seek(HEADER_SIZE + dsize)
        token_data = f.read(tsize)
        feature_data = f.read(fsize)

    num_tokens = len(token_data) // 16
    vocab = {}

    for i in range(num_tokens):
        feat_off = struct.unpack_from("<I", token_data, i * 16 + 8)[0]
        if feat_off >= len(feature_data):
            continue
        end = feature_data.find(b"\x00", feat_off)
        if end == -1:
            continue
        try:
            feature = feature_data[feat_off:end].decode("utf-8")
        except UnicodeDecodeError:
            continue
        parts = feature.split(",")
        if len(parts) < 9:
            continue
        pos, pos1 = parts[0], parts[1]
        conj_type = parts[4]
        base = parts[6]
        reading = kata2hira(parts[7]) if parts[7] != "*" else None
        if not reading or base == "*":
            continue
        if base not in vocab:
            vocab[base] = (base, reading, pos, pos1, conj_type, base)

    return list(vocab.values())


def build_sqlite(bunsetsu_list, db_path):
    print(f"  SQLiteに書き込み中: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS bunsetsu;
        CREATE TABLE bunsetsu (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            kana     TEXT NOT NULL,
            kana_rev TEXT NOT NULL,
            display  TEXT NOT NULL,
            type     TEXT,
            freq     INTEGER DEFAULT 1
        );
    """)
    rows = [(k, k[::-1], d, t, 1) for k, d, t in bunsetsu_list]
    cur.executemany(
        "INSERT INTO bunsetsu (kana, kana_rev, display, type, freq) VALUES (?,?,?,?,?)",
        rows
    )
    cur.executescript("""
        CREATE INDEX IF NOT EXISTS idx_kana     ON bunsetsu(kana);
        CREATE INDEX IF NOT EXISTS idx_kana_rev ON bunsetsu(kana_rev);
    """)
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
    print("=" * 55)
    print("  文節データベース構築（改訂版）")
    print("  mecab-ipadic-NEologd → SQLite")
    print("=" * 55)

    # mecab-ipadic-NEologd を使用
    NEOLOGD = "/opt/homebrew/lib/mecab/dic/mecab-ipadic-neologd"

    print("\n[1/3] 文節生成中...")
    bunsetsu_list = generate_bunsetsu(NEOLOGD)
    print(f"  生成完了: {len(bunsetsu_list):,} 文節")

    print("\n[2/3] SQLite構築中...")
    build_sqlite(bunsetsu_list, str(db_path))

    print("\n[3/3] クエリ性能テスト...")
    test_query(str(db_path))

    print(f"\n✅ 完了: {db_path}")


if __name__ == "__main__":
    main()

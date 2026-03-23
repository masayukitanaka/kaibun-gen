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

import MeCab
import ipadic
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

def generate_bunsetsu(tagger):
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

    print("  語彙収集中...")
    vocab = collect_vocab(tagger)
    print(f"  語彙数: {len(vocab)} 件")

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


def collect_vocab(tagger):
    """MeCabで解析して語彙を収集"""
    seed_texts = [
        "食べる飲む走る歩く泳ぐ飛ぶ読む書く聞く話す見る来る行く帰る起きる寝る",
        "働く遊ぶ学ぶ教える考える思う感じる笑う泣く怒る喜ぶ悲しむ驚く",
        "作る壊す開く閉める買う売る借りる貸す持つ置く取る渡す受ける与える",
        "始める終える続ける止める変える直す調べる探す見つける失う忘れる覚える",
        "生きる死ぬ生まれる育つ成長する衰える回復する治る病む苦しむ楽しむ",
        "歌う踊る弾く描く撮る録る計る量る測る比べる選ぶ決める判断する",
        "乗る降りる運転する止まる動く回る転がる落ちる上がる下がる",
        "送る受け取る届ける運ぶ積む降ろす並べる整える片付ける",
        "切る割る折る曲げる伸ばす縮める広げる押す引く叩く蹴る",
        "洗う拭く掃除する整理する捨てる拾う集める分ける合わせる混ぜる",
        "咲く散る枯れる実る育てる植える刈る掘る蒔く収穫する",
        "泣ける燃える溶ける固まる膨らむ縮む揺れる震える光る輝く",
        "騙す疑う信じる認める否定する肯定する反論する同意する",
        "急ぐ焦る慌てる落ち着く冷静になる緊張する緩む",
        "漂う漕ぐ潜る浮かぶ沈む流れる",
        "山川海空雲風雨雪花木草石土水火光影音",
        "森林原野砂漠島半島岬湾湖池沼河川滝泉温泉",
        "春夏秋冬朝昼夜夕暮れ夜明け日暮れ",
        "東西南北上下左右前後中外",
        "人男女子供大人老人親子兄弟姉妹夫婦家族",
        "先生生徒学生医者看護師警官消防士料理人農家漁師",
        "王様女王騎士勇者魔法使い妖精悪魔天使神仏",
        "友達恋人仲間敵味方ライバルパートナー",
        "本紙ペン鉛筆刀剣弓矢盾鎧兜",
        "家部屋窓ドア壁床天井屋根階段廊下",
        "車電車船飛行機自転車バイク馬",
        "服靴帽子鞄財布時計指輪眼鏡",
        "米麦豆芋野菜果物肉魚卵牛乳",
        "皿茶碗箸鍋釜包丁まな板",
        "犬猫鳥魚虫蛇馬牛豚羊鹿熊狼狐狸兎",
        "桜梅松竹柳藤菊薔薇百合蓮朝顔ひまわり",
        "愛憎喜悲怒哀楽恐怖希望絶望信頼不信",
        "力技術知識経験記憶感情意志夢現実",
        "時間空間距離重さ大きさ温度速度",
        "国都市町村道路橋港駅空港",
        "病院学校図書館公園神社寺",
        "大きい小さい長い短い広い狭い高い低い重い軽い",
        "速い遅い強い弱い硬い柔らかい熱い冷たい暖かい涼しい",
        "明るい暗い赤い青い白い黒い黄色い",
        "美しい醜い綺麗汚い清潔不潔",
        "楽しい悲しい嬉しい怖い辛い苦しい痛い",
        "新しい古い若い正しい間違った良い悪い",
        "難しい易しい複雑単純詳しい簡単",
        "とても非常にかなりやや少しほとんど全く",
        "すぐにすでにまだもうやっとついにとうとう",
        "昨日今日明日今年来年去年先月今月来月",
        "タヌキキツネクマサクラウメフジ富士山琵琶湖",
    ]

    vocab = {}
    for text in seed_texts:
        node = tagger.parseToNode(text)
        while node:
            surface = node.surface
            feature = node.feature
            if surface and feature and surface not in vocab:
                parsed = parse_node(surface, feature)
                if parsed and parsed["reading"]:
                    vocab[surface] = (
                        parsed["surface"], parsed["reading"],
                        parsed["pos"], parsed["pos1"],
                        parsed["conj_type"], parsed["base"]
                    )
            node = node.next

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
    print("  MeCab + ipadic → SQLite")
    print("=" * 55)

    tagger = MeCab.Tagger(ipadic.MECAB_ARGS)

    print("\n[1/3] 文節生成中...")
    bunsetsu_list = generate_bunsetsu(tagger)
    print(f"  生成完了: {len(bunsetsu_list):,} 文節")

    print("\n[2/3] SQLite構築中...")
    build_sqlite(bunsetsu_list, str(db_path))

    print("\n[3/3] クエリ性能テスト...")
    test_query(str(db_path))

    print(f"\n✅ 完了: {db_path}")


if __name__ == "__main__":
    main()

"""
データベーススキーマの一元管理。
各スクリプトはここから DDL を取得して適用する。
"""

from pathlib import Path

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def read_schema():
    """schema.sql の全文を返す"""
    return _SCHEMA_PATH.read_text(encoding="utf-8")


def _extract_block(schema_text, table_name):
    """schema.sql から特定テーブルの DROP〜CREATE INDEX ブロックを抽出する"""
    lines = schema_text.splitlines()
    result = []
    capturing = False
    for line in lines:
        stripped = line.strip().upper()
        # このテーブルの DROP TABLE で開始
        if stripped.startswith("DROP TABLE") and table_name.upper() in stripped:
            capturing = True
        # 別のテーブルの DROP TABLE が来たら終了
        elif stripped.startswith("DROP TABLE") and table_name.upper() not in stripped:
            if capturing:
                break
        # 空行以外のコメント行がきたら、テーブルブロック終了（セクション区切り）
        elif stripped.startswith("-- ──") and capturing and result:
            break
        if capturing:
            result.append(line)
    return "\n".join(result)


def apply_table(conn, table_name, skip_indexes=False):
    """schema.sql から指定テーブルの DDL を抽出して適用する。
    skip_indexes=True の場合、CREATE INDEX を除外する（大量INSERT前に使用）。
    """
    schema = read_schema()
    ddl = _extract_block(schema, table_name)
    if not ddl.strip():
        raise ValueError(f"schema.sql にテーブル '{table_name}' が見つかりません")
    if skip_indexes:
        lines = [l for l in ddl.splitlines()
                 if not l.strip().upper().startswith("CREATE INDEX")]
        ddl = "\n".join(lines)
    conn.executescript(ddl)


def apply_indexes(conn, table_name):
    """schema.sql から指定テーブルの CREATE INDEX のみを抽出して適用する"""
    schema = read_schema()
    ddl = _extract_block(schema, table_name)
    lines = [l for l in ddl.splitlines()
             if l.strip().upper().startswith("CREATE INDEX")]
    if lines:
        conn.executescript("\n".join(lines))


def ensure_table(conn, table_name):
    """テーブルが存在しなければ schema.sql から作成する（DROP しない）"""
    cur = conn.cursor()
    row = cur.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row[0] == 0:
        # DROP を除去して CREATE のみ実行
        schema = read_schema()
        ddl = _extract_block(schema, table_name)
        lines = [
            l for l in ddl.splitlines()
            if not l.strip().upper().startswith("DROP TABLE")
        ]
        conn.executescript("\n".join(lines))

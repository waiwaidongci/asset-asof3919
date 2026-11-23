"""大簿入库收尾的集合操作 SQL。

以纯字符串保存，便于在 SQLite 上做逻辑回归（tests/test_ingest_sql.py）；
线上由 SQLAlchemy text() 在 PostgreSQL 执行。所有检查都在数据库内完成，
应用内存一次最多暂放一批（BATCH_SIZE）记录。
"""

# 同号异件：同一事件编号出现不同内容，按原簿行号报出最早的一组冲突。
SQL_CONFLICT = """
SELECT event_id FROM ledger_events
WHERE dataset_id = :dataset
GROUP BY event_id
HAVING COUNT(DISTINCT content_hash) > 1
ORDER BY MIN(source_row)
LIMIT 1
"""

SQL_EVENT_FIRST = """
SELECT source_row, content_hash FROM ledger_events
WHERE dataset_id = :dataset AND event_id = :event
ORDER BY source_row
LIMIT 1
"""

SQL_EVENT_CONFLICT = """
SELECT source_row FROM ledger_events
WHERE dataset_id = :dataset AND event_id = :event AND content_hash <> :hash
ORDER BY source_row
LIMIT 1
"""

# 同一工具同一天重复使用顺序号（不同事件编号），按原簿行号报出最早一组。
SQL_SLOT = """
SELECT asset_id, event_date, sequence FROM ledger_events
WHERE dataset_id = :dataset
GROUP BY asset_id, event_date, sequence
HAVING COUNT(DISTINCT event_id) > 1
ORDER BY MIN(source_row)
LIMIT 1
"""

SQL_SLOT_ROWS = """
SELECT MIN(source_row) AS source_row, event_id FROM ledger_events
WHERE dataset_id = :dataset AND asset_id = :asset AND event_date = :day AND sequence = :seq
GROUP BY event_id
ORDER BY source_row
LIMIT 2
"""

# 回放序号：按（交接日期、工具编号、同日顺序号、物理顺序）全簿编号，
# 回放按序号分页前进，重抄的同号同内容记录相邻，由引擎跳过。
SQL_REPLAY_ORDER = """
UPDATE ledger_events SET replay_order = ranked.rn FROM (
    SELECT id, ROW_NUMBER() OVER (ORDER BY event_date, asset_id, sequence, id) AS rn
    FROM ledger_events WHERE dataset_id = :dataset
) AS ranked
WHERE ledger_events.id = ranked.id AND ledger_events.dataset_id = :dataset
"""

# 去重后的可回放事件数（同号同内容只算一次）。
SQL_EVENT_COUNT = """
SELECT COUNT(DISTINCT content_hash) FROM ledger_events WHERE dataset_id = :dataset
"""

# 002 大簿分批入库与断点续跑

支持百万行交接簿：事件改为分片落库，盘点按批回放并保存断点。新部署由应用启动自动建表；已有数据的部署执行下列迁移。

## 变更

- `datasets` 不再以 JSON 保存整批行，新增入库状态与进度列；`checksum` 允许为空（入库完成后才落定）。
- 新增 `ledger_chunks` 分片回执（按批次与分片号唯一，重传不重复计数）。
- 新增 `ledger_events` 交接事件，保留原簿行号与内容指纹，`replay_order` 为全簿回放序号。
- 新增 `run_checkpoints` 盘点断点（回放游标、引擎状态、已处理笔数）。
- 新增 `run_asset_states` 盘点过程中的工具状态（按运行与工具编号为主键）。

```sql
ALTER TABLE datasets ADD COLUMN status VARCHAR(20) NOT NULL DEFAULT 'ready';
ALTER TABLE datasets ADD COLUMN imported_rows INTEGER NOT NULL DEFAULT 0;
ALTER TABLE datasets ADD COLUMN next_chunk INTEGER NOT NULL DEFAULT 0;
ALTER TABLE datasets ADD COLUMN event_count INTEGER;
ALTER TABLE datasets ADD COLUMN error VARCHAR(500);
ALTER TABLE datasets ADD COLUMN rolling_hash VARCHAR(64);
ALTER TABLE datasets ALTER COLUMN checksum DROP NOT NULL;
ALTER TABLE datasets DROP COLUMN rows;

CREATE TABLE ledger_chunks (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
    chunk_index INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (dataset_id, chunk_index)
);

CREATE TABLE ledger_events (
    id SERIAL PRIMARY KEY,
    dataset_id INTEGER NOT NULL REFERENCES datasets(id),
    chunk_index INTEGER NOT NULL,
    source_row INTEGER NOT NULL,
    event_id VARCHAR(80) NOT NULL,
    asset_id VARCHAR(80) NOT NULL,
    asset_name VARCHAR(120),
    event_date DATE NOT NULL,
    sequence INTEGER NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    replay_order INTEGER
);
CREATE INDEX ix_ledger_events_replay ON ledger_events (dataset_id, replay_order);
CREATE INDEX ix_ledger_events_event ON ledger_events (dataset_id, event_id);
CREATE INDEX ix_ledger_events_slot ON ledger_events (dataset_id, asset_id, event_date, sequence);

CREATE TABLE run_checkpoints (
    run_id INTEGER PRIMARY KEY REFERENCES runs(id),
    cursor INTEGER NOT NULL DEFAULT 0,
    processed INTEGER NOT NULL DEFAULT 0,
    payload JSON NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE run_asset_states (
    run_id INTEGER NOT NULL REFERENCES runs(id),
    asset_id VARCHAR(80) NOT NULL,
    asset_name VARCHAR(120),
    status VARCHAR(20) NOT NULL,
    last_event_date DATE NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_event_type VARCHAR(20) NOT NULL,
    last_event_id VARCHAR(80) NOT NULL,
    PRIMARY KEY (run_id, asset_id)
);
```

历史 `datasets` 行迁移后 `status` 为 `ready`，行明细不再保留；如需查阅明细请重新导入原 CSV。

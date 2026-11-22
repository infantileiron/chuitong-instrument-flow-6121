-- 可续跑的分批核算：运行拆成固定批次，每批中间态独立持久化，
-- 计算中断后从首个未完成批次续跑，沿用创建运行时冻结的同一份快照。
ALTER TABLE runs
    MODIFY status ENUM('queued','running','done','failed') NOT NULL DEFAULT 'queued',
    ADD COLUMN batch_size INT NOT NULL DEFAULT 5 AFTER result,
    ADD COLUMN total_batches INT NOT NULL DEFAULT 0 AFTER batch_size,
    ADD COLUMN worker_epoch INT NOT NULL DEFAULT 0 AFTER total_batches,
    ADD COLUMN heartbeat_at DATETIME NULL AFTER worker_epoch;

CREATE TABLE IF NOT EXISTS run_batches (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT NOT NULL,
    batch_no INT NOT NULL,
    -- 本批处理的快照凭据序号区间（左闭右开），批次边界由冻结快照确定，续跑不漂移
    start_index INT NOT NULL,
    end_index INT NOT NULL,
    status ENUM('queued','running','done','failed') NOT NULL DEFAULT 'queued',
    -- 累加器：(器材族, 出借馆, 入借馆) -> [签领件数, 冲销件数]，以及已配对的原签领 id
    partial JSON,
    reversed_ids JSON,
    error VARCHAR(240),
    worker_id VARCHAR(64) NOT NULL DEFAULT '',
    attempt INT NOT NULL DEFAULT 0,
    started_at DATETIME NULL,
    finished_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, batch_no),
    FOREIGN KEY (run_id) REFERENCES runs(id),
    INDEX run_batch_queue (run_id, status, batch_no)
);

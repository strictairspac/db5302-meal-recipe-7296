# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本只包含初始 schema。需要在已有部署升级表结构时，应单独引入版本化迁移。

分段展算使用两张新增表，均由 `create_all` 直接创建，可在已有库上原地补齐：

- `recipe_expansion_segments`：按 `(run_id, seq)` 保存已完成的展开段，重试时从最后一段继续。
- `run_leases`：保存当前执行者的租约与心跳时间，用于判断执行是否中断并允许重试。

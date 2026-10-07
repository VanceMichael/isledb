# 数据库销毁生命周期（Database Destroy Lifecycle）实施计划

## Repository Research

### 现有架构事实（基于代码）

- **对象布局**（[object-store-schema.md](file:///Users/vance/project/mineProject/swe/092906/project-03/docs/object-store-schema.md)、[blobstore.go](file:///Users/vance/project/mineProject/swe/092906/project-03/blobstore/blobstore.go#L149-L191)）：一个前缀 = 一个库。
  - `manifest/CURRENT`：唯一权威头，条件写（If-Match ETag/Generation；空 ETag 走 If-None-Match 创建）。
  - `manifest/snapshots/*.manifest.zst`、`manifest/pages/lXX/*.page.zst`：不可变清单快照/页。
  - `manifest/gc/{sst,change-feed,pages,snapshots,page-marks}/...`：GC 计划与标记。
  - `maintenance/HEAD`：maintenance 与 writer 之间的邮箱，独立 CAS（[maintenance_head.go](file:///Users/vance/project/mineProject/swe/092906/project-03/internal/manifest/maintenance_head.go)）。
  - `sstable/<bucket>/<id>`、`changes/<bucket>/<id>`：不可变数据/变更批次。
- **CURRENT 模型**（[manifest.go](file:///Users/vance/project/mineProject/swe/092906/project-03/internal/manifest/manifest.go#L128-L152)）：`Current` 含 `WriterFence`/`CompactorFence`、`MaxPinnedViewAge`、活动条目、页前沿、回执等；`EncodeCurrent/DecodeCurrent` 严格校验 `layout_version=2, format=isledb-manifest-v2`。
- **所有权与发布**（[store.go](file:///Users/vance/project/mineProject/swe/092906/project-03/internal/manifest/store.go)）：
  - writer/compactor 所有权 = `FenceToken{Epoch,Owner,ClaimedAt}`，在 CURRENT 中以 CAS 换代；旧进程发布时 `checkFenceWithCurrent` 返回 `ErrFenced`。
  - CURRENT 的全部 5 个写入点：`claimFence`(L233)、`appendInternal`(L530)、`EnableChangeFeed`(L1488)、`ApplyPendingMaintenance`(L455)、`AdvanceChangeFeedLogStart`(L2108)。
  - maintenance HEAD 的全部 3 个写入点：`ClaimMaintenance`、`StageMaintenance`、`ClearMaintenance`。
  - CAS 原语：`readCurrentWithETag` + `writeEncodedCurrentWithCAS`；`ErrPreconditionFailed` 重试，`currentObserved` 保证“见过 CURRENT 后再丢头 = fail-closed”。
  - 未确认 CAS 的对账：`reconcilePendingWriterCommit` 依据不可变条目位置+指纹识别已成功的提交。
- **Open fail-closed**（[api.go](file:///Users/vance/project/mineProject/swe/092906/project-03/api.go#L50-L83)）：无 CURRENT 时列举不可变对象族，前缀非空但无头 = `ErrManifestUnavailable`；空前缀才视为新库。
- **Reader pinned view**（[reader.go](file:///Users/vance/project/mineProject/swe/092906/project-03/reader.go#L180-L290)）：视图在 `viewRefreshAt` 后可刷新、`viewExpiresAt=加载时间+MaxPinnedViewAge` 后强制刷新；`Reload→ReplayWithArtifactValidation`，刷新失败不替换旧视图；过期后所有读/迭代器经 `ensureFreshManifest` 收敛到刷新错误。Snapshot/Iterator 继承同一 deadline。
- **ChangeReader**（[change_reader.go](file:///Users/vance/project/mineProject/swe/092906/project-03/change_reader.go#L266-L316)）：每次 Read 经 `LoadChangeFeedView` 重读 CURRENT（视图带 PinnedViewAge 过期时间）。
- **Writer**（[writer.go](file:///Users/vance/project/mineProject/swe/092906/project-03/writer.go#L442-L503)）：flush 先 `pollPendingMaintenance`（读 CURRENT 的 writer 门），再上传 SST/change batch，最后 `AppendWriterCommit`；fence 错误使 writer 安静终止。
- **Maintenance**（[maintenance.go](file:///Users/vance/project/mineProject/swe/092906/project-03/maintenance.go)）：`Run` 控制循环 + 3 条独立回收 lane；fence 错误是终止信号。压缩/清理产出均经 `StageMaintenance`（HEAD）+ writer `ApplyPendingMaintenance`（CURRENT）。
- **批删/列举**（[blobstore.go](file:///Users/vance/project/mineProject/swe/092906/project-03/blobstore/blobstore.go#L482-L630)）：`Delete`（NotFound 视为成功）、`BatchDelete`（去重、`BatchDeleteError.Failed`）、`ListPage`（页 token 不保证跨进程持久）、`Walk`。GC 计划/标记全部位于 `manifest/gc/`。
- 测试基座：`blobstore.NewMemory(prefix)`（memblob），fence/multiphase 等测试直接构造 manifest store。

### 关键设计决策

1. **终止标记 = 终止态 CURRENT 自身**。CURRENT 在清扫中永不删除，天然满足“保留最小终止标记，防止把缺失 CURRENT 当新库”。
2. **单次 CAS 完成两件事**：把 CURRENT 置为不可逆 `lifecycle="destroyed"`，同时清空 `WriterFence`/`CompactorFence`（旧 token 立即不匹配）。
3. **先确认终止，后删数据**；物理删除尊重既有 pinned-view 安全窗口：`notBefore = DestroyedAt + MaxPinnedViewAge`。控制对象（`maintenance/HEAD`、`manifest/gc/**`）提交终止后即可删；不可变数据（SST、change batch、snapshot、page）到点后才删。
4. **稳定错误哨兵**：`manifest.ErrDBDestroyed`，根包导出 `ErrDatabaseDestroyed`，所有闸门返回 `errors.Is` 可识别的同一错误。
5. **持久进度**：终止 CURRENT 内嵌 `DestructionRecord{destroyed_at, pinned_view_age, sweep_cursor, swept, swept_at}`；每批删除成功后 CAS 记录“最后完成键”。provider 页 token 不可跨进程依赖，恢复时从头列举、跳过 `<= cursor` 的键（比较是保守的，重复删安全）。
6. **幂等/响应丢失**：`CommitDestruction` 发现已终止即返回成功并重读记录；CAS 竞争重试。重复 Destroy 从持久状态继续，最终 `swept=true`。
7. **fail-closed 边界不变**：缺失/损坏 CURRENT 与普通 `DB.Close` 不触发销毁；Destroy 对空前缀/无头非空前缀一律拒绝（不新建标记）。

## Files and Modules

### 新增

- `internal/manifest/destruction.go`：生命周期类型、`ErrDBDestroyed`、CURRENT 终止/进度 CAS、`Destroyed()` 判定与统一闸门辅助。
- `destruction.go`（根包）：`DestroyOptions`、`DestroyResult`、`Destroy`、`DestroyBucket`、`destroyPrefix`（终止提交 + 两阶段可恢复清扫）。
- `destruction_test.go`（根包）：端到端语义测试。
- `internal/manifest/destruction_test.go`：CURRENT 状态机与闸门单元测试。

### 修改

- `internal/manifest/manifest.go`：`Current` 增加 `Lifecycle` 与 `Destruction *DestructionRecord`；编码/解码校验。
- `internal/manifest/state.go`：`Current.Clone()` 复制新字段。
- `internal/manifest/store.go`：5 个 CURRENT 写入点 + 3 类读/检查路径（Replay×3、LoadChangeFeedView、checkFence、CheckCompactorFenceToken、PrepareCheckpoint）加销毁闸门。
- `internal/manifest/maintenance_head.go`：`ClaimMaintenance`/`StageMaintenance`/`ClearMaintenance` 前置 CURRENT 销毁闸门。
- `api.go`：依赖 Replay 闸门即可（Open/Reader 路径）；缺失 CURRENT 分类逻辑保持不变。
- `writer.go`：新增 destroyed 终止态（与 fenced 同级控制流），Put/Flush/Close/后台循环收敛到 `ErrDBDestroyed`，不再尝试上传/发布。
- `maintenance.go`：`Run` 把 destroyed 视同 fence 终止；`reconcilePendingCommand` 对终止 CURRENT 返回 `ErrDBDestroyed`。
- `db.go`：`Writer.Close`/`closeDB` 的释放条件接受 `ErrDBDestroyed`（错误仍返回给调用方）。
- `docs/object-store-schema.md`：补充销毁生命周期与终止标记小节（运维向，既有文档）。

## Implementation Steps

1. **manifest 数据模型**
   - 新增 `LifecycleState`（`""` 活跃、`"destroyed"` 终止）与 `DestructionRecord`。
   - `Current.Lifecycle`、`Current.Destruction` JSON 字段（omitempty，旧 CURRENT 默认为活跃，向前兼容）。
   - 校验：终止态必须带 `destroyed_at`、`pinned_view_age>0`；非法 lifecycle 值拒绝；`Clone()` 深拷贝。
2. **错误与终止提交（`destruction.go`）**
   - `ErrDBDestroyed = errors.New("database destroyed")`；`(c *Current) Destroyed()`、`(c *Current) DestructionNotBefore()`。
   - `CommitDestruction(ctx) (*Current, error)`：commitMu 串行 + CAS 重试；nil CURRENT 返回 `ErrCurrentUnavailable`；已终止直接幂等返回；否则置 lifecycle、清双 fence、写记录。
   - `PersistDestructionProgress(ctx, cursor string, swept bool)`：仅终止态可写；CAS 重试；已 swept 时幂等 no-op。
3. **manifest 写入/读取闸门**
   - 5 个 CURRENT 写入点：claimFence/appendInternal/EnableChangeFeed/ApplyPendingMaintenance/AdvanceChangeFeedLogStart 在读到终止 CURRENT 时返回 `ErrDBDestroyed`；append 路径先保留“未确认提交对账成功”语义，再拦截新发布，并清空进程内双 fence。
   - 3 个 HEAD 写入点：读 CURRENT 确认未终止后再操作 HEAD。
   - Replay×3、LoadChangeFeedView、PrepareCheckpoint、checkFence、CheckCompactorFenceToken：终止 → `ErrDBDestroyed`（nil fence 原 ErrFenced 行为保留给非终止态）。
4. **根包销毁 API 与清扫器（`destruction.go`）**
   - `Destroy/DestroyBucket`：打开 store → 分类（空前缀/无头非空 → fail-closed 错误；损坏 CURRENT → 原样错误）→ `CommitDestruction` → 清扫。
   - 阶段 A（立即）：分批批删 `maintenance/` 与 `manifest/gc/` 前缀（幂等，NotFound 成功）。
   - 阶段 B（`now >= notBefore`）：按整前缀字典序列举（`ListPage` 有界页），跳过 `manifest/CURRENT` 与 `<= sweep_cursor` 的键；`BatchDelete`；整批成功后 CAS 推进 cursor；任何部分失败则返回错误、不推进 cursor。到点前返回 `DestroyResult{Terminal:true, Swept:false, RetryAfter:d}` 且 err=nil。
   - 完成：CAS `swept=true,swept_at`；已 swept 的再次调用做一次验证列举（清理竞态孤儿），无残留才报 Swept。
   - CAS 竞争/响应丢失：重读 CURRENT 识别终止与进度后继续。
5. **句柄收敛**
   - Writer：destroyed 标志；flush 轮询/发布观测到销毁即置位；Put 返回 `ErrDBDestroyed`；后台循环安静退出；Close 释放 writer 槽但返回错误。
   - Reader：Refresh/到期刷新返回 `ErrDBDestroyed` 且旧 pinned 视图不被替换；未到期 Get/Scan/Snapshot/Iterator 继续服务；到期后所有操作收敛到 `ErrDBDestroyed`（不刷新为活跃态）。
   - ChangeReader：OpenChangeReader 与 Read 刷新（LoadChangeFeedView）返回 `ErrDBDestroyed`。
   - Maintenance：OpenMaintenance（Replay + Claim×2）失败；陈旧 Run 收到 destroyed 即终止；stage/clear 无法改 HEAD；ApplyPendingMaintenance 无法改 CURRENT。
6. **测试（内存 bucket，短 MaxPinnedViewAge）**
   - 空前缀/无头非空/损坏 CURRENT → Destroy 拒绝；Open 行为不回归。
   - 提交终止：fence 清空；陈旧 writer Flush/Close 失败且 CURRENT 不变（NextSeq/条目不变）；陈旧 maintenance stage/clear/apply 失败且 HEAD/CURRENT 不变。
   - Open/OpenBucket/OpenWriter/OpenReader/OpenChangeReader/OpenMaintenance/Reader.Refresh → `errors.Is(err, ErrDatabaseDestroyed)`。
   - Reader/Snapshot/Iterator 在年龄边界内可读；强制刷新即失败且旧视图仍可用至到期；到期后收敛到销毁错误。
   - 两阶段清扫：notBefore 前只删控制对象并 RetryAfter；到点后 SST/changes/snapshots/pages/gc/HEAD 全清、仅 CURRENT 留。
   - 中断恢复：列举/批删中途失败与重复调用从 cursor 继续；二次 Destroy 幂等；swept 后验证列举。
   - 未销毁库的读写/compaction/回收全量既有测试不回归。
7. **文档与验证**
   - schema 文档增加“数据库销毁（终止标记与清扫）”小节。
   - `go build ./...`、`go test ./...`、`go vet ./...`、golangci-lint（按 `.golangci.yml`）。

## Dependencies and Considerations

- memblob/fileblob 的 ETag 基于内容：CAS 语义由 `WriteIfMatch/WriteIfNotExist` 既有实现保证，终止提交沿用同一路径，无需新原语。
- 终止 CURRENT 体积仅增加一个小记录，远低于 64KiB 上限。
- 旧版本二进制读到未知字段会忽略但仍可解码；终止态由新版本 Open 拦截。不做跨版本兼容承诺（运维工具与库同版本部署）。
- GC 回收 lane 读 `manifest/gc/` 计划：阶段 A 删除它们后，陈旧 maintenance 的回收 lane 会因找不到计划而空转，并随 Run 控制循环观测到销毁而整体退出。
- 陈旧 writer 在“轮询通过→上传 SST→终止提交→CAS 失败”窗口可能留下无主 SST：与现有 fence 孤儿同型；swept 验证列举会回收终止后被发现的对象；持续存活的陈旧进程在下次 flush 轮询（默认秒级）即终止。
- `DB.Close` 路径完全不触碰销毁逻辑。

## Validation

- 新增单元/端到端测试全部通过（内存 bucket + 故障注入：批删部分失败、进程退出=新 store 实例恢复、CAS 响应丢失=重复 CommitDestruction）。
- `go test ./...`（含 `internal/manifest`、blobstore、根包全部既有 fence/GC/reader 测试）零回归。
- `go vet ./...` 与 repo golangci 配置通过。
- 手工核对：终止后 CURRENT 仍可 GET 且解码为 destroyed；除 CURRENT 前缀下无其他对象；重复 Open 稳定报 `ErrDatabaseDestroyed`。

## Risks

- **风险：遗漏某个写 CURRENT/HEAD 的路径** → 已穷举 5+3 个写入点逐一加闸门；测试用陈旧句柄全操作矩阵验证对象字节/ETag 不变。
- **风险：cursor 恢复漏删** → cursor 仅作“跳过下界”，删除全幂等；swept 前必须完整扫到 EOF；swept 后再验证列举一次。
- **风险：pinned 窗口内误删数据** → notBefore 严格来自终止 CURRENT 的 `DestroyedAt + PinnedViewAge`，阶段 B 到点才运行；控制对象与数据对象分阶段。
- **风险：把既有 fail-closed 误改成销毁** → Destroy 显式拒绝缺失/损坏 CURRENT；Open 缺失 CURRENT 分支代码不动；普通 Close 不新增任何远端变更。

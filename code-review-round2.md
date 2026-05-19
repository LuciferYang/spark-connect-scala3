# Round 2 Multi-Role Code Review

> **范围**: cb6f016..e791edb（本轮 code-review-todo.md 11 项全部完成后的累积变更）
> **日期**: 2026-05-19（第 50 轮迭代后更新）
> **方法**: 3 名独立 reviewer 并行 review（accuracy / consistency / completeness）

---

## 总评

| Reviewer | 判定 | CRITICAL | HIGH | MEDIUM | LOW |
|---|---|---|---|---|---|
| code-reviewer | 通过 | 0 | 0 | 3 | 4 |
| security-reviewer | 通过 | 0 | 0 | 0 | 2 |
| three-hat（综合） | 通过 | 0 | 1 | 4 | 5 |
| 第 5 轮 completeness | 警告 | 0 | 1 | 2 | 0 |
| 第 10 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 11 轮 completeness | 警告 | 0 | 1 | 0 | 1 |
| 第 12 轮 completeness | 警告 | 0 | 0 | 0 | 1 |
| 第 15 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 16 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 17 轮 completeness | 警告 | 0 | 1 | 0 | 0 |
| 第 18 轮 completeness | 警告 | 0 | 0 | 1 | 1 |
| 第 19 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 20 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 21 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 22 轮 completeness | 警告 | 0 | 0 | 2 | 0 |
| 第 23 轮 completeness | 通过 | 0 | 0 | 0 | 0 |
| 第 24 轮 completeness | 通过 | 0 | 0 | 0 | 0 |
| 第 25 轮 accuracy | 警告 | 0 | 0 | 0 | 1 |
| 第 25 轮 completeness | 警告 | 0 | 1 | 0 | 0 |
| 第 26 轮 consistency | 警告 | 0 | 1 | 1 | 0 |
| 第 26 轮 completeness | 警告 | 0 | 1 | 0 | 0 |
| 第 27 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 27 轮 consistency | 警告 | 0 | 2 | 1 | 0 |
| 第 27 轮 completeness | 通过 | 0 | 0 | 0 | 0 |
| 第 28 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 28 轮 consistency | 警告 | 0 | 0 | 1 | 0 |
| 第 28 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 29 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 29 轮 consistency | 警告 | 0 | 1 | 1 | 0 |
| 第 29 轮 completeness | 通过 | 0 | 0 | 0 | 0 |
| 第 30 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 30 轮 consistency | 通过 | 0 | 0 | 0 | 0 |
| 第 30 轮 completeness | 警告 | 0 | 1 | 2 | 0 |
| 第 31 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 31 轮 consistency | 警告 | 0 | 0 | 2 | 0 |
| 第 31 轮 completeness | 警告 | 0 | 1 | 1 | 0 |
| 第 32 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 32 轮 consistency | 警告 | 0 | 0 | 3 | 0 |
| 第 32 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 33 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 33 轮 consistency | 警告 | 0 | 0 | 1 | 0 |
| 第 33 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 34 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 34 轮 consistency | 警告 | 0 | 0 | 1 | 0 |
| 第 34 轮 completeness | 警告 | 0 | 1 | 1 | 1 |
| 第 35 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 35 轮 consistency | 通过 | 0 | 0 | 0 | 0 |
| 第 35 轮 completeness | 警告 | 0 | 1 | 2 | 0 |
| 第 36 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 36 轮 consistency | 警告 | 0 | 0 | 1 | 2 |
| 第 36 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 37 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 37 轮 consistency | 警告 | 0 | 0 | 3 | 1 |
| 第 37 轮 completeness | 警告 | 0 | 2 | 1 | 0 |
| 第 38 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 38 轮 consistency | 警告 | 0 | 1 | 2 | 0 |
| 第 38 轮 completeness | 警告 | 1 | 0 | 0 | 0 |
| 第 39 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 39 轮 consistency | 警告 | 0 | 1 | 1 | 0 |
| 第 39 轮 completeness | 警告 | 0 | 1 | 1 | 0 |
| 第 40 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 40 轮 consistency | 警告 | 0 | 0 | 2 | 0 |
| 第 40 轮 completeness | 警告 | 0 | 0 | 1 | 0 |
| 第 41 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 41 轮 consistency | 通过 | 0 | 0 | 0 | 0 |
| 第 41 轮 completeness | 警告 | 0 | 2 | 4 | 0 |
| 第 42 轮 accuracy | 警告 | 0 | 0 | 0 | 1 |
| 第 42 轮 consistency | 警告 | 0 | 0 | 11 | 0 |
| 第 42 轮 completeness | 警告 | 0 | 2 | 0 | 0 |
| 第 43 轮 accuracy | 警告 | 0 | 0 | 0 | 2 |
| 第 43 轮 consistency | 警告 | 0 | 1 | 2 | 0 |
| 第 43 轮 completeness | 警告 | 0 | 1 | 3 | 3 |
| 第 44 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 44 轮 consistency | 警告 | 0 | 1 | 2 | 0 |
| 第 44 轮 completeness | 警告 | 0 | 2 | 3 | 0 |
| 第 45 轮 accuracy | 警告 | 0 | 0 | 0 | 1 |
| 第 45 轮 consistency | 通过 | 0 | 0 | 0 | 0 |
| 第 45 轮 completeness | 警告 | 0 | 3 | 0 | 0 |
| 第 46 轮 accuracy | 警告 | 0 | 0 | 0 | 1 |
| 第 46 轮 consistency | 警告 | 0 | 0 | 4 | 0 |
| 第 46 轮 completeness | 警告 | 0 | 1 | 1 | 0 |
| 第 47 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 47 轮 consistency | 警告 | 0 | 1 | 2 | 0 |
| 第 47 轮 completeness | 警告 | 0 | 1 | 0 | 2 |
| 第 48 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 48 轮 consistency | 警告 | 0 | 2 | 1 | 0 |
| 第 48 轮 completeness | 警告 | 0 | 1 | 1 | 0 |
| 第 49 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 49 轮 consistency | 警告 | 0 | 2 | 2 | 1 |
| 第 49 轮 completeness | 警告 | 0 | 1 | 3 | 0 |
| 第 50 轮 accuracy | 通过 | 0 | 0 | 0 | 0 |
| 第 50 轮 consistency | 通过 | 0 | 0 | 0 | 0 |
| 第 50 轮 completeness | 警告 | 0 | 3 | 2 | 0 |

注：上表为各 reviewer 原始报告计数（含跨 reviewer 重叠）。下方 R1-R90 为去重合并后的可操作清单，按严重度分组、组内顺序编号；另含 2 项不修建议（无编号）。结论中的 "4 项 HIGH" = three-hat 报告的 1 项（R2 共享常量）∪ 第 5 轮 completeness 报告的 1 项（R1 ListVector 数据丢失）∪ 第 11 轮 completeness 报告的 1 项（R15 `getBoolean` null 静默返回 false）∪ 第 17 轮 completeness 报告的 1 项（R20 Date 编解码时区不对称）。第 5 轮另新增 R8（RetryException 分支无 deadline，MEDIUM），并将原 R8（deadline 溢出）扩展为 R11（同时覆盖 backoff 溢出）。第 10 轮新增 R9（`Row.json`/`prettyJson` 输出非法 JSON，MEDIUM）。第 11 轮新增 R15（`getBoolean` null 静默 false，HIGH）与 R16（`toString`/`mkString` 对 `Array[Byte]` 渲染为 `[B@...`，LOW）。第 12 轮新增 R17（`ArrowDeserializerSuite` writer 未走 try/finally，LOW）；另发现 `Column.lit` 非原语类型 toString 兜底——映射到 `code-review-todo.md` 已有 "不修复" 决策，归入下方"⚪ 不修建议"。第 15 轮新增 R18（`Thread.sleep` `InterruptedException` 后未恢复中断标志，MEDIUM）。第 16 轮新增 R19（`Row.fromSeqWithSchema` 不校验长度，JSON 静默丢字段 / IOOB，MEDIUM）。第 17 轮新增 R20（`DateType` 编解码时区不对称，non-UTC JVM 下 round-trip 漂移，HIGH）。第 18 轮新增 R21（`TimeStampNanoVector` 解码静默截至毫秒，MEDIUM）与 R22（`SparkSession.close` 不清理 `StreamingQueryListenerBus` + `append` 持锁阻塞 30s gRPC，LOW）；另重复发现 `Column.lit` toString 兜底（已在 ⚪ 不修建议）与 `tryCompressPlan` Disabled 写无锁（已在 ✅ 已验证清洁项节，状态机单向粘滞，无语义竞态），均不重复列入。第 19 轮新增 R23（`ArrowSerializer.DecimalVector` 写入路径不归一化 `BigDecimal` scale，与上游 `ArrowWriter.DecimalWriter` 偏离，常见用户输入 `Row(BigDecimal("99"))` against `DecimalType(10,2)` 抛 Arrow 内部 `UnsupportedOperationException`，MEDIUM）；另发现 R18 在 MEDIUM 桶内被列入 conclusion 的"4 项防御性"——R18 既具防御属性（恢复中断标志为 Java 并发 idiom）又有真实正确性影响（cooperative cancellation 链路），归类为跨桶项，保留现有结构不强行迁移。第 20 轮新增 R24（`ArrowSerializer.setArrowValue` 缺失 `DurationVector`/`IntervalYearVector`/`TimeMicroVector` 写分支，但 `sparkTypeToArrow` 已为 `DayTimeIntervalType`/`YearMonthIntervalType`/`TimeType` 映射出 Arrow 类型且 `ArrowDeserializer` 已能解码——`Encoders.DURATION` / `Encoders.PERIOD` 公共入口将在 catch-all 处抛 `UnsupportedOperationException("Unsupported Arrow vector type: ...DurationVector")`，MEDIUM）。第 21 轮新增 R25（`ArrowSerializer.MapVector` 不校验 null key——`sparkTypeToArrowField` 已声明 `keyField nullable=false`，但 `setArrowValue:160-163` 对 null 短路到 `vec.setNull(idx)`，对 keys 子向量 violate validity-bit 不变量，与上游 `extractKey` 调用 `Objects.requireNonNull(key)` 偏离，MEDIUM）。第 22 轮新增 R26（`ArrowDeserializer` 缺失 `TimeStampMilliTZVector`/`TimeStampSecTZVector`/`TimeStampNanoTZVector` 三个 TZ-bearing case，仅有 `TimeStampMicroTZVector` 与三个非 TZ 兄弟，其余落到 `getObject` 兜底返回 `Long`——schema 仍宣称 `TimestampType` 触发 `Row.getTimestamp` ClassCastException，MEDIUM）与 R27（`Row.copy()` 丢失 schema——`def copy(): Row = Row.fromSeq(toSeq)` 不传 `schema`，导致 `copy().json` / `copy().fieldIndex(name)` 等抛 `UnsupportedOperationException`，与 upstream `GenericRowWithSchema.copy()` 保留 schema 偏离，MEDIUM）；另 Round 22 完整性 reviewer 重报 ListVector 未支持元素类型静默写 null（已在 R1）与 `Row.json` 非 String 值产出非法 JSON（已在 R9）——均为已记录项，不重复列入。第 23 轮 completeness reviewer 报告 CLEAN（无新代码层发现，72 次工具调用），consistency reviewer 报告 2 项文档内部一致性问题：(a) 结论行 "4 项防御性建议" 漏列 R22（同在 LOW 桶且其 body 自标 "**价值**: 防御性 + 并发正确性"），下方修正为 5 项并标注其跨桶属性；(b) R5 与 R27 在 "`Row.copy()` 丢失 schema" 维度根因重叠——R5 把 copy-schema-drop 列为 `equals`/`getAs(name)` 不对称三联中的一环，R27 独立给出最小修复 `def copy(): Row = this`（因 `Row` 为 `final` + immutable）与 upstream `GenericRowWithSchema.copy()` 对照，下方两项已互相 cross-reference 而非合并，以保留 R5 更广语境与 R27 的最小修复路径。Round 23 不引入新编号项；accuracy reviewer 报告 PASS（spot-check R24/R25/R26/R27 + R2/R8/R14 共 7 项，逐一对照源代码 file:line 后判定无失实）。第 24 轮 accuracy / consistency / completeness 三项全清——首个完全 CLEAN 轮（连续零发现计数 0 → 1/10）。Round 24 completeness reviewer 系统审计了 4 项候选并均以可达性 / 设计意图为由拒绝列入：(a) `TupleEncoder2.fromRow`（`Encoders.scala:58-65`）缺失 TupleEncoder3/4/5 在单 struct 行场景下的 `row.getStruct(0)` unwrap——asymmetry 真实，但 `joinWith` 路径下 server 返回的是 2 字段 struct（`_1`/`_2`）而非 1-field wrapper，无可达 wire-format；(b) `AgnosticEncoderWrapper.fromRow`（`Encoders.scala:34-37`）无条件抛 `UnsupportedOperationException`——惯用路径走 `Encoder.given` 实例（行 39-85），wrapper 仅为 `Aggregator.bufferEncoder` 这类 proto-only 入口存在，属设计约束；(c) `ResponseValidator.wrapIterator.next()`（`ResponseValidator.scala:37-40`）未捕获 `INVALID_HANDLE.SESSION_CHANGED` 翻 `sessionValid` 标志，`verifyResponse`（行 25-26）有——asymmetry 真实，但 `isSessionValid` 在 `src/main` 全无消费者（grep 验证），write-only 实质零影响；(d) `GrpcExceptionConverter.errorsToException`（行 143-162）的 cycle detection 用 `errorIdx`（非 `causeIdx`）入 `visited` set——已 trace 互相 cycle / self-cycle 两场景，均正确终止。consistency reviewer 注线 455 "下一步" 表格 R4/R17/R18 独立追加列表未含 R22——cosmetic 非 contradiction（A-G 选项是显式封闭集合），不强制列入。第 25 轮 accuracy reviewer 报告 1 项 LOW 失实：R24 body 称 "TimeType 同理：解码端有 `case _: ArrowType.Time => ...`（如 round-trip 设计），编码端缺 `TimeMicroVector` 分支"——经源码核对，`ArrowDeserializer.arrowTypeToSparkType` 无 `ArrowType.Time` case（落到 line 228 默认 `case _ => StringType`），`extractValue:70-160` 亦无 `TimeMicroVector`/`TimeNanoVector`/`TimeSecVector`/`TimeMilliVector` case（落到 `getObject` 兜底），故 TimeType 实为编/解码两端均缺失而非编码端单边缺失；R24 body 已修订为区分 Duration/Interval（编码端单边缺失，解码端就绪）与 TimeType（两端均缺失，需同步补齐）。consistency reviewer Round 25 报告 PASS；completeness reviewer 新增 R28（`DerivedEncoder.fromRow` 通过 `extractField` 在非 Option 原语字段路径直接 `row.get(idx)`，server null 进入 Tuple 后由 Scala 3 case-class `fromProduct` 编译生成的 `BoxesRunTime.unbox*(null)` 静默 unbox 为零值，覆盖 `collect()` / `first()` / `head` / `tail` / `toLocalIterator` 全部 typed action，HIGH——与 R15 在原语 null 维度根因同源、修复点对称：R15 在 `Row.getXxx` accessor 抛 NPE，R28 应在 encoder field extraction 抛 NPE，两项落在不同 API 入口、保留独立条目并互相 cross-reference）；另审计了 4 项候选并拒绝列入：(a) `ArrowDeserializer.MapVector` 用 `.toMap` 对重复 key 取最后一个（stdlib 语义，非 HIGH）；(b) `LiteralValueProtoConverter` TIME nano 返回（reachability 极窄）；(c) `GrpcRetryHandler` backoff `pow.toLong` 截断（已在 R11 范围内）；(d) `ArtifactManager` CRC reset / buf reuse（已 inspection 验证正确）。conclusion 行 HIGH 计数 4 → 5；R15/R28 互引 cross-reference。第 26 轮 consistency reviewer 报告 2 项文档内部一致性问题：(a) "建议下一步"表格 option B 标签 "三项 HIGH" 与 conclusion 行 "5 项 HIGH" 直接矛盾——R20/R28 在两轮 HIGH 列入 conclusion 后未同步进入 option 进阶序列 A→F；HIGH 修订；(b) 同表下方 "可独立追加" 旁注仅列 R4/R17/R18，R20/R22/R28 及 Arrow 编解码补丁组 R21/R23-R27 缺失追加路径——MEDIUM 修订。已更新 option B 至 "R1 + R2 + R15 + R20 + R28（五项 HIGH——R28 与 R15 同源对称，建议合并落地）"，C/D/E 内联 "B +" 引用以保持递进关系；旁注扩列 R22 + Arrow 编解码补丁组（R21/R23-R27 可一并修以闭合 Arrow 路径）。accuracy reviewer Round 26 报告 PASS（spot-check 多项 file:line + 与源代码逐一比对，无失实）；completeness reviewer Round 26 新增 R29（`SparkSession.failPendingObservations` 跨查询观测污染，HIGH——`observationRegistry.values().forEach(failMetrics)` + 全量 `clear()` 在 session-scoped registry 上做无差别失败广播 + 全清空，`DataFrame.collect/tail` catch-all 在每条查询路径都调用此方法，并发观测路径下导致 (a) 不相关查询的 Observation Future 被错误异常完成、(b) 已成功 `processObservedMetrics` 的 query 在下游 catch 触发 `clear()` 时把其他正在进行的 query 的 observation 整体抹掉，promise 既不 fulfill 也不 fail，用户线程死锁——对照上游 `Observation` 与 query 一对一绑定语义偏离）；另审计了 8 项候选并均拒绝列入：(a) `ExecutePlanResponseReattachableIterator` 状态锁（已 inspection 验证 lock-coverage 完整）、(b) `SharedChannel` CAS（atomic 操作正确）、(c) `ArtifactManager` CRC reset / buf reuse（已在 Round 25 验证）、(d) `GrpcRetryHandler` backoff `pow.toLong` 截断（已在 R11 范围内）、(e) `SessionCleaner` Cleaner 注册（lifecycle 管理 idiom 正确）、(f) `StreamingQueryListenerBus` daemon thread（已在 R22 范围内）、(g) `Observation` promise 双重完成（promise.tryComplete 自身幂等）、(h) `LiteralValueProtoConverter` proto deprecated API（`-Wconf` 已显式静默，构建配置覆盖）。conclusion 行 HIGH 计数 5 → 6；R29 独立条目（与 R15/R28 不同族系——前两者是 null 语义、R29 是共享 registry 范围过大）。第 27 轮 accuracy reviewer 报告 PASS（spot-check R29 `SparkSession.scala:290-295` + `DataFrame.scala:347-349/665-667` / R28 `Encoder.scala:230-235/253-266` / R20 `ArrowSerializer:173-179` + `ArrowDeserializer:89-90` / R23 `ArrowSerializer:184-189` / R25 MapVector `:193-215` + schema `:131-143` 共 6 处 file:line，逐一对照源代码后判定无失实）；consistency reviewer 报告 3 项文档内部一致性问题：(a) "建议下一步"表格 option B 上界 190 与 R1(45)+R2(5)+R15(20)+R20(35)+R28(30)+R29(45)=180 不符（HIGH）——上轮添加 R29 时上界未同步收紧，已修订为 125-180；(b) options C/D/E/F 估时未传播 R29 +30-45 min（HIGH）——C 原 112-162 低于其超集 B 的下界 125 直接 strictly less than 自身超集，已重算 C=142-197、D=162-217（取 R7 选项 A 时 177-232）、E=192-262、F=194-264 以恢复递进单调性；(c) R20↔R21 cross-reference 单向（MEDIUM）——R21:258 已引用 R20，但 R20:96 仅引 R1 family，已在 R20 body 末附 **关联** 行双向引用 R21（同属 vector-specific 解码不对称族系，Date 路径 vs. Timestamp 子族）。completeness reviewer 报告 CLEAN（10 项审计候选均以可达性 / 设计意图 / 已覆盖为由拒绝列入：ConnectRepl `NonFatal` close、ArtifactManager 批量阈值、Catalog `escapeSqlLiteral` config 翻转、LiteralValueProtoConverter MAP `.toMap` dedup、SparkSession `activeSession` ThreadLocal API 限制、SparkConnectClient `paramMap.get("token")` case-sensitive、Observation `markRegistered` 防御性、SparkSession `cloneSession` 不绑 active、ArtifactManager `readNextChunk` 0-byte spin、vendored sketch 上游代码——全部 reject）；Round 27 不引入新编号项；连续零发现计数因 consistency 3 项找回保持 0/10。第 28 轮 accuracy reviewer 报告 PASS（spot-check R1/R3/R5/R7/R8/R10-R14/R16/R18/R19/R22 共 14 处 file:line + 与源代码逐一比对，无失实）；consistency reviewer 报告 1 项 MEDIUM：line 38 注段开头 "下方 R1-R27 为去重合并后的可操作清单" 范围标记 stale，未随 R28（Round 25 添加）/ R29（Round 26 添加）同步——已修订为 "R1-R29"，本轮新增 R30 后已二次修订为 "R1-R30"；completeness reviewer 新增 R30（`ArrowDeserializer.extractValue` `StructVector` 分支用 `Row.fromSeq` 构造嵌套 struct row 不传 schema，与同文件 `:55` 外层 `Row.fromSeqDirectWithSchema` 路径不对称——`df.collect().head.getStruct(0).getAs[String]("name")` / `.json` / `.fieldIndex` 等按字段名 / schema 的访问全部抛 `UnsupportedOperationException`，含 `StructType` 列的 DataFrame 是常见路径，MEDIUM）；另审计了 8 项候选并均拒绝列入：(a) `Column.lit(Any)` toString 兜底（已在"⚪ 不修建议"）、(b) `Observation.get()` `TimeoutException` cause 吞掉（防御性）、(c) `JsonEscaping` U+2028/U+2029 处理（验证后实际为正确处理）、(d) Logging stubs 未 gate（无调用方）、(e) `SparkConnectClient.parseUrl` 错误消息 echo（用户自身 malformed input，无 server 状态泄漏）、(f) `ArtifactManager.addArtifactsImpl` Await 超时不取消 gRPC（防御性，资源泄漏窗口有界）、(g) `GrpcExceptionConverter` 状态码映射（已在 R24 验证 cycle detection 正确）、(h) `GrpcRetryHandler` retry 边界（已在 R8/R11/R18 范围内）。conclusion 行 HIGH 计数维持 6（R30 为 MEDIUM）；R30 与 R5/R7/R27 区分 cross-reference（前三者围绕 `Row.copy()` 在外层丢 schema，R30 是 deserializer 在嵌套构造时单边丢 schema）；连续零发现计数因 consistency 1 项 + completeness 1 项保持 0/10。**第 28 轮后规则调整**：因 consistency reviewer 多轮发现的均为机械性 doc 自洽问题（添加新条目后表格 / 计数 / 范围标记未同步——R20↔R21 cross-reference、option 估时算术、R1-R27→R1-R30 stale 范围等），与新代码 bug 不同质，故收敛标准改为 "accuracy + completeness 两清即一轮干净，consistency 单独修但不重置计数器"。按新规回推：Round 23/24 = clean（计数 0→1→2），Round 25 因 accuracy LOW 失实 + completeness 新 R28 重置归零，Round 26 因 completeness 新 R29 维持 0，Round 27 = clean（0→1），Round 28 因 completeness 新 R30 重置归零。第 29 轮 accuracy reviewer 报告 PASS（spot-check 28/30 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实）；consistency reviewer 报告 2 项内部一致性问题：(a) R30 结构性归位错位（HIGH）——R30 body 自标 MEDIUM，但被遗留的 line 386 `---` 分隔符隔在 MEDIUM 闭合 `---` 与 LOW header 之间，视觉上不属任何严重度桶，已通过移除 orphan `---` 将 R30 收入 MEDIUM 段（接 R27 之后）；(b) R30 单向 cross-reference（MEDIUM）——R30 body 引用 R5/R7/R27 但三者均无反向引用，已为 R5/R7/R27 各加 "与 R30 区分" 关联行（R30 是 deserializer 嵌套 struct 行单边丢 schema 落点，R5/R27 是外层 `Row.copy()` 主动丢 schema，R7 是 `equals`/`hashCode` `Array[Byte]` 引用相等，三者根因与影响面不同）；completeness reviewer 报告 CLEAN（审计 17 个文件 + ~14 项候选并均以可达性 / 设计意图 / 已覆盖为由拒绝列入）。Round 29 不引入新编号项；按新规 accuracy + completeness 两清，计数 0 → 1/10。第 30 轮 accuracy reviewer 报告 PASS（spot-check R1/R2/R3/R4/R5/R7/R8/R11/R14/R15/R20/R21/R28/R29/R30 共 14 处 file:line + 与源代码逐一比对，无失实）；consistency reviewer 报告 PASS（HIGH=6 / MEDIUM=16 / LOW=8 = 30 总数与 R1-R30 一致；R5↔R27/R5↔R30/R7↔R30/R27↔R30/R15↔R28/R20↔R21 双向引用验证；options B-F 估时严格单调递增 125-180 ⊂ 142-197 ⊂ 162-217 ⊂ 192-262 ⊂ 194-264，无算术漂移；range marker R1-R30 / 日期标记 / severity bucket 划分均准确）；completeness reviewer 新增 3 项：R31（`Row.getList`/`getJavaMap` 在 null Seq/Map 列上构造包裹 null 的 wrapper，首次方法调用 NPE 进入 scala.jdk 内部栈，与 `getSeq`/`getMap` 返回 null 的 Scala 端契约不对称，HIGH——与 R15/R28 区分：前两者为原语 null silent unbox，本项为引用类型 null loud failure），R32（`arrowTypeToSparkType` 在 `ArrayType`/`MapType` 分支硬编码 `containsNull = true` / `valueContainsNull = true`，忽略 Arrow child field 的 `isNullable` 标记，与 Struct 分支正确传播 `child.isNullable` 不对称，schema round-trip 失真——下游基于 schema 推断 "保证非空" 的代码丢失保证，MEDIUM），R33（`ExecutePlanResponseReattachableIterator.next()` 仅检查 `closed` 不检查 `resultComplete`，与 `hasNext` 的双重 gate 不对称——直接调用 `next()` 而不先 `hasNext` 的代码在 ResultComplete 后再次 `next()` 可能触发非预期 reattach 或底层 gRPC 异常，错误信息非"iterator 已耗尽"语义，MEDIUM）；另审计 18+ 项候选并均拒绝（SparkConnectClient.parseUrl IPv6 / SharedChannel 中断恢复 / Tuple3-5 fromRow 已在 skip / Catalog escapeSqlLiteral 上游对齐 / TypedAggregators NaN 上游对齐 / SimpleJsonParser surrogate-pair 验证正确 / ArtifactManager numChunks 为文件级竞争超出库范围 / 等）。Round 30 引入 R31/R32/R33；conclusion 行 HIGH 计数 6 → 7（R31 为 HIGH），MEDIUM 计数 16 → 18（R32/R33 均 MEDIUM）；按新规 completeness 新增重置计数器，1/10 → 0/10。第 31 轮 accuracy reviewer 报告 PASS（spot-check R1/R2/R5/R8/R11/R15/R18/R20/R28/R30/R31/R32/R33 共 13 处 file:line + 与源代码逐一比对，无失实）；consistency reviewer 报告 2 项 MEDIUM 内部一致性问题：(a) "建议下一步"表格 option B 标签 "六项 HIGH" 与 conclusion 行 "7 项 HIGH" 直接矛盾——R31 在 Round 30 列入 conclusion 后未同步进入 option 进阶序列，已修订为 "八项 HIGH" 并将 R31 + R34（本轮新增 HIGH）纳入 option B 枚举与估时累加；(b) "可独立追加" 旁注未列 R31/R32/R33（R31 已纳入 option B HIGH 主链路；R32 与现有 Arrow 编解码补丁组同族系，已并入；R33 落 iterator 状态机 + R35 落 DataType DDL 保真，独立追加列出）；completeness reviewer 新增 2 项：R34（`functions.scala:2080-2083` `lambdaVar(name)` 构造 `UnresolvedNamedLambdaVariable` 时使用裸 name，无唯一后缀；`createLambda1`/`createLambda2` 是仅有的两个调用点，分别复用字面量 `"x"`/`"y"`；嵌套高阶函数如 `transform(arr, x => filter(x, y => y > x))` 或 `transform(... transform_keys(... transform(...)))` 链时，每层嵌套发出同名 `UnresolvedNamedLambdaVariable("x")`，server-side analyzer 要么将内层引用错误绑定到最内层 outer scope、要么直接抛 ambiguous-name 解析错误——上游 `spark-mine-12/sql/api/.../columnNodes.scala` 通过 `private val nextId = new AtomicLong(); s"${name}_${nextId.incrementAndGet()}"` 显式规避此问题，HIGH），R35（`DataType.scala:130-131` `StructType` 未 override `def sql`，继承 base trait `def sql = typeName.toUpperCase` 返回字面字符串 `"STRUCT"`——丢失字段；`StructType.toDDL`（`:176-181`）逐字段调用 `f.dataType.sql`，含嵌套 struct 的列 DDL 渲染为 `\`b\` STRUCT` 等不合法字符串无法 round-trip 经 `DataType.fromDDL` 解析；`MAP<STRING, STRUCT<...>>` / `ARRAY<STRUCT<...>>` 等外层复合类型递归传播，与同文件 `ArrayType:120` / `MapType:126` 的 `override def sql` 路径不对称，MEDIUM）；另审计候选并均拒绝列入（细节见 reviewer 报告：`SparkSession.setDefaultSession` 冗余 null check / `GrpcRetryHandler.RetryException` 无界即时重试为 server-driven 设计 / `Observation.markRegistered` AtomicBoolean 防御性 / `Catalog` getBoolean 已在 R15 / `arrowTypeToSparkType` 已在 R32 / `next()` `resultComplete` 已在 R33 等）。Round 31 引入 R34/R35；conclusion 行 HIGH 计数 7 → 8（R34 为 HIGH），MEDIUM 计数 18 → 19（R35 为 MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 32 轮 accuracy reviewer 报告 PASS（spot-check R1/R2/R8/R11/R15/R18/R20/R21/R23/R28/R29/R31/R32/R33/R34/R35 共 15 处 file:line + 与源代码逐一比对，无失实）；consistency reviewer 报告 3 项 MEDIUM 内部一致性问题：(a) R31↔R15/R28 反向引用缺失——R31 body 已引 R15/R28（"前两者为原语 null silent unbox，本项为引用类型 null loud failure"），但 R15:90 与 R28:123 仅互相引用未旁及 R31，已为 R15 与 R28 各加 R31 反向引用行，明确 silent vs. loud 失败模式区分；(b) R34↔R28 反向引用缺失——R34 body 已引 R28 集合（"functions.scala 客户端构造表达式缺失上游随机/唯一化注入"），但 R28:123 未反向 cite R34，已加 R34 反向引用（"expression-tree builder vs. wire-format encoder——同 typed Dataset 路径但失败入口不同"）；(c) R35↔R32 反向引用缺失——R35 body 已引 R32（schema-fidelity 族系），但 R32:492 未反向 cite R35，已加 R35 反向引用（"R35 是 schema → DDL 字符串渲染缺字段（公共 API 输出层），R32 是 Arrow round-trip 中 nullability 元信息失真（deserializer 内部）"）；completeness reviewer 新增 R36（`functions.rand`/`randn` 默认参数 `seed: Long = 0L` 致无参 `rand()` / `randn()` 始终展开为 `rand(0L)`/`randn(0L)`，不同查询产出相同伪随机序列——`df.sample(0.1)` 客户端多次得到固定子集 / ML noise 注入恒定 / `df.withColumn("a", rand()).withColumn("b", rand())` 两列完全相等 / A/B 划分不变；对照上游 `spark-mine-12/sql/api/.../functions.scala:2416-2452` 无参 `rand()` 路径用 `rand(SparkClassUtils.random.nextLong)`，`randn()` 同；注意 `random()` 路径走 `callFn("random")` 无 seed 参数无问题，MEDIUM——与 R34 同属 "functions.scala 客户端表达式构造缺失上游状态注入" 族系，R34 是命名 ID 唯一化缺失、R36 是 seed 随机化缺失，修复路径同构）；另审计候选并均拒绝列入。Round 32 引入 R36；conclusion 行 HIGH 计数维持 8（R36 为 MEDIUM），MEDIUM 计数 19 → 20（R36 为 MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 33 轮 accuracy reviewer 报告 PASS（spot-check 24 项 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实，覆盖 R1/R2/R8/R9/R11/R13/R14/R15/R18/R19/R20/R21/R23/R25/R26/R28/R29/R30/R31/R32/R33/R34/R35/R36，且双向引用 R5↔R27/R5↔R30/R7↔R30/R27↔R30/R15↔R28/R15↔R31/R28↔R31/R28↔R34/R20↔R21/R32↔R35/R34↔R36 全部交叉验证）；consistency reviewer 报告 1 项 MEDIUM：R36（Round 32 添加，MEDIUM）未列入 "可独立追加" 旁注，按 R33/R35 的同 round 处理模式应同步追加——已修订旁注，将 R36 与 R34 关系（同 functions.scala 族系，R34 在 option B HIGH 链路、R36 标 MEDIUM 独立追加）一并标注；completeness reviewer 新增 R37（`DataFrame.sample` / `DataFrame.randomSplit` / `Dataset.sample`(无 seed) / `Dataset.randomSplit`(无 seed) 默认 `seed=0L` 致无显式 seed 调用产出确定性子集——`DataFrame.scala:261, 621` 用 Scala 默认参数 `seed: Long = 0L`，`Dataset.scala:260-261, 266-267, 426-427` 五处包装层 `0L` 兜底，对照上游 `Dataset.scala:2079-2081` 无 seed 重载用 `SparkClassUtils.random.nextLong` 注入随机种子；可观测影响：A/B 划分稳定但实验失去随机性、ML cross-validation train/test split 跨执行不变、`df.sample(0.1).collect()` 多次得到固定行子集、ML pipeline 重训掩盖 split 敏感的 generalization 问题，MEDIUM——与 R36 同根因 "客户端表达式构造缺失上游随机状态注入"，仅 API 入口不同：R36 是 `functions.rand`/`randn` 两个入口，R37 是 `DataFrame`/`Dataset.sample`/`randomSplit` 五个入口共 7 处缺陷点同型修复，可与 R36 一并落地）；另审计候选并均拒绝列入（`SharedChannel.release` `awaitTermination` 不捕 InterruptedException 与 R18 同族 / `ExecutePlanResponseReattachableIterator.releaseAsync onError` 同步 blocking RPC 设计选择 / `stateLock` 实质死代码 / `LiteralValueProtoConverter.toDataType` ARRAY/MAP `containsNull = true` 与 R32 同维度 / `SparkSession.Builder.build()` pre-check + CAS 冗余但功能正确 / `Catalog.escapeSqlLiteral` 反斜杠引号处理无注入风险 / `Observation.get` row null 防御性 / `ArtifactManager.addBatchedArtifacts` CRC reset per-artifact 正确）。Round 33 引入 R37；conclusion 行 HIGH 计数维持 8（R37 为 MEDIUM），MEDIUM 计数 20 → 21（R37 为 MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 34 轮 accuracy reviewer 报告 PASS（spot-check R1/R2/R8/R11/R15/R20/R21/R23/R28/R29/R30/R31/R32/R33/R34/R35/R36/R37 共 18 处 file:line + 与源代码逐一比对，无失实，覆盖 cross-reference R5↔R27/R5↔R30/R7↔R30/R27↔R30/R15↔R28/R15↔R31/R28↔R31/R28↔R34/R20↔R21/R32↔R35/R34↔R36/R36↔R37 双向引用）；consistency reviewer 报告 1 项 MEDIUM：R37（Round 33 添加，MEDIUM）未列入 "可独立追加" 旁注，按 R36 的同 round 处理模式应同步追加——已在前一轮修订时一并追加，本轮再次确认；completeness reviewer 报告 4 项审计候选，逐一核对后 1 项拒绝、3 项纳入新编号：(a) [拒绝] `Column.lit` 对非原语类型走 `toString` 兜底——已在 ⚪ 不修建议（line 707）显式列入"保留"决策，曾在 Rounds 12/18/28 重复出现并均拒绝列入，本轮 reviewer 重报系不熟悉历史决策，按既有"保留"标记继续不入清单；(b) [接受] R38（`LiteralValueProtoConverter.toScalaValue` TIME 分支返回 `literal.getTime.getNano` 原始 `Long` 而非 `LocalTime`，`LiteralValueProtoConverter.scala:74-75`，曾在 Round 25 以"reachability 极窄"为由拒绝，本轮 reviewer 重新论证并发现新可达路径——`DataFrame.observe` 的 `processObservedMetrics`（`DataFrame.scala:1037`）逐条调用 `LiteralValueProtoConverter.toScalaValue(lit)` 把每个观测指标值材料化进 Row；用户调用 `df.observe("m", first(timeCol))` 时 server 返回 TIME 字面量，客户端把它当作 `Long` 放入 Row 但 schema 在 `toDataType` 路径仍宣称 `TimeType`，下游 `row.getLocalTime(0)` 抛 ClassCastException、`row.getAs[java.time.LocalTime]("m")` 同样失败、`row.json` 输出整数而非时间字面量——typed-API 误用 + 序列化失真两条用户可见路径；与 R20（DateType 编解码 TZ 不对称）、R26（TZ-bearing TimestampVector 解码缺 case）同属 vector-specific 解码不对称族系，但失败点不同：R20/R26 在 ArrowDeserializer，R38 在 LiteralValueProtoConverter（observe 与 retrieved-literal 路径），MEDIUM——比 R20/R26 reachability 更窄，但仍是用户可调用的公共 API 路径，且修复点单一明确）；(c) [接受] R39（`StreamingQueryListenerBus.registerServerSideListener` 在 `StreamingQueryListenerBus.scala:89` 落空 fallthrough——while 循环 `iterator.hasNext` 返回 false 时直接 `return iterator`，未抛出"未收到 ListenerBusListenerAdded 确认"异常，server 在发送确认前关闭流时 caller 误以为注册成功；后续 `executionThread` 在 `queryEventHandler:100` `iter.hasNext` 立即 false 退出 finally，listener 永不收到事件——silent registration failure，与 line 82-83 timeout 路径形成不对称（timeout 抛 TimeoutException，hasNext=false fallthrough 不抛），与 streaming 路径耦合 coverage 已排除（`build.sbt:142`），MEDIUM）；(d) [接受] R40（`StructType.treeString(maxLevel: Int)` 在 `DataType.scala:186-206` 缺失 `maxLevel <= 0` 兜底——上游 `spark-mine-12/sql/api/.../StructType.scala:386` 显式 `val depth = if (maxDepth > 0) maxDepth else Int.MaxValue` 把非正参数 reroute 到 MaxValue 等价 "show all"，SC3 的实现 `if indent <= maxLevel` 在 `maxLevel = 0` 时 `1 <= 0 = false` 短路掉所有字段，输出仅 `"root\n"`——与上游"非正即无限"惯例偏离；用户调用 `schema.treeString(0)` 期望"全部展开"会得到空骨架，LOW，display-only 影响）。Round 34 引入 R38/R39/R40；conclusion 行 HIGH 计数维持 8（R38/R39 均 MEDIUM、R40 LOW），MEDIUM 计数 21 → 23，LOW 计数 8 → 9；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 35 轮 accuracy reviewer 报告 PASS（spot-check 19 处 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实，覆盖 R1/R2/R5/R8/R15/R20/R21/R28/R29/R30/R31/R32/R33/R34/R35/R36/R37/R38/R39/R40，且双向引用 R5↔R27/R5↔R30/R7↔R30/R15↔R28/R15↔R31/R20↔R21/R28↔R34/R32↔R35/R34↔R36/R36↔R37/R38→R20/R26/R39→R22 全部交叉验证）；consistency reviewer 报告 PASS（HIGH=8 / MEDIUM=23 / LOW=9 = 40 总数与 R1-R40 一致；14 对双向引用全部双向；range marker R1-R40 与最高 R-编号一致；options B-F 估时严格单调递增 135-200 ⊂ 152-217 ⊂ 172-237 ⊂ 202-282 ⊂ 204-284；conclusion "8 项 HIGH" 与 HIGH 桶 R1+R2+R15+R20+R28+R29+R31+R34 = 8 计数一致；本轮 PASS 非 stale-doc 修正轮）；completeness reviewer 报告 3 项审计候选，逐一核对后全数纳入新编号：(a) [接受] R41（`GroupedDataFrame.agg(aggExpr: (String, String), aggExprs: (String, String)*)` 在 `GroupedDataFrame.scala:42` 把 `aggExpr +: aggExprs` 拼成 `Seq[(String, String)]` 后调用 `.toMap`——Scala stdlib `toMap` 对重复 key 取最后一个 value，将 `df.groupBy("dept").agg("salary" -> "max", "salary" -> "min")` 静默合并为 `agg("salary" -> "min")`（删去 max），用户无任何错误信号；对照上游 `RelationalGroupedDataset.scala:89-90` 用 `.map(toAggCol)` 直接 map 而非 toMap，重复 column 在 aggregate 列表中被保留独立——本项是公共 API 行为分歧，HIGH，与 R29 同属 SparkSession/GroupedDataFrame 公共 API 路径但根因不同：R29 是跨查询 observation 污染、R41 是单查询内重复聚合静默丢失）；(b) [接受] R42（`ArrowDeserializer.extractValue` `IntervalYearVector` 分支在 `:143-144` 返回 `v.get(index)` 即原始 `Int`，但 `arrowTypeToSparkType` 在 `:225` 对 `_: ArrowType.Interval` 映射到 `YearMonthIntervalType`——schema 与值类型异步：`row.get(0)` 返回 `java.lang.Integer` 而 schema 宣称 `YearMonthIntervalType`，typed accessor `row.getAs[java.time.Period](0)` 抛 ClassCastException；upstream `IntervalYearVectorReader.getPeriod(i)` 用 `vector.getObject(i).normalized()` 返回 `java.time.Period`；与 R26/R38 同属 vector-specific 解码 schema/值类型不对称族系，但落点在 `IntervalYearVector` 而非 `TimeStampVector`/TIME literal 路径，MEDIUM）；(c) [接受] R43（`LiteralValueProtoConverter.toScalaValue` `STRUCT` 分支在 `:54-57` 用 `Row.fromSeq(values)` 构造嵌套 struct row 不传 schema——`toDataType` 路径在 `:135` 对 `s.hasStructType` 正确返回 `StructType(...)`，外层 `processObservedMetrics`（`DataFrame.scala:1042`）用 `Row.fromSeqWithSchema` 包装顶层但内层嵌套 struct 仍为 schema-less Row；`df.observe("m", first(struct("a", "b").as("nested")))` 路径下用户 `obs.get()("m").asInstanceOf[Row].getAs[Int]("a")` 抛 `UnsupportedOperationException("getAs by field name requires a Row with schema")`；与 R30 同根因 "嵌套 struct 行解码丢 schema" 但落点不同：R30 在 `ArrowDeserializer.extractValue` `StructVector` 分支（Arrow 路径），R43 在 `LiteralValueProtoConverter.toScalaValue` `STRUCT` 分支（observe 路径）；两条解码链具同型 asymmetry，可同步修复，MEDIUM）；另审计 ~15 项候选并均拒绝列入（`Logging.logInfo`/`logWarning`/`logError` 无调用方 / `Artifact.LocalFile` TOCTOU 超出库范围 / `JsonEscaping` U+2028/U+2029 处理验证后正确 / `SimpleJsonParser` 嵌套 escape 防御性 / `Encoder.given` primitive `fromRow` 已在 R15 族 / `DerivedEncoder.toRow` 不在 wire 路径低可达 / `cloneSession` 复用 stub 设计 / `SharedChannel.release` 已在 R18 族 / `StreamingQueryListenerBus.close` `case _` 防御性 / `DataFrame.jdbc` connectionProperties 覆盖 url/dbtable 设计语义 / `treeString` ArrayType/MapType 嵌套缩进 display-only / `Row.getAs(name)` + `.copy()` 链已在 R5/R27 / `KeyValueGroupedDataset.keyAs` `asInstanceOf` 类型擦除 idiom / `Logging.scala:31-34` `Log.debug` 设计语义 / `DataFrameNaFunctions.replace` `Map[T, T]` dedup 上游对齐）。Round 35 引入 R41/R42/R43；conclusion 行 HIGH 计数 8 → 9（R41 为 HIGH），MEDIUM 计数 23 → 25（R42/R43 均 MEDIUM），LOW 维持 9；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 36 轮 accuracy reviewer 报告 PASS（spot-check 22 处 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实，覆盖 R1/R2/R5/R8/R11/R15/R20/R21/R28/R29/R30/R31/R32/R33/R34/R35/R36/R37/R38/R39/R41/R42/R43，且双向引用 R5↔R27/R5↔R30/R7↔R30/R15↔R28/R15↔R31/R20↔R21/R28↔R34/R32↔R35/R34↔R36/R36↔R37/R38→R20/R26/R39→R22/R30↔R43/R26/R38↔R42 全部交叉验证）；consistency reviewer 报告 1 项 MEDIUM + 2 项 LOW 内部一致性问题：(a) R19（`Row.fromSeqWithSchema` 不校验长度，JSON 静默丢字段 / IOOB，MEDIUM）未列入"建议下一步"旁注的可独立追加列表，按 R33/R35/R36 同 round 添加模式应同步追加——已修订旁注，将 R19 与现有 MEDIUM 项一并追加；(b/c) 两项 LOW 为 cosmetic 表述微调（无规范性矛盾、不影响 R-item 严重度或修复路径），不强制处理；按新规 consistency 不重置计数器；completeness reviewer 新增 R44（`LiteralValueProtoConverter.toScalaValue` `CALENDAR_INTERVAL`/`YEAR_MONTH_INTERVAL`/`DAY_TIME_INTERVAL` 三分支在 `:58-64` 返回原始 `(Int,Int,Long)` tuple / `Int` / `Long`，但 `toDataType` 路径在 `:140-142` 把这些 literal 类型声明为 `CalendarIntervalType`/`YearMonthIntervalType`/`DayTimeIntervalType`——schema 与值类型异步：observe 路径 `df.observe("m", first(intervalCol))` 后 `obs.get()("m").asInstanceOf[Row].getAs[java.time.Period]("ymInterval")` 抛 ClassCastException；upstream `SparkIntervalUtils.microsToDuration` / `monthsToPeriod` / `new CalendarInterval` 构造正确 JSR-310 / `CalendarInterval` 对象；与 R38（同方法 TIME 分支返回 raw `Long`）/ R43（同方法 STRUCT 分支构造 schema-less Row）形成 LiteralValueProtoConverter 类型契约族系完整闭环，与 R42（`ArrowDeserializer.IntervalYearVector` 分支返回 raw Int）同属 INTERVAL 类型解码 schema/值不对称族系但落点不同（ArrowDeserializer 路径 vs. LiteralValueProtoConverter observe 路径），MEDIUM）；另审计 ~6 项候选并均拒绝列入（`ConnectRepl` `NonFatal` close 防御性 / `ArtifactManager` 0-byte spin 上游对齐 / `SparkSession.cloneSession` 设计语义 / `Catalog.escapeSqlLiteral` 上游对齐 / `Logging` stubs 无调用方 / `JsonEscaping` 验证后正确）。Round 36 引入 R44；conclusion 行 HIGH 计数维持 9（R44 为 MEDIUM），MEDIUM 计数 25 → 26，LOW 维持 9；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 37 轮 accuracy reviewer 报告 PASS（spot-check 24 处 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实，覆盖 R1/R2/R5/R8/R11/R15/R20/R21/R28/R29/R30/R31/R32/R33/R34/R35/R36/R37/R38/R39/R41/R42/R43/R44，且 14+ 对双向引用 R5↔R27/R5↔R30/R7↔R30/R15↔R28/R15↔R31/R20↔R21/R28↔R31/R28↔R34/R32↔R35/R34↔R36/R36↔R37/R30↔R43/R38↔R42/R38↔R44/R42↔R44/R43↔R44 全部交叉验证）；consistency reviewer 报告 3 项 MEDIUM + 1 项 LOW 内部一致性问题：(a) R44↔R38 cross-reference 单向——R44 body（line 831）已引 R38，但 R38 body（line 690）原文未反向 cite R44，已为 R38 末尾追加 "Round 36 新增 R44 同方法对 INTERVAL 三分支同型缺陷，可与本项一并修复"；(b) R44↔R43 cross-reference 单向——R44 body 已引 R43，但 R43 body（line 795）原文未反向 cite R44，已加 R44 反向引用；(c) R44↔R42 cross-reference 单向——R44 body 已引 R42，但 R42 body（line 755）原文仅引 R26/R38 未及 R44，已加 R44 反向引用；(d) [LOW] cosmetic 表述微调，不强制处理；按新规 consistency 不重置计数器；completeness reviewer 新增 3 项：R45（`ResponseValidator.wrapIterator` 在 `ResponseValidator.scala:32-41` 提取 session ID 但未把 `iter.next()` 包在 `verifyResponse` 提供的 `try/catch` 中——`StatusRuntimeException` carrying `INVALID_HANDLE.SESSION_CHANGED` 在 streaming RPC 路径上发生时不会触发 `sessionValid = false`，与同文件 line 21-29 的 `verifyResponse` 处理 unary RPC 的对称行为偏离；具体路径：`ExecutePlan` / `ReattachExecute` 等流式响应在中段抛 `INVALID_HANDLE.SESSION_CHANGED` 时，`isSessionValid` 仍返回 true，下游 `SparkConnectClient.execute` 后续调用以为 session 仍可用，重试 / 后续命令在错误的失效 session 上发起，HIGH——失败模式与 unary 路径 sessionValid 标志一致，但流式路径完全跳过保护，与上游 ResponseValidator 行为偏离），R46（`ExecutePlanResponseReattachableIterator.callIter` 在 `:124-131` 捕获 `OPERATION_NOT_FOUND` / `SESSION_NOT_FOUND` 后无条件 `iter = rawExecutePlan(request)`——若已通过 `lastReturnedResponseId` 消费过部分响应，silent 重新执行原 plan 可能产出重复行 / 状态不一致；upstream `spark-mine-12/sql/connect/common/.../ExecutePlanResponseReattachableIterator.scala:244-254` 显式：`if (lastReturnedResponseId.isDefined) throw new IllegalStateException("OPERATION_NOT_FOUND/SESSION_NOT_FOUND on the server but responses were already received from it.", ex)` 抛错使外层重试逻辑感知到 partial-consumption 不一致；SC3 缺这层保护，HIGH——partial-consumption silent re-execute 是数据正确性问题，与 R45 同属 client retry/session 路径但落点不同），R47（`Catalog.getCreateTableString(name, asSerde=true)` 在 `Catalog.scala:159-161` 构建 `s"SHOW CREATE TABLE AS SERDE \`$name\`"`——但 SQL 语法 `SqlBaseParser.g4:334` 为 `SHOW CREATE TABLE identifierReference (AS SERDE)?`，关键字 `AS SERDE` 必须**跟在表名之后**而非之前，server 直接 parse 失败抛 ParseException，`asSerde=true` 路径完全不可达；SC3 实现把关键字放反，与 grammar 直接矛盾，MEDIUM——公共 API 在 `asSerde=true` 路径全失效）；另审计候选并均拒绝列入。Round 37 引入 R45/R46/R47；conclusion 行 HIGH 计数 9 → 11（R45/R46 均 HIGH），MEDIUM 计数 26 → 27（R47 为 MEDIUM），LOW 维持 9；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 38 轮 accuracy reviewer 报告 PASS；consistency reviewer 报告 3 项内部一致性问题：(a) R45/R46 hallucinated cross-references to R23/R24/R35（三者根因均与 R45/R46 不同——R23 是 `ArrowSerializer.DecimalVector` BigDecimal scale 归一化、R24 是 `ArrowSerializer.setArrowValue` 缺 vector 分支、R35 是 `StructType.sql` override 缺失），已修订移除并替换为 R45 是 ResponseValidator 路径首个 R 项 / R46 与 R45 同 client 子包 SparkConnectClient retry-session-failure 族系的 honest sibling 描述；(b) Options table C-F 估时打破单调性——B 上界因 R45/R46 添加上调到 175-255 后，C 仍为 162-232 导致 strictly less than 自身超集；已重算 C=192-272 / D=212-292 / E=242-337 / F=244-339 恢复 monotonicity；(c) MEDIUM 旁注未显式列入 R30，仅在 R43 描述中括号引用——已显式补加 R30 条目（与 R43 同根因不同落点，10-15 min）；completeness reviewer 新增 R48（CRITICAL，`DataFrame.union` / `unionByName` 默认 `isAll=false` 致服务端套 `Distinct`，行为退化为 `UNION` 而非 `UNION ALL`——`DataFrame.scala:227-228` `union` 与 `:232-233` `unionByName` 调用 `setOp` 不传 `isAll`，`setOp` 默认形参 `isAll: Boolean = false` 写入 `SetOperation.setIsAll(false)`，server-side `SparkConnectPlanner` 对 `SET_OP_TYPE_UNION + isAll=false` 显式 `logical.Distinct(union)` 包裹；客户端 `df.union(df).count() == df.distinct().count()` 而非预期 `2 * df.count()`，typed `Dataset.union/unionAll/unionByName` 三入口均委托 `df.union/unionByName` 同因受影响——数据正确性级偏离，与 SQL 文本路径 `UNION ALL` 行为分歧；对照上游 `spark-mine-12/sql/connect/common/.../Dataset.scala:665-676` 显式 `setIsAll(true)`，`intersectAll`/`exceptAll` 在本仓正确传 `isAll=true`，仅 union 三入口偏离——是单点偏离而非 setOp 系列整体偏离；测试 `DataFrameIntegrationSuite.scala:103-109` 用不相交输入 `range(0,3) union range(3,6)`，去重 / 不去重均产 6 行 → 缺陷不可观测）。Round 38 引入 R48；conclusion 行 CRITICAL 计数 0 → 1；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 39 轮 accuracy reviewer 报告 PASS（spot-check 历史 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实）；consistency reviewer 报告 2 项内部一致性问题：(a) [HIGH] R48 cross-reference 单向——R29/R41 两条 HIGH 同族系条目未反向引用 R48，已为 R29 / R41 各加 R48 反向引用行（"客户端 API 路径静默语义偏离" 族系闭环）；(b) [MEDIUM] reviewer-counts 表缺 Round 38 三行——已补齐；按新规 consistency 不重置计数器；completeness reviewer 新增 2 项：R49（`KeyValueGroupedDataset.cogroup` / `cogroupSorted` 在 `:263-302` / `:310-353` 不使用 `mapValues` 派生状态——`originalRelation` / `originalValueEncoder` / `mapValuesFunc` 三者仅 `flatMapGroups`（`:140-183`）路径正确读取并 compose `MapValuesFlatMapAdaptor`，cogroup 系列直接用当前 `ds.df.relation` + 当前 `valueEncoder`（已是 W 而非原始 V）+ raw `func`，导致 `ds.groupByKey(_.id).mapValues(_.amount).cogroup(other.groupByKey(_.id))((k, vs, ws) => ...)` 调用链下 `vs` 类型与用户期望偏离——预期为 `Iterator[Double]`（mapValues 后的 W），实际收到 server 端按 `ds.df.relation` 与 V (而非 mapped W) groupBy 的迭代器；与上游 `KeyValueGroupedDataset.scala` `cogroup` / `cogroupSorted` 通过 `MapValuesCoGroupAdaptor` / `UDFAdaptors.coGroupWithMappedValues` 显式 compose 偏离，HIGH——同 mapValues 派生状态在不同入口的 dispatch 不一致，flatMapGroups 正确而 cogroup 系列遗漏，是公共 typed API 的静默正确性问题），R50（`DataFrameWriter.jdbc` 在 `:134-141` 仅用 `format("jdbc").option(...).save()` 直通用户 partitionBy/bucketBy/clusterBy 状态进 proto，缺上游 `assertNotPartitioned("jdbc")` / `assertNotBucketed("jdbc")` / `assertNotClustered("jdbc")` 三重护栏与 `validatePartitioning()` 检查；用户 `df.write.partitionBy("a").bucketBy(4, "b").jdbc(url, table, props)` 调用链下 partitionCols / numBuckets / bucketColNames / sortColNames / clusteringCols 全部静默写入 proto——server-side `SparkConnectPlanner` 解析 jdbc relation 时忽略这些字段，用户预期"按列分区写出"实际未生效；上游 `spark-mine-12/sql/api/.../DataFrameWriter.scala:327-329 + :462/472/478/487` 在 `jdbc` 入口先 assert 拒绝、不允许此组合，MEDIUM——客户端缺 fail-fast 验证致用户误以为生效，是 silent contract violation）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 40 轮 accuracy reviewer 报告 PASS（spot-check 历史 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实）；consistency reviewer 报告 2 项 MEDIUM 内部一致性问题：(a) R49 cross-reference 反向引用缺失——R36/R37 body 已注 R49 同 typed Dataset / functions.scala 客户端表达式构造分歧族系，但反向引用未补；已为 R36/R37 各加 R49 反向引用行（"客户端表达式构造层与上游分歧" 大类，R49 落点在 typed Dataset API dispatch 完整性而非 functions.scala seed 随机化注入）；(b) R50 cross-reference 反向引用缺失——R29/R41/R47/R48 同 "客户端 API 路径静默语义偏离" / "silent contract violation" 族系，但 R50 添加后四条上反向引用未补；已为 R29/R41/R47/R48 各加 R50 反向引用行（与既有族系区分 R50 落点在 sink format + partition/bucket/cluster 校验缺失）；按新规 consistency 不重置计数器；completeness reviewer 新增 R51（`ArtifactManager.addArtifactsImpl` 在 `:123-161` 用 inline `StreamObserver[AddArtifactsResponse]` 直接调 `asyncStub.addArtifacts(responseHandler)` 注册 server 流响应——既未走 `responseValidator.wrapIterator` 包裹，也未在 `onNext` 中校验 `v.getSessionId` 与本地 `sessionId` 是否一致；上游 `spark-mine-12/sql/connect/common/.../ArtifactManager.scala:315-318` 显式 `if (SparkStringUtils.isNotEmpty(v.getSessionId) && v.getSessionId != sessionId) throw new IllegalStateException(...)` ——服务端在该 RPC 路径上发生 session swap 时静默接受；与 R45 `wrapIterator.next()` 漏 SESSION_CHANGED catch 同属客户端 session/retry 防护族系但落点不同，与 R29 跨查询 observation 污染不同根因——R45 是 catch 漏写、R51 是单 RPC 路径上 sessionId mismatch guard 完全缺失，MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 41 轮 accuracy reviewer 报告 PASS（spot-check 历史 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实）；consistency reviewer 报告 PASS（HIGH/MEDIUM/LOW 桶计数与 conclusion 一致，cross-reference 双向闭合，options table 估时 monotonic，无 stale doc 项）；completeness reviewer 新增 6 项：R52（`Catalog.listColumns` / `tableExists` / `functionExists` 在 `Catalog.scala:78,180,190` 形参顺序为 `(tableName, dbName)` / `(tableName, dbName)` / `(functionName, dbName)`，与上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala:146,247,275` `(dbName, tableName)` / `(dbName, functionName)` 顺序相反——用户从上游迁移代码 `catalog.tableExists("mydb", "users")` 在 SC3 上被反向解读为 `tableName="mydb"` / `dbName="users"`，server 抛 NoSuchTableException 或更隐蔽：若 `mydb` 与 `users` 都恰好是合法 identifier，server 在 `users` 数据库下找名为 `mydb` 的表——silent argument-swap，与 R47 `getCreateTableString` `AS SERDE` 关键字位置错放属同一 Catalog 公共 API 静默契约违反族系，HIGH——typed signature 用户无法察觉），R53（`DataFrameNaFunctions.replace` 在 `:98-113` 单列重载未对 `col == "*"` 做特殊处理（应转为"对所有列做替换"语义）且 `Map[T, T]` 中数值类型未做 `convertToDouble` 归一化——上游 `spark-mine-12/sql/connect/common/.../DataFrameNaFunctions.scala:111-114` 显式 `val cols = if (col != "*") Some(Seq(col)) else None` + `buildReplacement` 中 `convertToDouble` 把 `Int`/`Long`/`Float` 统一为 `Double`，SC3 用户调用 `na.replace("*", Map(0 -> 1))` 抛 AnalysisException（`*` 不是合法列名），调用 `na.replace("a", Map(1 -> 2))` 时 server 类型不匹配（schema 为 `LongType` 而 literal 为 `Int`）抛错——public API 与上游契约偏离，MEDIUM），R54（`StreamingQueryManager.get(id)` 在 `:27-31` 无条件 `result.getQuery` 不做 `hasQuery` 检查——上游 `spark-mine-12/sql/connect/common/.../StreamingQueryManager.scala` 显式 `if (response.hasQuery) { ... } else { null }`，SC3 在不存在的 query id 路径下返回伪造 `StreamingQuery(session, "", "", None)` 对象，用户后续调用 `q.id` / `q.runId` / `q.status` 在 server 端遇到 OPERATION_NOT_FOUND 抛远程异常，错误信息非"该 id 不存在"语义；与 R39 `StreamingQueryListenerBus.registerServerSideListener` silent registration failure 同属 streaming 公共 API silent fallthrough 族系，落点不同：R39 在 listener 注册路径，R54 在 query lookup 路径，MEDIUM），R55（`DataStreamReader.textFile(path)` 在 `:75` 返回 `DataFrame` 且未调用 `assertNoSpecifiedSchema("textFile")`——上游 `spark-mine-12/sql/api/.../streaming/DataStreamReader.scala:284-285` 返回 `Dataset[String]` 且强制 `assertNoSpecifiedSchema("textFile")`，SC3 用户从上游迁移 `val ds: Dataset[String] = ssr.textFile(...)` 编译失败（类型不兼容）；且 `ssr.schema(customSchema).textFile(...)` 在 SC3 静默接受 schema 而上游显式拒绝，public API 返回类型契约 + 校验语义双重偏离，HIGH——typed signature 用户必须改代码），R56（`UDFRegistration` 缺 `registerJava(name, className, returnDataType)` 方法——上游 `spark-mine-12/sql/connect/common/.../UDFRegistration.scala:47-55` 提供 `override def registerJava(...)` 构建 `JavaUdfBuilder` 注册 Java UDF，SC3 仅有 Scala `register(name, udf)` 与 inline `register[R, A1...]` arity 重载，Java/PySpark 互操作场景下用户无法注册 Java 类；与 Spark Connect Java 客户端 API surface 偏离，MEDIUM——public API 缺失而非缺陷），R57（`StreamingQuery.recentProgress` / `lastProgress` / `exception` 在 `:50,56,79` 返回 `Seq[String]` / `Option[String]` / `Option[String]`——上游 `spark-mine-12/sql/api/.../streaming/StreamingQuery.scala:79,95,102` 返回 typed `Array[StreamingQueryProgress]` / `StreamingQueryProgress` / `Option[StreamingQueryException]`，SC3 用户从上游迁移 `q.recentProgress.head.numInputRows` 编译失败（String 无 numInputRows 方法），需自行 JSON 解析；public typed API 契约偏离，与 R22/R39 streaming 路径族系同源不同落点（R22 是 listener bus close + lock，R39 是 register fallthrough，R57 是返回类型 typed→raw string），MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 42 轮 accuracy reviewer 报告 1 项 LOW 失实：R56 body 引用的 SC3 现状代码 snippet 把 `register[R, A1, ...]` arity 重载示意为 `def register(...): Unit`——核对 `src/main/scala/org/apache/spark/sql/UDFRegistration.scala` 后实际签名为 `def register(...): UserDefinedFunction`，已修订 R56 snippet 返回类型 `Unit → UserDefinedFunction`，对结论与族系归属无影响；consistency reviewer 报告 11 项 MEDIUM 内部一致性问题——R52/R53/R55/R56/R57 五条本轮上一轮新增 HIGH/MEDIUM 条目的 cross-reference 反向引用全部缺失，已为 R47（getCreateTableString AS SERDE 关键字位置）⟷R52 / R39（StreamingQueryListenerBus 注册 fallthrough）⟷R54（StreamingQueryManager.get 不查 hasQuery）/ R22⟷R57 / R55⟷R57（streaming 路径族系闭环）/ R52⟷R55⟷R56⟷R53⟷R57（"public API 与上游契约偏离/缺失" family 全连通）共 11 条反向引用补齐；按新规 consistency 不重置计数器；completeness reviewer 新增 2 项：R58（`RuntimeConfig.get(key: String): String` 在 `SparkSession.scala:498` 通过 `client.getConfigOption(key).getOrElse("")`-equivalent 路径——`SparkConnectClient.getConfig:271-273` 上对 server 返回的 `pairsList.headOption.map(_.getValue).getOrElse("")` 静默返回空字符串而非抛 `NoSuchElementException`，对照上游 `spark-mine-12/sql/connect/common/.../RuntimeConfig.scala:48-52` 显式 `@throws[NoSuchElementException]("if the key is not set and there is no default value")` + `getOption(key).getOrElse { throw new NoSuchElementException(key) }`——SC3 用户从上游迁移 `try { conf.get("spark.unknown") } catch { case _: NoSuchElementException => default }` 路径下捕获不到异常，得到空字符串后续 `.toInt` 抛 NumberFormatException 错误信息非 "key not set" 语义，且 `conf.get` 与 `conf.getOption` 行为完全等价——上游本意是让用户用 `getOption` 表达可空、`get` 表达必须存在，SC3 把二者合并致 typed contract 区分丢失，HIGH——public API @throws 契约偏离 + typed 路径区分破坏），R59（`Catalog.getTable(tableName, dbName)` / `getFunction(functionName, dbName)` 在 `Catalog.scala:141-148, 150-157` 形参顺序 `(tableName, dbName)` / `(functionName, dbName)` 与上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala:181, 211` 的 `(dbName, tableName)` / `(dbName, functionName)` 反向，silent argument-swap——`@deprecated` 重载在上游用于 4.x 路径迁移，SC3 直接落地为反向参数顺序，用户从上游迁移 `catalog.getTable("mydb", "users")` 在 SC3 被反向解读为 `tableName="mydb"` / `dbName="users"`，server 端 `mydb` 不是数据库下的表抛 NoSuchTableException 或更隐蔽：`mydb` 与 `users` 都是合法 identifier 时 server 在 `users` 数据库下找名为 `mydb` 的表——typed signature 用户无法察觉，HIGH——R52 同族系 Catalog public API 形参顺序静默反转，本轮新增使该族系在 5 个 site `Catalog.scala:78,141,151,180,190` 上完整闭环：`listColumns/getTable/getFunction/tableExists/functionExists` 全部参数顺序与上游相反）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 43 轮 accuracy reviewer 报告 2 项 LOW 失实：(a) R59 body 引用的 site 列表 `Catalog.scala:78,141,151,180,190` 中 `:151` 与 `:180,:190` 三处源自 R52 而非 R59 自身（R59 仅覆盖 `getTable:141` / `getFunction:151` 两个 site），已修订 R59 body 显式区分"R59 自身落点"（141/151）与"R52 同族系落点"（78/180/190）；(b) R58 body 末尾断言 `get` 与 `getOption` 在 "key 不存在" 路径上"完全等价"——上游 `RuntimeConfig.get` 默认值重载 `def get(key: String, default: String): String` 在 SC3 中确实存在但 `default == null` 路径会触发 NPE-equivalent，与 `getOption.getOrElse(default)` 在 default=null 时返回 null 不完全等价；表述已软化为"在常用 'key 不存在 + 无默认值' 调用路径上观察等价"。consistency reviewer 报告 1 项 HIGH + 2 项 MEDIUM：(a) [HIGH C1] R52/R55/R56/R58/R59 同族系（"public API 与上游契约偏离/缺失"）的反向引用闭环不完整——R47⟷R52、R39⟷R54、R22⟷R57、R55⟷R57、R52⟷R55⟷R56⟷R53⟷R57 已在 Round 42 闭合，但 R58/R59 添加后未向 R52/R55/R47/R56/R57 五条同族系条目反向追加 Round 43 cross-reference paragraph，已为 R52（line 574）/ R55（line 606）/ R47（line 1299）/ R56（line 1569）/ R57（line 1627）各加 Round 43 段落，明确 R58 落点 SparkSession `RuntimeConfig` `@throws` 契约与 R59 完成 Catalog 5-site 形参顺序闭环；(b) [MEDIUM C2] 同 (a) 落点扩展，将 R58/R59 与 R55+R56+R53+R57 的 family 关系（"public API 与上游 typed/throws 契约偏离" 大类）显式注入 R52/R55 的 Round 43 段落而非仅作为 R58/R59 自身的孤立条目；(c) [MEDIUM C3 — cosmetic, deferred] "建议下一步"表格 option B 标签 "十六项 HIGH" 与 conclusion 行 "16 项 HIGH" 相符，但本轮 R60 添加为 HIGH 后需同步上调到 17 项，已在结论与 option B 同步至 "17 项 HIGH"，C3 收编为本轮 bookkeeping 一部分；按新规 consistency 不重置计数器；completeness reviewer 新增 7 项 + 1 项重新审计后拒绝列入：[拒绝] `TupleEncoder2.fromRow`（`Encoders.scala:58-65`）单 struct 行 `row.getStruct(0)` unwrap 与 TupleEncoder3/4/5 不对称——曾在 Round 24 (a) 候选审计中以 "joinWith server 返回 2 字段 struct 而非 1-field wrapper、无可达 wire-format" 为由拒绝列入，本轮 reviewer 重新论证未提供新可达路径，按既有决策继续不入清单；[接受] R60（`Catalog.listDatabases`/`listTables`/`listFunctions`/`getDatabase`/`getTable`/`getFunction` 全部返回 raw `DataFrame` 而非 typed `Dataset[Database]`/`Dataset[Table]`/`Dataset[Function]`/`Database`/`Table`/`Function`——`Catalog.scala:52-94, 131-154` 上 6 个公共 API 入口共 12+ method overload 全部返回 `DataFrame`，与上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala:56,72,155,168,194` `Dataset[Database]`/`Dataset[Table]`/`Dataset[Function]` typed 返回偏离；用户从上游迁移 `catalog.listTables.where($"name" === "x").as[Table].collect()` 在 SC3 上 `as[Table]` 抛 ClassCastException 或编译失败，`getTable("x")` 返回 `DataFrame` 后用户必须 `.as[Table].head` 而非直接 typed object 访问 `.name` / `.database` 字段——public typed API 大面积偏离，HIGH——与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]` 同根因不同 surface，与 R52/R59 同 Catalog 子系但落点正交：R52/R59 是参数顺序静默反转、R60 是返回类型从 typed DTO 退化为 raw `DataFrame`）；[接受] R61（`StorageLevel` 缺 `DISK_ONLY_3`/`MEMORY_ONLY_SER_2`/`MEMORY_AND_DISK_SER_2` 三个常量——`StorageLevel.scala:24-34` 仅提供 10 个常量，对照上游 `common/utils/.../storage/StorageLevel.scala:149-161` 13 个常量；用户调用 `df.persist(StorageLevel.DISK_ONLY_3)` / `df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)` 等替换=3 副本 / 序列化+副本组合编译失败，必须 fall back 到手工构造 `StorageLevel(useDisk=true, replication=3, ...)`——public API 缺失，MEDIUM）；[接受] R62（`Observation.get` 在 `Observation.scala:43,85` 用 `Await.result(future, ObservationTimeout)` 其中 `private val ObservationTimeout: Duration = Duration(10, "minutes")`——上游 `spark-mine-12/sql/api/.../Observation.scala:155` `Await.result(promise.future, Duration.Inf)`，长时运行 batch / 慢 stage / observe 后 cluster 资源紧张时用户 `obs.get` 在 10 分钟时抛 `TimeoutException` 而非阻塞至 future 完成，与上游"无限等待至 query 完成"语义偏离，MEDIUM）；[接受] R63（`Observation.get` 在 `Observation.scala:49-53` `row.schema = None` 路径下静默 fall back `Map.empty`——成功 metric 已写入但 schema 丢失致用户误判为"无指标"，与 R30/R43/R44 同根 schema 丢失族系但落点在 observe 入口且仅为 cosmetic 表象（值已正确传递、仅 schema 元信息缺失），LOW）；[接受] R64（`ArrowSerializer` 在 `ArrowSerializer.scala:172,234` 两处用字符串 `"UTF-8"` 而非 `StandardCharsets.UTF_8` 调 `String.getBytes`——隐式声明 checked `UnsupportedEncodingException` 但 UTF-8 在所有 JVM 上保证可用所以分支永不触发，且 idiom 与 Java 11+ 推荐相反——cosmetic LOW，5 min）；[接受] R65（`StreamingQuery.status` 在 `StreamingQuery.scala:63-65` 直接返回 raw proto `StreamingQueryCommandResult.StatusResult`——public API 直接泄漏 proto 类型给用户代码，与上游 `spark-mine-12/sql/api/.../streaming/StreamingQuery.scala` 返回 typed `StreamingQueryStatus` 偏离；用户从上游迁移 `q.status.message` / `q.status.isDataAvailable` 在 SC3 上字段名虽碰巧一致但类型实为 proto getter `getStatusMessage` / `getIsDataAvailable`，typed pipeline `case class S(msg: String); val s = S(q.status.message)` 在 proto getter 返回 `String` 路径恰好可用但 typed 升级为 `q.exception.map(_.cause).getOrElse(..)` 等典型用法时直接失败，与 R57 同 streaming 路径 typed 契约偏离族系但落点不同：R57 在 `recentProgress`/`lastProgress`/`exception` 三 getter，R65 在 `status` getter，MEDIUM）；[接受] R66（`getPlanCompressionOptions` 在 `SparkConnectClient.scala:113-137` 把 `getConfig("spark.connect.session.planCache.threshold").toInt` 的 `NumberFormatException` 留给 `case e if NonFatal(e)` 兜底——错误 config 触发的 transient `NonFatal` 留 state `Uninitialized` 致每个 plan 提交都重做 `getConfig` RPC + `.toInt` 失败 + retry，永远不会缓存为 `Disabled`/`Enabled`；用户错配 `spark.connect.session.planCache.threshold=invalid` 后每条 plan 路径上重复 RPC 与构造异常，与 R66 同 `getConfig` 路径上的 transient classification 偏离——`NumberFormatException` 是 user-config 错误应分类为 permanent + 缓存 `Disabled`，LOW）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 44 轮 accuracy reviewer 报告 PASS（spot-check 历史 R-条目逐一对照源代码 file:line / 代码描述 / cross-reference，无失实）；consistency reviewer 报告 1 项 HIGH + 2 项 MEDIUM：(a) [HIGH] options B 标签 / 估时未随 R67/R68 同步——已修订 "十七项 HIGH" → "十九项 HIGH"，估时 300-440 → 345-510 min（R67 +30-45 min / R68 +15-25 min），级联 C/D/E/F 同步上调到 362-527 / 382-547 / 412-592 / 414-594；(b) [MEDIUM] R55→R67/R68 与 R57→R65/R67 反向引用缺失——已为 R55 / R57 各加 Round 44 段落，明确 R67 是 typed-Dataset MERGE DSL 在 `mergeInto` 入口丢类型参数 / R68 是 batch reader `textFile` 同型缺陷扩展自 streaming reader R55；(c) [MEDIUM] cosmetic doc 表述微调，不强制处理；按新规 consistency 不重置计数器；completeness reviewer 新增 3 项：R69（`options(java.util.Map[String, String])` Java-interop 重载在 5 个 reader/writer 入口完全缺失——`DataFrameReader.scala:34` / `DataFrameWriter.scala:49` / `DataStreamReader.scala:31` / `DataStreamWriter.scala:66` / `DataFrameWriterV2.scala:35` 仅有 Scala `Map[String, String]` 重载，Java/PySpark 互操作场景下用户 `reader.options(javaProps)` 编译失败——与 R56 `UDFRegistration.registerJava` 同 "Java-interop 公共 API 缺失" 族系但落点不同：R56 在 UDF 注册路径，R69 在 reader/writer options 入口共 5 site，MEDIUM），R70（`GroupedDataFrame.mean`/`avg`/`max`/`min`/`sum` 在空 `colNames*` 入口下静默返回原始 df——`GroupedDataFrame.scala:77-93` 用 `if cols.isEmpty then df else agg(cols.head, cols.tail*)` 兜底；上游 `RelationalGroupedDataset.scala:60-64` `aggregateNumericColumns` 通过 `selectNumericColumns(colNames)` 自动 fall back 到 schema 中所有数值列；用户 `df.groupBy("dept").mean()` 调用预期得到 "对所有数值列求均值" 实际得到原始 df——与 R41 `GroupedDataFrame.agg .toMap` 重复 key 静默丢失同 GroupedDataFrame 公共 API 行为偏离族系，落点正交，MEDIUM），R71（`DataFrame.join(right, usingColumn: String, joinType: String)` 三参单列重载完全缺失——`DataFrame.scala:110-149` 只有 5 个 join 重载，上游 `spark-mine-12/sql/api/.../Dataset.scala:713` 显式定义 `def join(right: Dataset[_], usingColumn: String, joinType: String): DataFrame` 三参单列形式；用户从上游迁移 `df1.join(df2, "id", "left_outer")` 编译失败必须改写为 `df1.join(df2, Seq("id"), "left_outer")`——与 R56 / R61 / R69 同 "公共 API 完全缺失" 族系，落点正交，MEDIUM）；按新规 completeness 新增重置计数器，0/10 维持 0/10。第 45 轮 accuracy reviewer 报告 1 项 LOW 失实：R70 problem 段引用源文件 `sql/api/src/main/scala/org/apache/spark/sql/RelationalGroupedDataset.scala:60-64,176`，核对上游后实际在 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/RelationalGroupedDataset.scala:77-84` 的 Connect 路径上 `selectNumericColumns(colNames) = colNames.map(df.col)` 仅做平凡 column 映射，并显式注释"connect 与 classic 行为不同——auto-numeric 自动 fallback 仅在 server-side classic（`sql/core/.../classic/RelationalGroupedDataset.scala:111-126`）实现"；R70 body 已修订重新定位上游对照点为 Connect 路径，并明确 SC3 `if cols.isEmpty then df` 短路与上游 Connect `colNames.map(df.col)`（空列表场景下产出仅 grouping cols 的 empty-agg DataFrame）的行为差异本质在 client-side empty-cols 兜底逻辑而非 auto-numeric 缺失；consistency reviewer 报告 PASS（HIGH/MEDIUM/LOW 桶计数与 conclusion 一致，cross-reference 双向闭合，options table 估时 monotonic）；completeness reviewer 新增 3 项：R72（`Catalog.createTable(tableName, source, description, schema, options)` 5-arg 重载在 `Catalog.scala:250-256` 形参顺序为 `(tableName, source, description, schema, options)`，与上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala:486-490` `(tableName, source, schema, description, options)` 顺序相反——`description` 与 `schema` 两形参反转致 silent argument-swap：用户从上游迁移 `catalog.createTable("tbl", "parquet", schema, "my desc", opts)` 在 SC3 上被反向解读为 `description=schema.toString` / `schema=opts(?)`——四参重载 `:242-249` 内部 `createTable(tableName, source, "", schema, options)` 与本身一致但反向调用上游 5-arg 形态时 description 与 schema 位置错对换，HIGH——typed signature `(String, String, String, StructType, Map)` vs `(String, String, StructType, String, Map)` 用户编译时只有当传 String literal 给 description 才会触发 type mismatch 编译失败，传 typed `description: String` 变量与 `schema: StructType` 时编译通过但 server 收到错位字段；与 R52/R59 同族系第三个站点：R52 闭合 `listColumns/tableExists/functionExists`、R59 闭合 `getTable/getFunction`、R72 闭合 `createTable` 5-arg 重载，6 个 site `Catalog.scala:78,141,151,180,190,250` 完整闭环），R73（`DataFrameWriter.insertInto(tableName)` 在 `DataFrameWriter.scala:98-114` 硬编码 `setMode(WriteOperation.SaveMode.SAVE_MODE_APPEND)` 无条件覆盖用户先前 `mode("overwrite")` 调用——上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/DataFrameWriter.scala:136-145` `insertInto` 路径不调 `setMode`，让 server 端按 `INSERT INTO` 语义自身决策（`INSERT INTO ... OVERWRITE` 与 `INSERT INTO ... APPEND` 由 saveMode 字段透传决定）；用户调用链 `df.write.mode("overwrite").insertInto("tbl")` 在 SC3 上 mode 被静默 downgrade 至 APPEND，server 端执行普通 append 而非 overwrite——数据正确性级偏离（用户预期清空表后插入，实际只追加），HIGH——与 R50 `DataFrameWriter.jdbc` partitionBy/bucketBy/clusterBy silent contract violation 同 DataFrameWriter 公共 API 静默语义偏离族系：R50 是入口校验缺失致 partition 状态被忽略、R73 是 mode override 致用户 mode 调用被忽略，两者均为 silent contract violation 但失败模式不同），R74（`DataStreamWriter.outputMode(streaming.OutputMode)` 在 `DataStreamWriter.scala:47-48` typed 重载直接 `this.outputMode(outputMode.toString)` 转发不调 `.toLowerCase(Locale.ROOT)`——上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/DataStreamWriter.scala:51-54` 显式 `sinkBuilder.setOutputMode(outputMode.toString.toLowerCase(Locale.ROOT))`；server 端 `InternalOutputModes.apply` 用 strict-match `"append"`/`"complete"`/`"update"` 三个全小写常量，`OutputMode.Append.toString == "Append"`（驼峰）直传致 server 抛 `IllegalArgumentException("Unknown output mode Append. Accepted output modes are 'append', 'complete', 'update'")`——typed 重载 `df.writeStream.outputMode(OutputMode.Append).start(...)` 直接 streaming-startup failure，与 String 重载 `outputMode("append")` 行为不一致：String 入口若用户传小写则可工作，但 typed 入口完全失效，HIGH——新族系：streaming 写出路径 wire-protocol casing 偏离，与 R55/R57/R65/R67/R68 streaming/typed-return 族系正交但同 streaming 子系）。按新规 completeness 新增重置计数器，0/10 维持 0/10。


**结论**：1 项 CRITICAL（R48 `DataFrame.union` / `unionByName` 默认 `isAll=false` 致服务端套 `Distinct`，行为退化为 `UNION` 而非 `UNION ALL`，数据正确性级偏离；typed `Dataset.union/unionAll/unionByName` 三入口同因受影响）。29 项 HIGH（R1 `ArrowDeserializer.ListVector` 元素类型回落 `null` 致 `Array<Int>` 等数据丢失；R2 `ArrowSerializer`/`ArrowDeserializer` `MAX_BATCH_SIZE` 上下游硬编码常量未共享致序列化预算与反序列化阈值漂移；R20 `DateType` 编解码 TZ 不对称致 non-UTC JVM 下 `df.collect()` round-trip 漂移一日——三项数据丢失/腐蚀；1 项设计建议；1 项 null 语义静默 + 1 项 typed encoder 静默 unbox 零值——R15/R28 同源对称：R15 修 Row 直接 accessor 入口，R28 修 typed Dataset 的 `as[CaseClass].collect()` 入口；1 项 SparkSession 跨查询 observation 污染——R29 `failPendingObservations` 在 session-scoped registry 上做无差别失败广播 + 全量 clear；1 项 Java-interop 公共 API 在 nullable 字段下 NPE——R31 `Row.getList`/`getJavaMap`；1 项嵌套高阶函数 lambda 变量名碰撞——R34 `functions.lambdaVar`；1 项 `GroupedDataFrame.agg` 重复 column 静默合并——R41；R45 `ResponseValidator.wrapIterator` 流式路径未捕获 `INVALID_HANDLE.SESSION_CHANGED`；R46 `ExecutePlanResponseReattachableIterator.callIter` partial-consumption silent re-execute；1 项新增 R49 `KeyValueGroupedDataset.cogroup`/`cogroupSorted` 忽略 mapValues 派生状态致 typed cogroup 路径值类型不一致；R52 `Catalog.listColumns`/`tableExists`/`functionExists` 形参顺序与上游相反致 silent argument-swap；R55 `DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]` 且无 `assertNoSpecifiedSchema` 校验；R58 `RuntimeConfig.get(key)` 在 key 不存在且无默认值时不抛 `NoSuchElementException` 静默返回 `""`，与上游 `@throws[NoSuchElementException]` 契约偏离；R59 `Catalog.getTable`/`getFunction` 形参顺序为 `(tableName, dbName)` / `(functionName, dbName)`，与上游 `(dbName, tableName)` / `(dbName, functionName)` 反向，silent argument-swap——R52 同族系 Catalog public API 形参顺序静默反转；R60 `Catalog.listDatabases`/`listTables`/`listFunctions`/`getDatabase`/`getTable`/`getFunction` 全部返回 raw `DataFrame` 而非 typed `Dataset[Database]`/`Dataset[Table]`/`Dataset[Function]`/`Database`/`Table`/`Function`，与 R55 同 typed-return 契约退化族系，落点在 Catalog 6 个公共 API 入口共 12+ method overload；R67 `Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` 而非 `MergeIntoWriter[T]` 致 typed-Dataset 的 MERGE DSL 链上 `T` 在第一步即丢失，`whenMatched`/`whenNotMatched`/`whenNotMatchedBySource` 全部回落到 `Row` 视图致用户从上游迁移类型驱动 schema evolution 代码无法编译——与 R55/R57/R60/R65/R68 同 typed-return 契约退化族系；R68 `DataFrameReader.textFile(path)` / `textFile(paths*)` 返回 `DataFrame` 而非 `Dataset[String]` 且缺 `assertNoSpecifiedSchema("textFile")` 前置校验——与 R55 streaming reader 同型缺陷扩展到 batch reader 入口；R72 `Catalog.createTable` 5-arg 重载形参顺序 `(tableName, source, description, schema, options)` 与上游 `(tableName, source, schema, description, options)` 反向，silent argument-swap，闭合 R52/R59 family 第三个站点使 Catalog 形参顺序族系在 6 个 site 完整闭环；R73 `DataFrameWriter.insertInto` 硬编码 `setMode(SAVE_MODE_APPEND)` 无条件覆盖用户先前 `mode("overwrite")` 调用，与上游不调 `setMode` 让 server 自身决策偏离致数据正确性级 silent contract violation，R50 同族系扩展；R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载不调 `.toLowerCase(Locale.ROOT)` 致 server `InternalOutputModes.apply` strict-match 失败抛 `Unknown output mode Append`，typed 重载 streaming-startup 直接失败，新族系 streaming 写出路径 wire-protocol casing 偏离；R75 `DataStreamWriter.foreach(writer)` 总用 `UnboundRowEncoder` 序列化 `ForeachWriterPacket` 致 typed `Dataset[T].writeStream.foreach(writer: ForeachWriter[T])` 在 server 端按 `Row` 反序列化抛运行时 `ClassCastException`，根因为 `DataStreamWriter` 类缺 `[T]` 类型参数无从拿 dataset encoder——与 R74 同 `DataStreamWriter` 文件双 site 闭环（outputMode + foreach），与 R55/R67/R68 同 typed-DSL 契约偏离族；R77 `SparkSession.range(end)` / `range(start, end, step)` / `range(start, end, step, numPartitions)` 全部返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载——与 R55/R57/R60/R65/R67/R68/R75 同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口致 `spark.range(10).map(x => x * 2)` typed pipeline 编译失败；R80 `SparkSession.Builder.interceptor(ClientInterceptor)` 方法完全缺失 + `SparkConnectClient.create` 不接受 interceptor 列表参数——企业用户从上游迁移 `SparkSession.builder.remote(uri).interceptor(authInterceptor).build()` 在 SC3 上编译失败，无法注入观测/认证/计费 gRPC client interceptor，与 R56/R69/R71/R78/R79 Java-interop 公共 API 缺失族系同根因不同 surface：R56/R78/R79 是 stat/na/UDF Java-friendly 重载缺失（lower-stakes 互操作）、R80 是 SparkSession 顶层入口的 ClientInterceptor 注入路径完全断链（高价值企业可观测性/鉴权/审计要素）；R82 `DataFrameReader.table(tableName)` / `DataStreamReader.table(tableName)` 静默丢弃用户已通过 `option(...)` 设置的 reader-level options——`DataFrameReader.scala:66` `def table(tableName) = session.table(tableName)` 完全跳过 `opts` 与 `assertNoSpecifiedSchema` 检查、`DataStreamReader.scala:78-89` 构建 `NamedTable` 时不调 `putAllOptions(opts)`；用户调用链 `spark.readStream.format("kafka").option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", "10000").table("my_kafka_table")` 在 SC3 上 `startingOffsets`/`maxOffsetsPerTrigger` 全部静默丢弃，server 端按 default options 启动 streaming source 致用户从 latest 而非 earliest 开始消费、无 rate limit——production streaming pipeline 数据丢失级 silent contract violation；与 R55/R68 同 reader 入口公共 API silent contract violation 族系，落点正交：R55/R68 是返回类型契约偏离、R82 是 options 状态丢失）；R86 `Dataset[T].writeTo(table)` 返回 untyped `DataFrameWriterV2`——`Dataset.scala:649` 直接调 `df.writeTo(table)` 丢 T，`DataFrameWriterV2` 类签名 `:15` 缺 `[T]` 类型参数，与 R55/R57/R60/R65/R67/R68/R75/R77 同 typed-return 契约退化族系，落点扩展到 V2 writer 路径致用户从上游迁移类型驱动 schema evolution `df.as[Event].writeTo("t").overwritePartitions()` 编译失败；R87 `Encoders` 对象工厂入口 `bean(Class[T])` / `kryo[T]` / `javaSerialization[T]` / `row(StructType)` 4 个完全缺失——`Encoders.scala:1-280` 仅含 primitive/String/Decimal/Date/Timestamp + parameterless `row` 工厂，对照上游 `spark-mine-12/sql/api/.../Encoders.scala:218/238/251/264` 4 个公共 API 入口缺失致用户无法注册 Java POJO / fallback Kryo / 显式构造 schema-less Row encoder——与 R56/R69/R71/R78/R79/R80 Java-interop 公共 API 缺失族系同根因不同 surface，落点在 typed encoder 入口 4 个 site 集中缺失；R88 `Encoder[T]` / `Dataset[T]` / `Row` 三类均未 `extends Serializable`——`Encoder.scala:15` / `Dataset.scala:37` / `Row.scala:12` 全部裸定义，对照上游 `spark-mine-12/sql/api/.../Encoder.scala:69` / `Dataset.scala:125` / `Row.scala:144` 显式 `extends Serializable`，致用户在 `Aggregator[IN, BUF, OUT]` 子类中将 `Encoder[BUF]` / `Dataset[T]` 作为 instance 字段时 server-side `KryoSerializer` / `JavaSerializer` 序列化路径抛 `NotSerializableException`——新族系：missing-Serializable 结构性 typing 维度，破坏 Spark 集群分发 closure 的核心约束，三类核心 typed API 同型缺陷可一并修复。无主动安全漏洞，5 项防御性建议（R10 token redact / R11 算术钳制 / R17 writer try-finally / R18 中断标志恢复 / R22 streaming bus 关闭 + `append` 锁内 30s gRPC——跨防御性 ∩ 并发正确性桶）。

---

## 按 ROI 排序的可操作清单

### 🔴 CRITICAL

#### R48. `DataFrame.union` / `unionByName` 默认 `isAll=false` 致服务端套 `Distinct`，行为退化为 `UNION` 而非 `UNION ALL`（typed `Dataset.union/unionAll/unionByName` 三入口同因受影响——数据正确性级偏离）
- **文件**:
  - `src/main/scala/org/apache/spark/sql/DataFrame.scala:227-228` — `union` 调用 `setOp` 不传 `isAll`
  - `src/main/scala/org/apache/spark/sql/DataFrame.scala:232-233` — `unionByName` 调用 `setOp` 不传 `isAll`
  - `src/main/scala/org/apache/spark/sql/DataFrame.scala:1170-1186` — `setOp` 默认形参 `isAll: Boolean = false`，`SetOperation.setIsAll(isAll)` 直接写入 wire
  - `src/main/scala/org/apache/spark/sql/Dataset.scala:499, 501, 503-504` — typed `Dataset.union` / `unionAll` / `unionByName` 委托 `df.union/unionByName`，继承同一缺陷
- **问题**: 客户端构造 `SetOperation` proto 时把 `isAll` 留空（默认 false）：
  ```scala
  // DataFrame.scala:227-228
  def union(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = false)

  // DataFrame.scala:232-233
  def unionByName(other: DataFrame, allowMissingColumns: Boolean = false): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = true, allowMissingColumns)

  // DataFrame.scala:1170-1186
  private def setOp(
      other: DataFrame,
      opType: SetOperation.SetOpType,
      byName: Boolean,
      allowMissingColumns: Boolean = false,
      isAll: Boolean = false                        // ← 默认 false 透传到 wire
  ): DataFrame =
    withRelation(_.setSetOp(
      SetOperation.newBuilder()
        ...
        .setIsAll(isAll)                            // ← false 写入 SetOperation
        ...
    ))
  ```
  服务端 `spark-mine-12/sql/connect/server/src/main/scala/org/apache/spark/sql/connect/planner/SparkConnectPlanner.scala:2403-2412` 对 `SET_OP_TYPE_UNION + isAll=false` 显式套 `logical.Distinct(union)`：
  ```scala
  case proto.SetOperation.SetOpType.SET_OP_TYPE_UNION =>
    val union = if (rel.getByName) Union(...) else Union(left, right)
    if (rel.getIsAll) union else logical.Distinct(union)
  ```
  结果：`df.union(other)` 在客户端命名为 "union"（按 SQL/Spark 约定 `Dataset.union` 文档表述为 "返回包含两个 Dataset 行的合并（不去重）"，等价于 `UNION ALL`），实际产出去重后的 UNION——`df.union(df).count() == df.distinct().count()`，而非预期的 `2 * df.count()`。`df.unionAll(other)` 由于内部直接转发 `union(other)`（line 230：`def unionAll(other: DataFrame): DataFrame = union(other)`），`unionAll` 入口同样去重，与方法名直接矛盾。
  对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/Dataset.scala:665-676`：
  ```scala
  def union(other: sql.Dataset[T]): Dataset[T] =
    buildSetOp(other, SET_OP_TYPE_UNION) { builder => builder.setIsAll(true) }
  def unionByName(other: sql.Dataset[T], allowMissingColumns: Boolean): Dataset[T] =
    buildSetOp(other, SET_OP_TYPE_UNION) { builder =>
      builder.setByName(true).setIsAll(true).setAllowMissingColumns(allowMissingColumns)
    }
  ```
  upstream 显式 `setIsAll(true)`。同时 upstream `intersect` / `except`（行 681-702）显式 `setIsAll(false)`、`intersectAll` / `exceptAll` 显式 `setIsAll(true)`——与 SC3 本仓 `intersectAll`（`DataFrame.scala:321-322`，`isAll=true`）/ `exceptAll`（行 324-325，`isAll=true`）/ `intersect`（line 235-236，走默认 `isAll=false`）/ `except`（line 238-239，走默认 `isAll=false`）的语义一致。**唯独 `union` / `unionByName` / `unionAll` 三个 union 入口语义偏离**，是单点偏离而非 setOp 系列整体偏离。
- **可观测影响**:
  - `df.union(df).count()` 返回 `df.distinct().count()`，而非 `2 * df.count()`
  - `df1.union(df2)` 在两 DataFrame 行有重叠时静默丢失重复行（数据正确性级偏离）
  - 与 SQL 路径 `spark.sql("SELECT * FROM df1 UNION ALL SELECT * FROM df2")` 行为不一致——`spark.sql` 路径正确给 UNION ALL，client API 路径给 UNION
  - 性能影响：服务端无谓地为每次 `union` 加 `Distinct` 算子，触发 shuffle / 排序，对大表 union 的查询规划与执行成本显著放大
  - typed `Dataset.union` / `unionAll` / `unionByName`（`Dataset.scala:499/501/503-504`）三入口均委托 `df.union/unionByName`，覆盖整条 typed 与 untyped API 链
- **测试为何未发现**: `src/test/scala/org/apache/spark/sql/DataFrameIntegrationSuite.scala:103-109` 用 `range(0,3) union range(3,6)` 两个**不相交**输入测试，去重结果（6 行）与未去重结果（6 行）相同——bug 在该测试下不可观测。要触发需 `df1.union(df1)` 或两 DataFrame 有重叠数据。
- **建议**: `DataFrame.scala:227-228` 与 `:232-233` 显式传 `isAll = true`：
  ```scala
  def union(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = false, isAll = true)

  def unionByName(other: DataFrame, allowMissingColumns: Boolean = false): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = true,
          allowMissingColumns, isAll = true)
  ```
  `intersect`（`:235-236`）/ `except`（`:238-239`）保留默认 `isAll=false`（语义正确，与 SQL `INTERSECT` / `EXCEPT` 默认去重一致，且与上游 `Dataset.scala:681-702` 行为一致）。同步补回归：`DataFrameIntegrationSuite.scala:103-109` 改用包含重叠的输入（如 `range(0,3) union range(2,5)`）以防回归；新增专用 `df.union(df).count() == 2 * df.count()` 断言锁住"UNION ALL 不去重"语义；新增 `unionByName` 同语义断言；新增 typed `Dataset.union` / `unionAll` / `unionByName` 三入口对应断言。
- **预估**: 10-15 min（两行 `isAll=true` + 5 项回归断言 + IntegrationSuite 输入调整 + typed Dataset 入口断言）
- **价值**: CRITICAL。`Dataset.union` 是 Spark 公共 API 中最常用的几条之一，"union 不去重" 是 SQL/Spark 用户根深蒂固的预期；当前 SC3 客户端给出的是与方法名直接矛盾的去重语义，且对 typed `Dataset[T]` 路径无差别影响——属于数据正确性级偏离，且与上游、与 SQL 文本路径行为不一致。Round 38 completeness 维度新增。
- **关联**: 与 R29（`SparkSession.failPendingObservations` 跨查询观测污染）、R41（`GroupedDataFrame.agg` `.toMap` 重复 key 静默丢失）同属 "客户端 API 路径静默语义偏离"族系——R29 在跨查询广播路径偏离、R41 在单查询内重复 key 路径偏离、R48 在 setOp `isAll` 默认值路径偏离，三者落点正交但同根：客户端构造请求时未显式覆盖 proto 默认值导致服务端按缺省语义执行、与上游 / 与方法名 / 与 SQL 路径产生分歧。Round 38 completeness 维度新增。Round 40 新增 R50（`DataFrameWriter.jdbc` 缺 `assertNotPartitioned`/`assertNotBucketed`/`assertNotClustered` + `validatePartitioning()` 四重客户端校验致 `partitionBy`/`bucketBy`/`clusterBy` + jdbc 组合 silent contract violation，MEDIUM）与本项同属 "客户端 API 路径静默语义偏离" 大类——R48 是 proto 默认值未覆盖致 setOp 套 Distinct，R50 是参数组合校验缺失致 partition/bucket/cluster 状态被 server 静默丢弃，两者均为客户端构造请求路径上的 silent contract violation；Round 40 consistency 反向引用补齐。

---

### 🔴 HIGH

#### R1. `ArrowSerializer` ListVector 对未支持元素类型静默写 null（数据丢失风险）
- **文件**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:222-240`
- **问题**: `writeItem` lambda 仅匹配 Int / Long / Double / Float / Short / Byte / Boolean / String，其余落入 `case _ => listWriter.writeNull()`。当 schema 声明为 `ArrayType(DateType)` / `ArrayType(TimestampType)` / `ArrayType(DecimalType)` / `ArrayType(BinaryType)` 时，元素被静默写为 null——schema 看似匹配、序列化也不抛错，但 round-trip 后元素全部丢失。`ArrowRoundTripSuite` 仅覆盖 `ArrayType[Int]` 与 `ArrayType[String]`（行 184、196），未触达此路径。
- **建议**: 二选一：
  - A. 递归调用 `setArrowValue` 复用顶层 type-aware 写入路径（首选）
  - B. 将 `case _` 改为 `throw UnsupportedOperationException(s"ListVector element type not supported: ${item.getClass}")`，并补 `ArrayType[Date|Timestamp|BigDecimal|Array[Byte]]` 的 property 测试
- **预估**: 30-45 min（含测试）
- **价值**: 消除静默数据丢失。当前是 schema 通过但 payload 丢失，调试代价远高于显式失败。

#### R2. 抽取 `MaxAllocatorBytes` 共享常量
- **文件**:
  - `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:33`
  - `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:25`
- **问题**: `256L * 1024 * 1024 * 1024` 在两处分别定义，未来调整易不同步。
- **建议**: 抽到共享 object，例如：
  ```scala
  // org/apache/spark/sql/internal/ArrowAllocators.scala
  private[sql] object ArrowAllocators:
    val MaxAllocatorBytes: Long = 256L * 1024 * 1024 * 1024
  ```
- **预估**: 5 min
- **价值**: 单一事实源，README "Memory & Resource Limits" 段落与代码对齐。

#### R15. `Row.getBoolean` 对 `null` 静默返回 `false`，与其余 primitive 取数器不对称
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:29-41`
- **问题**: `getBoolean(i): Boolean = get(i).asInstanceOf[Boolean]`。Scala 规范下 `null.asInstanceOf[Boolean] == false`（primitive 取默认值）；调用者无法区分 "字段为 false" 与 "字段为 null"。`getByte/Short/Int/Long/Float/Double` 走 `asInstanceOf[Number].byteValue()` 路径，`null.asInstanceOf[Number]` 为 `null`，再调 `.byteValue()` 抛 NPE——行为与 `getBoolean` 不一致。upstream Spark 通过 `getAnyValAs` 统一在 `isNullAt(i)` 时抛 `NullPointerException(s"Value at index $i is null")`。
  当前 `RowSuite.scala:16` 仅测试 `row.getBoolean(3)` 在值为 `true` 时返回，未触发 null 分支，缺陷无回归保护。
- **建议**: 在每个 primitive 取数器入口加 `if isNullAt(i) then throw NullPointerException(s"Value at index $i is null")`；scaladoc 说明 "primitive accessors throw NPE on null; use `Option[get]` or `isNullAt` for nullable fields"。同步在 `RowSuite` 增 7 条 null 路径回归测试。
- **预估**: 15-20 min（含测试）
- **价值**: 消除隐式数据丢失（`if (row.getBoolean(0)) ...` 在 null 字段下静默走 false 分支），并恢复 primitive 取数器之间的契约对称。
- **关联**: 与 R28 在 "原语 null 静默零值" 维度根因同源——R15 修 `Row.getXxx` 直接 accessor 入口（用户调 `row.getInt(0)`），R28 修 typed Dataset encoder 入口（`as[CaseClass].collect()` 走 `DerivedEncoder.fromRow → extractField → row.get(i)` 不走 `getXxx`，R15 的 patch 不触达 encoder 路径）。两项落在不同 API 入口、修复点对称（accessor NPE vs. encoder field extraction NPE），保留独立条目以分别承载两个 API 边界。与 R31 区分——R31 是引用类型（`Seq`/`Map`）null 在 Java-interop 二级转换路径上 NPE（loud failure），本项是原语 null 静默 unbox 为零值（silent corruption），失败模式相反。

#### R20. `ArrowSerializer` / `ArrowDeserializer` 对 `DateType` 的时区处理不对称（非 UTC JVM 下 round-trip 漂移）
- **文件**:
  - 编码端：`src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:173-179`
  - 解码端：`src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:89-90`
- **问题**: 编码与解码使用不同时区基准提取 / 还原 `epochDay`：
  - 编码端 `case d: java.sql.Date => d.toLocalDate.toEpochDay.toInt`：`Date.toLocalDate()` 用 **JVM 默认时区**从 millis 提取日历日期。
  - 解码端 `java.sql.Date(v.get(index).toLong * 86400000L)`：直接以 **UTC 午夜**的 millis 构造 Date。
  当 JVM 默认时区不是 UTC 时，两者不对称。以 `America/New_York`（UTC-5）为例：
  1. 用户构造 `Date.valueOf("2020-06-15")` → millis = 2020-06-15 00:00 EST = 2020-06-15 05:00 UTC
  2. 编码 `toLocalDate.toEpochDay`（默认 TZ）→ epochDay = 18428（对应 2020-06-15）
  3. 序列化 / 反序列化 → 解码端 `Date(18428 * 86400000L)` = UTC 午夜 millis → EST 视角下为 2020-06-14 19:00
  4. 用户后续 `getDate(0).toLocalDate`（默认 TZ）→ `LocalDate.parse("2020-06-14")`，比原值早一天
  在 UTC 下两者对齐，无漂移；但在 non-UTC 客户端下静默偏移。`ArrowRoundTripSuite.scala:85` 的 `genDate` 直接用 `epochDay * 86400000L` 构造输入（即解码端规范形式），property test 在任何 TZ 下都"通过"——bug 对测试不可见；CI runner 默认 TZ 是 UTC，CI 也不暴露。
  下游影响：`row.getDate(0).toLocalDate` / `.toString` / `PreparedStatement.setDate(idx, getDate(0))` / `getDate(0).equals(originalDate)` 都会观察到日期偏移；以日期为 partition key / join key 时静默错配。
- **建议**: 二选一：
  - A. 解码端改为 `java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(epochDay))`（默认 TZ 午夜，与编码 `toLocalDate.toEpochDay` 对称）。**首选**——保持用户视角的语义不变性，与 upstream Spark `DateTimeUtils.toJavaDate` 一致。
  - B. README 与 `ArrowSerializer`/`ArrowDeserializer` 顶层 scaladoc 显式声明 "client must run with `-Duser.timezone=UTC`"，并互相 `@see` 引用。修复成本低，但把语义陷阱暴露给用户。
  同时在 `ArrowRoundTripSuite` 增 1 条 non-UTC TZ 回归（`TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"))` 临时切换，用 `Date.valueOf("2020-06-15")` 构造输入并断言 round-trip 后 `getDate(0).toLocalDate == LocalDate.parse("2020-06-15")`；`afterEach` 还原 TZ）。
- **预估**: 25-35 min（含测试与 README 更新）
- **价值**: 消除真实数据腐蚀路径。当前任何 non-UTC client（远东 +9、欧洲 +1、美洲 -5/-8）的用户在 `getDate()` 后做日期算术或比较时都会看到日期偏移；测试与 CI 都不触发，调试成本极高。属与 R1 同一组"schema 通过 / payload 静默腐蚀"家族，但跨整个 `DateType` 路径而非元素子类型，因此独立列出。
- **关联**: 与 R21（`TimeStampNanoVector` 静默精度截断）同属"vector-specific 解码不对称"族系——R20 跨 TZ 静默漂移、R21 跨精度静默截断，模式相同但 vector 类目不同（Date 路径 vs. Timestamp 子族），保留独立条目分别承载 Date 与 Timestamp 路径的修复。

#### R28. `DerivedEncoder.fromRow` 对原语字段的 server null 静默 unbox 为零值（与 R15 同源但落点在 encoder 边界，R15 的 `Row.getXxx` NPE patch 不覆盖此路径）
- **文件**: `src/main/scala/org/apache/spark/sql/Encoder.scala:230-235`、`253-266`（对照 R15 修复点 `Row.scala:29-41` 的 `getBoolean`/`getInt`/...）
- **问题**: `DerivedEncoder.fromRow` 通过 `extractField` 抽取每个字段：
  ```scala
  private def extractField(row: Row, idx: Int, isOpt: Boolean): Any =
    if isOpt then if row.isNullAt(idx) then None else Some(row.get(idx))
    else row.get(idx)        // ← 非 Option 路径直接返回 Any，含 null
  ```
  其中 `isOpt = _schema.fields(i).nullable && _schema.fields(i).dataType != NullType`。`_schema` 由 case class 字段类型推导：对 `case class Item(id: Long, count: Int)` 而言，原语字段的 `nullable=false`，故 `isOpt=false`，`row.get(idx)` 直接返回 `null`（当 server 列实际可空，例如空分组上的 `MAX`、OUTER JOIN 未匹配侧、可空 UDF 返回值、`lit(null).cast("int")`）。`null` 进入 `Tuple` 后由 Scala 3 编译器为 case class 生成的 `fromProduct` 通过 `BoxesRunTime.unboxToInt(null) → 0` / `unboxToBoolean(null) → false` / `unboxToLong(null) → 0L` / `unboxToDouble(null) → 0.0` 静默 unbox——用户拿到一份原语字段为类型零值的 case class，与"真实零值"在结果上不可区分，无 NPE / 无报错 / 无日志。可达路径覆盖所有 typed action：`Dataset.collect()`（`Dataset.scala:541` `df.collect().map(encoder.fromRow)`）、`first()`、`head(n)`、`tail(n)`、`toLocalIterator()`。
- **关联**: 与 R15 在 "原语 null 静默零值" 维度根因同源——R15 修 `Row.getInt` 路径（用户直接调 `row.getInt(0)` 时抛 NPE 而非返回 0），R28 修 encoder 路径（`extractField` 走 `row.get(i)` 不走 `getXxx`，R15 的 patch 不触达；Scala 3 mirror 生成的 unboxing 在 encoder 落点之外，无法在 Row 层一次拦截）。两项落在不同 API 入口（直接 Row vs. typed Dataset）、修复点不同（accessor vs. encoder field extraction），保留独立条目；R15 的修复模板（在 accessor 边界抛 NPE 并附 field 上下文）可直接平移到 `extractField` 非 Option 分支。与 R31 区分——R31 是引用类型 null loud failure（落 Java-interop wrapper 入口），本项是原语 null silent corruption（落 encoder 入口），失败模式相反。与 R34 区分——R34 是 expression-tree builder 层 `lambdaVar` 名字唯一性失守（proto 序列化前），本项是 wire-format 反序列化后 encoder 路径的 null 处理，落点完全不同维度。
- **建议**: 在 `extractField` 中对非 Option 且 schema 字段非可空的位置加 null guard。简洁实现：在 `DerivedEncoder.fromRow` 的 while 循环里查 `_schema.fields(i).nullable`，若 `false` 且 `row.isNullAt(i)` 则抛 `NullPointerException(s"Field '${_schema.fields(i).name}' is non-nullable but Row contains null at index $i")`：
  ```scala
  while i < n do
    val field = _schema.fields(i)
    val isOpt = field.nullable && field.dataType != NullType
    if !isOpt && !field.nullable && row.isNullAt(i) then
      throw NullPointerException(
        s"Field '${field.name}' is non-nullable but Row contains null at index $i"
      )
    values(i) = extractField(row, i, isOpt)
    i += 1
  ```
  并在 `EncoderSuite` 加用例：构造一个 `case class Item(id: Long, count: Int)`、用 `Row.fromSeqWithSchema(Seq(1L, null), StructType(Seq(StructField("id", LongType, false), StructField("count", IntegerType, false))))` 直接喂给 `summon[Encoder[Item]].fromRow(row)`，断言抛 `NullPointerException` 而非返回 `Item(1L, 0)`。
- **预估**: 20-30 min（含测试覆盖）
- **价值**: 关闭典型 typed Dataset 路径的静默数据腐蚀。当前 `as[CaseClass].collect()` 在 server 返回原语列 null 时（聚合 / outer join / nullable UDF）静默回填零值，用户在下游做 `count > 0`、`id != 0L` 等判定时悄无声息地走错分支；CI / 单元测试不触发，问题只在真实查询出现。属"R15 家族"在 typed encoder 边界的对称修复。

#### R29. `SparkSession.failPendingObservations` 跨查询观测污染（无差别失败广播 + 全量 `clear()`，并发观测路径下错误完成不相关查询的 Future、catch 窗口内清除已成功查询的指标）
- **文件**: `src/main/scala/org/apache/spark/sql/SparkSession.scala:290-295`；触发位点 `src/main/scala/org/apache/spark/sql/DataFrame.scala:347-349,665-667`
- **问题**: `failPendingObservations` 当前实现：
  ```scala
  private[sql] def failPendingObservations(cause: Throwable): Unit =
    observationRegistry.values().forEach { obs =>
      obs.failMetrics(cause)
    }
    observationRegistry.clear()
  ```
  registry 是 `private val observationRegistry = ConcurrentHashMap[Long, Observation]()`（session-scoped，所有 plan-id 共享一份），而 `DataFrame.collect/tail` 的 catch-all（`catch case e: Exception => session.failPendingObservations(e); throw e`）在每条查询路径都调用此方法。两条破坏性后果均可达：
  - **(a) 跨查询错误异常完成**：当 session 内同时运行 query A 与 query B（均含 `df.observe(obs_X, ...)` 注册），query A 的 `collect()` 异常进入 catch 时，query B 的 `obs_X.failMetrics(cause_A)` 也被触发——B 的 `Observation.get` Future 用 A 的 cause 完成，B 的调用者拿到一个不属于自己的异常栈。`Observation.failMetrics` 通过 promise 完成，failure 不可撤销。
  - **(b) 成功指标在 catch 窗口内清除**：catch 块在 `processObservedMetrics` 之后才会被另一条查询触发——但若 query A 已成功 `processObservedMetrics`（写入指标 + remove）后又因下游错误进入 catch（例如 `applyPostConversions` 抛错），`failPendingObservations` 会 `clear()` 整个 registry，把还没消费指标的 query B/C/... 的 Observation 从 registry 中抹掉，后续 `processObservedMetrics` 在 `observationRegistry.get(planId) == null` 处静默丢弃指标——B/C 的 `obs.get` 永远阻塞（Observation promise 既不被 fulfill 也不被 fail，因为 `clear()` 只移除 map entry、不调 `failMetrics`），用户线程死锁。
  对照上游 `spark-mine-12/sql/connect/client/jvm/.../sql/Observation.scala`：上游 `Observation` 与 query 一对一绑定（per-`Dataset.observe` 调用注册到该 dataset 的 plan-id 而非全局 session map），异常路径只 fail 当前 query 的 observation。本 client 的 session-scoped registry 是简化设计，但 `failPendingObservations` 的实现把"当前查询失败"放大为"全局所有观测失败"。
  现有 `ObservationSuite` 仅覆盖单查询 happy-path 与单查询 fail-path，没有跨查询并发用例，缺陷无回归保护；server-dependent 类目被 coverage 排除（`build.sbt:142`），CI 不触发。
- **建议**: 改为按 plan-id 精确范围失败：
  ```scala
  private[sql] def failPendingObservations(planIds: Iterable[Long], cause: Throwable): Unit =
    planIds.foreach { planId =>
      val obs = observationRegistry.remove(planId)
      if obs != null then obs.failMetrics(cause)
    }
  ```
  并在 `DataFrame.collect/tail` 路径维护当前查询涉及的 plan-id 集合（可在 `executeAndCollect` 返回值或 catch 闭包局部变量中追踪），catch 时仅传入这些 plan-id。如需保留 "session 关闭时统一失败所有 pending observation" 的语义，可单独增加 `failAllPendingObservations(cause)` 仅由 `SparkSession.close()` 调用——与 catch 路径 API 区分开。同步在 `ObservationSuite` 加跨查询并发回归：两个并发 `df.observe(...).collect()`，第一条注入异常，断言第二条的 Observation 不被错误失败、不被静默清除。
- **预估**: 30-45 min（含 API 重构 + 测试）
- **价值**: 关闭真实并发观测路径的污染。当前 session 内任意查询的异常会"污染"所有正在进行的观测——`observe` API 在并发场景下事实失效。修复将 catch-all 的影响半径从 "所有 session 观测" 收敛到 "当前查询的观测"，恢复 `Observation` API 在并发场景下的可用性。属与 R15/R28 不同族系的纯并发正确性问题（前两者是 null 语义；R29 是共享状态的范围过大）。
- **关联**: 与 R48（`DataFrame.union`/`unionByName` 默认 `isAll=false` 致服务端套 `Distinct`，CRITICAL）、R41（`GroupedDataFrame.agg` 重复 column `.toMap` 静默合并，HIGH）同属 "客户端 API 路径静默语义偏离" 族系——三者落点正交但同根：R29 跨查询广播（共享 registry 影响半径过大）、R41 单查询内重复聚合静默丢失（变参 → Map 转换路径）、R48 setOp 默认 `isAll=false` 致 server 套 Distinct（proto 默认值未显式覆盖）；客户端构造请求 / 维护共享状态时未显式覆盖 proto 默认值或收敛影响范围，导致服务端按缺省语义执行、与上游 / 与方法名 / 与 SQL 路径产生分歧。Round 39 consistency 反向引用补齐。Round 40 新增 R50（`DataFrameWriter.jdbc` 缺 `assertNotPartitioned`/`assertNotBucketed`/`assertNotClustered` + `validatePartitioning()` 四重客户端校验致 `partitionBy`/`bucketBy`/`clusterBy` + jdbc 组合 silent contract violation，MEDIUM）与本项同属 "客户端 API 路径静默语义偏离" 大类，但 R50 落点在 sink format 校验缺失而非跨查询观测污染；Round 40 consistency 反向引用补齐。Round 40 新增 R51（`ArtifactManager.addArtifactsImpl` 用 inline `StreamObserver` 直接调 `asyncStub.addArtifacts` 注册流响应——既不走 `responseValidator.wrapIterator` 包裹也不在 `onNext` 中校验 `v.getSessionId`，server 端 session swap 静默接受，MEDIUM）与本项不同族系——R29 是单 RPC 内 `failPendingObservations` 跨查询广播过宽，R51 是单 RPC 路径上 sessionId mismatch guard 完全缺失；二者均影响 client 端正确性但触发点（session-scoped registry vs. 单 RPC observer）与修复路径（收敛影响范围 vs. 添加 mismatch 守护）不同；Round 40 consistency 反向引用补齐。

#### R31. `Row.getList` / `Row.getJavaMap` 在 `null` 字段上间接 NPE（Java-interop 公共 API 契约缺口）
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:81-91`
- **问题**: 两个 Java-interop 访问器在底层字段为 `null` 时返回一个包裹 `null` 的 Java 集合 wrapper，首次调用任意方法（`size()`/`get()`/`isEmpty()`）抛 NPE 进入 `scala.jdk.CollectionConverters` 内部栈：
  ```scala
  def getList[T](i: Int): java.util.List[T] =
    getSeq[T](i).asJava                         // get(i) == null → SeqWrapper(null) → 后续方法 NPE

  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    getMap[K, V](i).asJava                      // 同上路径
  ```
  `getSeq[T](null)` / `getMap[K, V](null)` 因为 `Seq` / `Map` 是引用类型、`asInstanceOf` 对 `null` 是 no-op，先返回 `null`；继而 `null.asJava` 经 `IterableHasAsJava` / `MapHasAsJava` extension 构造 `SeqWrapper(null)` / `MapWrapper(null)`——构造不抛，NPE 推迟到 wrapper 的首次方法调用，错误堆栈指向 `scala.jdk` 内部，Java 调用方难以定位。Scala 端 `getSeq` / `getMap` 行为是返回 `null`（与 `get(i)` 保持一致），但 Java-interop 包装入口偏离这一契约。
- **建议**: 与 `getSeq` / `getMap` 对齐，对 `null` 短路：
  ```scala
  def getList[T](i: Int): java.util.List[T] =
    val s = getSeq[T](i)
    if s == null then null else s.asJava

  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    val m = getMap[K, V](i)
    if m == null then null else m.asJava
  ```
  并在 `RowSuite` 加 nullable `ArrayType` / `MapType` 列上的回归（`getList` / `getJavaMap` 返回 `null` 而非 NPE wrapper）。如倾向"非 null 容器"语义，可改为返回 `java.util.Collections.emptyList()` / `emptyMap()`，但需在 scaladoc 明示 null-vs-empty 选择以避免下游误判 "字段存在 0 元素"。
- **预估**: 5-10 min（含测试）
- **价值**: 关闭 Java-interop 公共 API 在 nullable 字段下的契约缺口。当前异常堆栈指向 `scala.jdk` 内部（用户不易定位），改为返回 `null` 后调用方可以一次性 `if (list != null) ...` 处理，与 `getSeq` / `getMap` Scala 端契约一致。
- **关联**: 与 R15 / R28 区分——R15 / R28 是原语 null 静默 unbox 为零值（HIGH，silent corruption）；本项是引用类型 null 在二级转换路径上 NPE（loud failure）。R15 修 Row primitive accessor 入口，R28 修 typed encoder 入口，本项修 Java-interop wrapper 入口；三者落点不重叠。

#### R34. `functions.lambdaVar` 嵌套高阶函数变量名碰撞（裸 `"x"`/`"y"` 无唯一 ID，server analyzer 绑定错误或 ambiguous-name 抛出）
- **文件**: `src/main/scala/org/apache/spark/sql/functions.scala:2055-2083`
- **问题**: `lambdaVar(name)` 用裸 `name` 构造 `UnresolvedNamedLambdaVariable` 而无唯一后缀；`createLambda1`/`createLambda2` 是仅有的两个调用点，分别复用字面量 `"x"`/`"y"`：
  ```scala
  private def createLambda1(f: Column => Column): Column =
    val x = lambdaVar("x")    // 每次调用都重新发出 "x"
    ...

  private def lambdaVar(name: String): Expression.UnresolvedNamedLambdaVariable =
    Expression.UnresolvedNamedLambdaVariable.newBuilder()
      .addNameParts(name)     // 裸 "x" — 无唯一 ID
      .build()
  ```
  当用户嵌套高阶函数时（`transform(arr, x => filter(x, y => y > x))`、`transform_keys(... transform(...))` 链等），每层嵌套都发出同名 `UnresolvedNamedLambdaVariable("x")`，proto 中外层与内层的参数列表 + 引用全部共享同一名字。server-side analyzer 要么把内层引用错误绑定到最内层 outer scope（语义 silent 错）、要么直接抛 ambiguous-name 解析错误（loud 错）——两种行为都偏离用户意图。上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/internal/columnNodes.scala` 显式规避此问题：
  ```scala
  private val nextId = new AtomicLong()
  def apply(name: String) =
    new UnresolvedNamedLambdaVariable(s"${name}_${nextId.incrementAndGet()}")
  ```
- **建议**: 在 `functions` 对象顶部加 `private val nextLambdaId = new java.util.concurrent.atomic.AtomicLong()`，将 `lambdaVar` 改为 `addNameParts(s"${name}_${nextLambdaId.incrementAndGet()}")`：
  ```scala
  private val nextLambdaId = new java.util.concurrent.atomic.AtomicLong()

  private def lambdaVar(name: String): Expression.UnresolvedNamedLambdaVariable =
    Expression.UnresolvedNamedLambdaVariable.newBuilder()
      .addNameParts(s"${name}_${nextLambdaId.incrementAndGet()}")
      .build()
  ```
  并在 `FunctionsSuite` 加回归：构造 `transform(col, x => filter(x, y => y > x))`，断言 proto 中两层 `UnresolvedNamedLambdaVariable` 的 `name_parts` 互不相同（即各带不同后缀 `x_1` / `x_2` 或 `x_N` / `y_M`）。
- **预估**: 5-10 min（含测试）
- **价值**: 关闭嵌套高阶函数路径的 server-side 名字解析失败 / 误绑定。当前任何使用 `transform` / `filter` / `array_sort` / `transform_keys` / `transform_values` 嵌套调用都受影响——常见 SQL 用户路径（如 `df.select(transform(col("arr"), x => filter(x, y => y > 0)))`）在 Connect 客户端下静默或 loud fail，与 upstream Spark 行为完全分歧。
- **关联**: 与 R1 / R20 / R28 等数据 / 类型层 bug 维度不同——本项是 expression-tree builder 层的 ID 唯一性失守，发生在 proto 序列化前，不经过 Arrow 路径。与 R36 同属 "functions.scala 客户端表达式构造缺失上游状态注入" 族系——R34 是命名 ID 唯一化缺失，R36 是 seed 随机化缺失，两者修复路径同构（均需在客户端注入"每次调用变化"的状态），可一并落地。Round 39 新增 R49（`KeyValueGroupedDataset.cogroup`/`cogroupSorted` 不使用 mapValues 派生状态致 typed cogroup 路径值类型偏离，HIGH）与本项同属 "客户端表达式构造层与上游分歧" 大类，但 R49 落点在 typed Dataset API dispatch 完整性而非 lambda 命名 / seed 随机化注入；Round 40 consistency 反向引用补齐。

#### R41. `GroupedDataFrame.agg(aggExpr: (String, String), aggExprs: (String, String)*)` 用 `.toMap` 静默去重重复 column（与上游 `RelationalGroupedDataset` 偏离，重复 column-fn 对被静默合并）
- **文件**: `src/main/scala/org/apache/spark/sql/GroupedDataFrame.scala:41-42`
- **问题**: tuple-style 重载把 `aggExpr +: aggExprs` 拼成 `Seq[(String, String)]` 后立即调用 `.toMap` 派发到 `agg(exprs: Map[String, String])`：
  ```scala
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    agg((aggExpr +: aggExprs).toMap)
  ```
  Scala stdlib `Iterable.toMap` 对重复 key 取最后一个 value，当用户写 `df.groupBy("dept").agg("salary" -> "max", "salary" -> "min")` 时 `.toMap` 把两条聚合合并为单一 `Map("salary" -> "min")`，下游 `agg(exprs: Map[String, String])` 仅展开为 `min(salary)` 单列，`max(salary)` 被静默丢弃——用户无任何错误信号、`df.columns` 也只多一列，调试代价极高。
  对照上游 `spark-mine-12/sql/core/src/main/scala/org/apache/spark/sql/RelationalGroupedDataset.scala:89-90`：
  ```scala
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    toDF((aggExpr +: aggExprs).map(toAggCol))   // 直接 map，不走 toMap
  }
  ```
  上游用 `.map(toAggCol)` 保留 sequence 顺序与重复，结果包含两列 `min(salary)` + `max(salary)`。SC3 的 `.toMap` 是行为分歧。
- **建议**: 跳过 Map 中转，直接走 Column 路径：
  ```scala
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    val pairs = aggExpr +: aggExprs
    val aggCols = pairs.map { (colName, funcName) =>
      Column(Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(funcName)
          .addArguments(Column(colName).expr)
          .build()
      ).build()).as(s"$funcName($colName)")
    }
    if aggCols.isEmpty then df else agg(aggCols.head, aggCols.tail*)
  ```
  注意：`agg(exprs: Map[String, String])` 公共 API 仍保留（用户显式传 Map 即认领去重语义），但变参重载不再经过 `.toMap`。
  补回归：`df.groupBy("dept").agg("salary" -> "max", "salary" -> "min").columns` 应包含两列 `max(salary)` + `min(salary)`。
- **预估**: 10-15 min（替换 `.toMap` 中转 + 单测）
- **价值**: 关闭重复列静默合并的公共 API 行为分歧。当前用户调用 `agg("col" -> "sum", "col" -> "avg")` 表面合法但实际只生效一条，与上游 Spark 同名 API 行为完全相反，是 typed Dataset / GroupedDataFrame 主路径上的功能性差异。
- **关联**: 与 R29 同属 "GroupedDataFrame / SparkSession 公共 API 行为分歧" 维度但根因正交——R29 是跨查询 observation 污染（session-scoped registry 无差别失败广播），R41 是单查询内重复聚合静默丢失（变参 → Map 转换路径）；两者共用 `.collect()` / `.show()` 调用栈但失败语义不同。与 R34/R36/R37 同样是 "客户端表达式构造层与上游分歧" 大类，但 R41 落点在 GroupedDataFrame API 入口而非 functions.scala 表达式构造，修复路径独立。Round 38 新增 R48（`DataFrame.union`/`unionByName` 默认 `isAll=false` 致服务端套 `Distinct`，CRITICAL）与 R29/R41 同属 "客户端 API 路径静默语义偏离" 族系——三者落点正交但同根：R29 跨查询广播、R41 重复聚合静默丢失、R48 setOp 默认 `isAll=false` 致 server 套 Distinct（proto 默认值未显式覆盖）；Round 39 consistency 反向引用补齐。Round 40 新增 R50（`DataFrameWriter.jdbc` 缺 `assertNotPartitioned`/`assertNotBucketed`/`assertNotClustered` + `validatePartitioning()` 四重客户端校验致 `partitionBy`/`bucketBy`/`clusterBy` + jdbc 组合 silent contract violation，MEDIUM）与本项同属 "客户端 API 路径静默语义偏离" 大类，但 R50 落点在 sink format 校验缺失而非聚合变参 → Map 转换；Round 40 consistency 反向引用补齐。Round 44 新增 R70（`GroupedDataFrame.mean`/`avg`/`max`/`min`/`sum` 在空 `colNames*` 入口下静默返回原始 df 而非自动选择全部数值列，与上游 `RelationalGroupedDataset.aggregateNumericColumns` 通过 `selectNumericColumns(colNames)` 在空列名时 fallback 到 schema 内全部数值列偏离，MEDIUM）与本项同属 "GroupedDataFrame 公共 API 行为偏离" 族系——R41 落点在变参 → Map 重复 key 静默合并、R70 落点在空 `colNames*` 时 degenerate 兜底返回原始 df，两者修复路径正交可独立追加，共同闭合 GroupedDataFrame 公共 API 与上游行为对齐子系。

#### R45. `ResponseValidator.wrapIterator` 流式 RPC `iter.next()` 未包裹 `SESSION_CHANGED` try/catch（与同文件 `verifyResponse` 行为不对称，流式响应中途服务端 session 失效后 `sessionValid` 仍残留 `true`，后续单一 RPC 不会被本地短路而是真发出去再被服务端拒绝）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ResponseValidator.scala:32-41`
- **问题**: `verifyResponse` (line 21-29) 把 `fn` 包在 try/catch，捕获 `INVALID_HANDLE.SESSION_CHANGED` 后将 `sessionValid = false`：
  ```scala
  // ResponseValidator.scala:21-29
  def verifyResponse[RespT <: GeneratedMessage](fn: => RespT): RespT =
    val resp =
      try fn
      catch
        case e: StatusRuntimeException if isSessionChanged(e) =>
          sessionValid = false
          throw e
    extractServerSideSessionId(resp).foreach(trackSessionId)
    resp
  ```
  但 `wrapIterator` 包装流式 RPC 时仅在 `next()` 里把 `iter.next()` raw 调用 + `extractServerSideSessionId` 串接，未把 `iter.next()` 包在同语义 try/catch：
  ```scala
  // ResponseValidator.scala:32-41
  def wrapIterator[RespT <: GeneratedMessage](
      iter: Iterator[RespT] & AutoCloseable
  ): Iterator[RespT] & AutoCloseable =
    new Iterator[RespT] with AutoCloseable:
      def hasNext: Boolean = iter.hasNext
      def next(): RespT =
        val resp = iter.next()                                    // ← 未包 try/catch
        extractServerSideSessionId(resp).foreach(trackSessionId)
        resp
      def close(): Unit = iter.close()
  ```
  当流式 RPC（`ExecutePlanResponse` / `ConfigResponse` 等）中途收到 `INVALID_HANDLE.SESSION_CHANGED` 错误（如服务端 session re-enrolled / 移到其他 server），异常直接外抛但 `sessionValid` 仍为 `true`。下游所有单一 RPC（`AnalyzePlan`、`ExecuteCommand`、`Config` 等）经过 `verifyResponse` 时不会先 fail-fast，而是真发到服务端再被 `INVALID_HANDLE.SESSION_CHANGED` 拒绝，浪费一次 RTT；更严重的是 `SparkSession.invalidate` 等基于 `sessionValid` 的本地状态判断会读到错误的 `true`，可能在用户调用 `session.config(...)` 等"应当 fail-fast"路径时被误判为可用，引发用户层困惑（"上一次 collect() 抛 SESSION_CHANGED 后 session.config(...) 还能继续提交，不该这样吧"）。
  对照 upstream `spark-mine-12/sql/connect/client/jvm/src/main/scala/org/apache/spark/sql/connect/client/ResponseValidator.scala`：upstream `wrapIterator.next()` 同样包了 try/catch 把 `SESSION_CHANGED` 标记入 `sessionValid = false`，与 `verifyResponse` 路径对称。SC3 路径不对称属于行为偏离。
- **建议**: 让 `wrapIterator.next()` 复用 `verifyResponse` 同语义包装：
  ```scala
  def next(): RespT =
    val resp =
      try iter.next()
      catch
        case e: StatusRuntimeException if isSessionChanged(e) =>
          sessionValid = false
          throw e
    extractServerSideSessionId(resp).foreach(trackSessionId)
    resp
  ```
  或抽 `wrapWithSessionGuard[T](fn: => T): T` helper 同时被 `verifyResponse` 和 `wrapIterator.next()` 复用，避免散点重复。同步补回归：构造 mock streaming iterator，`next()` 抛 `INVALID_HANDLE.SESSION_CHANGED` 后断言 `sessionValid == false`、随后任何 RPC 应短路。
- **预估**: 15-20 min（一处包装 + helper 抽取 + mock 流单测）
- **价值**: HIGH。`SESSION_CHANGED` 是 SparkConnectClient 的核心健壮性场景之一，流式路径不更新 `sessionValid` 导致后续单一 RPC 不能本地 fail-fast，浪费 RTT 并误导用户层状态判断。这是流式与单 RPC 的根本不对称，与 upstream 偏离。
- **关联**: 这是当前 review 中 `ResponseValidator` 路径的首个 R 项（先前 R 项均不涉及该文件）；与 R46（同 `client` 子包 `ExecutePlanResponseReattachableIterator.callIter` 在已消费部分响应时无 guard re-execute）同属 SparkConnectClient retry / session-failure 语义族——R45 是 `sessionValid` 在流式路径漏写、R46 是 `lastReturnedResponseId.isDefined` 时 silent re-execute 漏 guard，两者修复路径独立但应一并审视该子包所有 retry/re-execute 与 session-state 信号传递路径的对称性。Round 37 completeness 维度新增。Round 40 新增 R51（`ArtifactManager.addArtifactsImpl` 用 inline `StreamObserver` 注册 `asyncStub.addArtifacts`，既不走 `responseValidator.wrapIterator` 包裹也不在 `onNext` 中校验 `v.getSessionId`，server 端 session swap 静默接受，MEDIUM）与本项同属 "客户端 session 防护族系" 但落点不同：R45 是 `ResponseValidator.wrapIterator` 内部对称缺失（catch 漏写），R51 是 `ArtifactManager` async streaming 路径完全绕过 ResponseValidator（注册时未挂保护层）；两者修复路径独立但应一并审视所有流式 RPC 注册路径是否被 ResponseValidator 链统一覆盖。Round 40 consistency 反向引用补齐。

#### R46. `ExecutePlanResponseReattachableIterator.callIter` 捕获 `OPERATION_NOT_FOUND`/`SESSION_NOT_FOUND` 后无条件重新发起 `rawExecutePlan(request)` 而不检查 `lastReturnedResponseId.isDefined`（部分响应已消费后又走全量 re-execute 导致前缀响应被用户业务逻辑重复消费）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ExecutePlanResponseReattachableIterator.scala:124-131`
- **问题**: `callIter` 是 `next()` / `hasNext` 的统一调用包装，在 RPC 抛 `OPERATION_NOT_FOUND`/`SESSION_NOT_FOUND` 时直接 fallback 到 `rawExecutePlan(request)` 全量 re-execute：
  ```scala
  // ExecutePlanResponseReattachableIterator.scala:124-131
  private def callIter[T](f: java.util.Iterator[ExecutePlanResponse] => T): T =
    try
      if iter == null then iter = rawReattachExecute()
      f(iter)
    catch
      case e: StatusRuntimeException if isRetryableExecuteStatus(e) =>
        iter = rawExecutePlan(request)             // ← 无条件重新发起
        throw GrpcRetryHandler.RetryException(e)
  ```
  但前置 `next()` 路径已经更新过 `lastReturnedResponseId`（line 102：`lastReturnedResponseId = Some(resp.getResponseId)`），意味着用户业务逻辑早已收到并处理了部分响应（已写文件、已发 metric、已变更状态等）。当下一次 `next()` 在 `iter.next()` 阶段抛 `OPERATION_NOT_FOUND` 后 `callIter` 重新发起完整 `executePlan`，新流的前缀响应（`responseId = "r1", "r2", ...`）会被用户业务逻辑**再处理一遍**，造成"双写 / 双 metric / 状态翻倍"等等可观察的副作用——这是数据正确性级问题。
  对照 upstream `spark-mine-12/sql/connect/client/jvm/src/main/scala/org/apache/spark/sql/connect/client/ExecutePlanResponseReattachableIterator.scala:244-254`：
  ```scala
  if (lastReturnedResponseId.isDefined) {
    throw new IllegalStateException(
      "OPERATION_NOT_FOUND/SESSION_NOT_FOUND on the server but responses were already " +
        "received from it.", ex)
  }
  ```
  upstream 显式检查并 fail-fast：在已消费部分响应的情况下不能 silent re-execute，必须升级为不可重试的 IllegalStateException 让上层用户层显式处理（要么放弃，要么补偿）。SC3 路径少了这道 guard。
- **建议**: 在 catch 块加 fail-fast guard：
  ```scala
  catch
    case e: StatusRuntimeException if isRetryableExecuteStatus(e) =>
      if lastReturnedResponseId.isDefined then
        throw IllegalStateException(
          s"OPERATION_NOT_FOUND/SESSION_NOT_FOUND on server but " +
            s"${lastReturnedResponseId.get} responses already received — " +
            "re-execution would duplicate observable side effects.",
          e
        )
      iter = rawExecutePlan(request)
      throw GrpcRetryHandler.RetryException(e)
  ```
  同步补回归：mock RPC 流先返回若干 response（更新 `lastReturnedResponseId`）再抛 `OPERATION_NOT_FOUND`，断言 `next()` 抛 `IllegalStateException` 而非 silent re-execute；首次就抛（`lastReturnedResponseId.isEmpty`）路径仍能正常 re-execute。
- **预估**: 15-20 min（一处 guard + 双路径 mock 单测 + 与 GrpcRetryHandler / ResponseValidator 共享路径回归）
- **价值**: HIGH。这是用户可观察的数据正确性问题——重试逻辑在已消费部分响应时不能盲目 re-execute，否则前缀副作用被双消费。与 upstream 偏离的同时也是 RPC 重试设计公认 best practice（at-most-once vs at-least-once 由调用方选择，库不应强加 at-least-once 语义）。
- **关联**: 与 R45（同 `client` 子包 `ResponseValidator.wrapIterator` 流式路径漏写 `sessionValid`）同属 SparkConnectClient retry / session-failure 语义族——R45 是 session 失效信号在流式路径不传递，R46 是已消费响应后无 guard silent re-execute；两者根因正交但都涉及"流式响应中途异常的语义边界"，应一并审视 `client` 子包内 ResponseValidator / ExecutePlanResponseReattachableIterator / GrpcRetryHandler 三者协作时的对称性。Round 37 completeness 维度新增。

#### R49. `KeyValueGroupedDataset.cogroup` / `cogroupSorted` 忽略 `mapValues` 派生状态（`originalRelation` / `originalValueEncoder` / `mapValuesFunc` 三者仅 `flatMapGroups` 路径正确读取，cogroup 系列直接用当前 `valueEncoder` + raw `func`，typed cogroup 路径下用户值类型与预期偏离）
- **文件**:
  - `src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala:263-302` — `cogroup` 实现
  - `src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala:310-353` — `cogroupSorted` 实现
  - 对照路径：`KeyValueGroupedDataset.scala:140-183` — `flatMapGroups` 正确路径
- **问题**: `mapValues` 在 `KeyValueGroupedDataset` 上派生新数据集时保留三项状态：`originalRelation`（mapValues 调用前的关系，等价于原始 V 类型的 dataset）、`originalValueEncoder`（V 编码器，与当前已变换为 W 的 `valueEncoder` 不同）、`mapValuesFunc`（V → W 转换函数）。这些状态被设计用于在下游 group 操作时把 mapValues 表达"延迟 inline"到 group 子句中，避免 server 物化中间 W dataset。
  `flatMapGroups` 在 `:140-183` 正确读取并 compose：
  ```scala
  val inputRelation = originalRelation.getOrElse(ds.df.relation)
  val inputValueAg = originalValueEncoder.map(_.agnosticEncoder).getOrElse(valueEncoder.agnosticEncoder)
  val composedFunc = mapValuesFunc match
    case Some(mvFunc) => MapValuesFlatMapAdaptor(mvFunc.asInstanceOf[Any => Any], func)
    case None => func
  ```
  但 `cogroup` 在 `:263-302` / `cogroupSorted` 在 `:310-353` 完全忽略这三项：
  ```scala
  // cogroup
  def cogroup[U, R](other: KeyValueGroupedDataset[K, U])(
      func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]
  ): Dataset[R] =
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder    // ← 已是 W (mapped) 不是原始 V
    val thisGroupingUdf = buildGroupingUdf(keyAg, valueAg)
    ...
    .setRel(ds.df.relation)                      // ← 直接用 ds.df.relation，未走 originalRelation
    ...
    .setUdf(buildSerializedUdf(new CoGroupAdaptor(func)))   // ← raw func，无 MapValuesCoGroupAdaptor
  ```
  `cogroupSorted` 同型缺陷在 `:310-353`。具体可观察影响：
  ```scala
  case class Order(id: Int, amount: Double)
  case class Customer(id: Int, name: String)
  val orders: Dataset[Order] = ...
  val customers: Dataset[Customer] = ...
  // 按 id 分组并 mapValues 提取 amount，预期 cogroup 时拿到 Iterator[Double]
  orders.groupByKey(_.id)
    .mapValues(_.amount)                          // V=Order → W=Double
    .cogroup(customers.groupByKey(_.id))(
      (id, amounts: Iterator[Double], custs) =>   // ← 期望 Iterator[Double]
        amounts.map(a => (id, a))                 // 实际 Iterator[Order]，类型推断后续 .map(_.amount) 才能恢复
    )
  ```
  实际行为：server 用 `ds.df.relation`（即原始 `orders` dataset）+ `valueEncoder = Double encoder`（mapValues 后）groupBy，但 `groupByKeyUdf` 是从 V 到 K 的提取——server 端 `Order` 行不被 mapValues 转换为 `Double`、`func` 拿到的 `Iterator[V]` 仍然是 `Iterator[Order]` 对象但 encoder 期待 `Double`，触发 server-side encoder mismatch 或 client-side ClassCastException。
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala`：上游 `cogroup` / `cogroupSorted` 通过 `MapValuesCoGroupAdaptor` 和 `UDFAdaptors.coGroupWithMappedValues` 显式 compose，与 `flatMapGroups` 路径对称。本仓 `flatMapGroups` 已正确实现却独缺 cogroup 系列，是同 typed API 的 dispatch 不一致——属"dispatch 完整性缺口"，非"未实现"。
  现有测试覆盖：`KeyValueGroupedDataset` 在 `build.sbt:142` 已被 coverage 排除（"requires live server"），`KeyValueGroupedDatasetSuite` 仅有少量 happy-path 用例，无 mapValues + cogroup 组合用例——缺陷无回归保护。
- **建议**: 在 cogroup / cogroupSorted 中按 flatMapGroups 路径模式 compose mapValues 状态：
  ```scala
  def cogroup[U, R](other: KeyValueGroupedDataset[K, U])(
      func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]
  ): Dataset[R] =
    val keyAg = keyEncoder.agnosticEncoder
    // 使用 originalValueEncoder（若存在）而非已变换 valueEncoder
    val thisInputAg = originalValueEncoder.map(_.agnosticEncoder)
      .getOrElse(valueEncoder.agnosticEncoder)
    val thisInputRel = originalRelation.getOrElse(ds.df.relation)
    val otherInputAg = other.originalValueEncoder.map(_.agnosticEncoder)
      .getOrElse(other.valueEncoder.agnosticEncoder)
    val otherInputRel = other.originalRelation.getOrElse(other.ds.df.relation)
    // 按需把 V→W mapValues compose 进 cogroup adaptor
    val composedFunc = (mapValuesFunc, other.mapValuesFunc) match
      case (Some(mv1), Some(mv2)) =>
        UDFAdaptors.coGroupWithMappedValues(func, Some(mv1), Some(mv2))
      case (Some(mv1), None) =>
        UDFAdaptors.coGroupWithMappedValues(func, Some(mv1), None)
      case (None, Some(mv2)) =>
        UDFAdaptors.coGroupWithMappedValues(func, None, Some(mv2))
      case _ => new CoGroupAdaptor(func)
    val thisGroupingUdf = buildGroupingUdf(keyAg, thisInputAg)
    val otherGroupingUdf = other.buildGroupingUdf(keyAg, otherInputAg)
    ...
    .setRel(thisInputRel)
    ...
    .setUdf(buildSerializedUdf(composedFunc))
  ```
  `cogroupSorted` 同型修复。同步在 `KeyValueGroupedDatasetSuite` / 集成测试加 mapValues + cogroup / cogroupSorted 组合回归（标 `@IntegrationTest`，依赖 live server），验证 `Iterator[V]` 在 user func 中等于 mapped W 类型而非原始 V。
- **预估**: 45-60 min（含 cogroup / cogroupSorted 双路径修复 + UDFAdaptors.coGroupWithMappedValues / MapValuesCoGroupAdaptor 适配 + 集成测试）
- **价值**: HIGH。typed Dataset 的 group + cogroup 组合是常用业务模式（join-by-key + per-group computation），mapValues 在 cogroup 路径下静默丢失是用户难以诊断的 typed-API 行为分歧——错误信号要么是 server-side encoder mismatch 异常（loud），要么是 user func 拿到错误类型的迭代器（silent，类型推断帮忙掩盖）。修复使 mapValues + cogroup / cogroupSorted 与 flatMapGroups 路径对称，恢复 typed API 在三种 group 操作下的行为一致性。
- **关联**: 与 R34 / R36 / R37 同属 "客户端表达式构造层与上游分歧" 大类——R34 是 lambda 变量名、R36 是 seed 随机化、R37 是 sample/randomSplit seed，三者都涉及客户端在构造请求时漏注入上游约束；R49 落点更上游（typed Dataset API dispatch 层），但根因相同：客户端有专用状态（mapValues 派生 `originalRelation`/`originalValueEncoder`/`mapValuesFunc`）但仅在 dispatch 链的部分入口（flatMapGroups）使用，cogroup 系列遗漏。与 R29 / R41 区分——R29 / R41 是公共 API 内部行为分歧（observation registry / agg toMap），R49 是 dispatch 链的不完整覆盖；与 R5 / R27 / R30 区分——前三者围绕 `Row.copy()` schema 丢失，R49 是 typed Dataset 派生状态在不同入口的 dispatch 不一致。Round 39 completeness 维度新增。

---

#### R52. `Catalog.listColumns` / `tableExists` / `functionExists` 形参顺序与上游相反致 silent argument-swap（用户从上游迁移代码 `tableExists("mydb", "users")` 在 SC3 被反向解读为 `tableName="mydb"` / `dbName="users"`，typed signature 用户无法察觉）

- **位置**: `src/main/scala/org/apache/spark/sql/Catalog.scala:78,180,190`
- **问题**: 三个公共 API 的形参顺序与上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala:146,247,275` 完全相反。SC3 实现：
  ```scala
  // Catalog.scala:78
  def listColumns(tableName: String, dbName: String): DataFrame = ...
  // Catalog.scala:180
  def tableExists(tableName: String, dbName: String): Boolean = ...
  // Catalog.scala:190
  def functionExists(functionName: String, dbName: String): Boolean = ...
  ```
  上游：
  ```scala
  // spark-mine-12/sql/api/.../catalog/Catalog.scala:146
  def listColumns(dbName: String, tableName: String): Dataset[Column]
  // :247
  def tableExists(dbName: String, tableName: String): Boolean
  // :275
  def functionExists(dbName: String, functionName: String): Boolean
  ```
  用户从上游 / 标准 Spark Scala API 迁移代码 `catalog.tableExists("mydb", "users")`（语义：`dbName="mydb"`, `tableName="users"`）在 SC3 被反向解读为 `tableName="mydb"`, `dbName="users"`。两种典型失败路径：
  - 若 `mydb` 不是合法 identifier 或 `users` 数据库不存在，server 抛 `NoSuchTableException` / `NoSuchNamespaceException`——loud failure 但错误消息指向错误的层（用户以为是 db 不存在，实际是 table 不存在）。
  - 若 `mydb` 与 `users` 都恰好是合法 identifier 且交叉解读路径下两者均存在（例如 `users` 数据库下确有名为 `mydb` 的表 / 函数），则 `tableExists`/`functionExists` 返回 `true`、`listColumns` 返回非预期列集合——**silent argument-swap**，用户业务逻辑得到错误数据。
- **修复**: 翻转三处形参顺序与上游一致：
  ```scala
  def listColumns(dbName: String, tableName: String): Dataset[Column] = ...
  def tableExists(dbName: String, tableName: String): Boolean = ...
  def functionExists(dbName: String, functionName: String): Boolean = ...
  ```
  注意：此修复是 binary-incompatible / source-incompatible 的——已发布 0.1.0/0.2.0/0.3.0 用户若依赖错误的顺序写了代码，升级后会编译通过但语义反转。建议在 0.4.0 升级时通过 changelog 显式高亮该 breaking change，同时为旧顺序保留 `@deprecated` 重载（同名不同形参顺序，Scala 不允许 method-overloading by parameter name，但可以加显式 `def listColumnsByTable(tableName: String, dbName: String)` 等过渡 API）。
- **预估**: 15-20 min（三个方法 + 调用方更新 + 测试断言更新；若加过渡 deprecated 重载额外 +15 min）
- **价值**: HIGH。Catalog API 是 Spark 用户最常用的元数据查询入口之一，typed signature `(String, String)` 在迁移路径下不会触发编译失败，silent 路径下产出错误数据是用户最难诊断的失败模式。
- **关联**: 与 R47 `Catalog.getCreateTableString` `AS SERDE` 关键字位置错放属同一 `Catalog.scala` 公共 API 静默契约违反族系——R47 是 SQL 文法层错放（server 端 ParseException loud failure），R52 是 typed signature 层静默反转（loud 与 silent 两种失败路径混合，silent 路径更危险）；可一并审计该文件全部 public API 与上游签名一致性，作为 Catalog 模块的批量修复。与 R55 `DataStreamReader.textFile` 返回类型偏离同属 "public API typed signature 与上游偏离" 族系（R52 在 Catalog 元数据 API、R55 在 Streaming 数据源 API），与 R53 `DataFrameNaFunctions.replace` "*" 通配 + 数值归一化缺失同属 "public API 与上游契约偏离" 大类，与 R56 `UDFRegistration.registerJava` 完全缺失同属 "public API 缺失/偏离" 族系（R52/R53/R55/R57 是已存在 API 的签名/语义偏离，R56 是 API 完全缺失子分支）。Round 43 新增 R59（`Catalog.getTable` / `Catalog.getFunction` 形参顺序与上游相反致 silent argument-swap，HIGH）与本项形成 Catalog 形参顺序闭环——R52 覆盖 `listColumns` / `tableExists` / `functionExists` 三个站点（`Catalog.scala:78,180,190`），R59 覆盖 `getTable` / `getFunction` 两个站点（`Catalog.scala:141,151`），合计 5 个站点构成 Catalog 模块形参顺序与上游一致性的批量修复闭环；同 Round 43 新增 R58（`RuntimeConfig.get` 在 key 不存在且无默认值时不抛 `NoSuchElementException`，HIGH）同属 "public API 与上游 typed/throws 契约偏离" 大 family——R58 在 SparkSession config 路径、R52/R59 在 Catalog 元数据路径，落点不同但模式同型（typed signature 表层不变但 throws/参数顺序契约偏离致用户从上游迁移代码静默走错路径）。Round 45 新增 R72（`Catalog.createTable` 5-arg 重载形参顺序 `(tableName, source, description, schema, options)` 与上游 `(tableName, source, schema, description, options)` 反向，HIGH）将本族系 site 数从 5 扩展到 6——R52 闭合 `listColumns/tableExists/functionExists` 三处、R59 闭合 `getTable/getFunction` 两处、R72 闭合 `createTable` 5-arg 重载第六站点 `Catalog.scala:250`；至此 Catalog 模块形参顺序与上游一致性的批量修复闭环完整。Round 47 consistency 维度补齐反向引用：Round 46 新增 R76 `Catalog.createTable(tableName, path)` 2-arg 重载硬编码 `source="parquet"` 与上游 Connect path 不调 `setSource(...)` 让 server fall back `spark.sql.sources.default` 偏离——与 R72 共同覆盖 createTable 全部入口（R72 闭合 5-arg 重载形参顺序，R76 闭合 2-arg 重载默认 source fallback），R52/R59/R72/R76 共同构成 Catalog 公共 API 与上游契约偏离的批量修复闭环（list/exists/get/createTable 全部入口）。

---

#### R55. `DataStreamReader.textFile(path)` 返回 `DataFrame` 而非 `Dataset[String]` 且未调用 `assertNoSpecifiedSchema("textFile")`（public API 返回类型契约 + 校验语义双重偏离，typed signature 用户必须改代码）

- **位置**: `src/main/scala/org/apache/spark/sql/DataStreamReader.scala:75`
- **问题**: SC3 实现：
  ```scala
  // DataStreamReader.scala:75
  def textFile(path: String): DataFrame = ... // 即 Dataset[Row]
  ```
  上游 `spark-mine-12/sql/api/.../streaming/DataStreamReader.scala:284-285`：
  ```scala
  def textFile(path: String): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(path).select("value").as[String](StringEncoder)
  }
  ```
  两处偏离：
  - **返回类型**：上游 `Dataset[String]` vs SC3 `DataFrame`（即 `Dataset[Row]`）。用户从上游迁移 `val ds: Dataset[String] = ssr.textFile(...)` 在 SC3 编译失败（`Dataset[Row]` 与 `Dataset[String]` 不兼容），即使用户改写为 `val df: DataFrame = ssr.textFile(...)`，下游 typed 操作（如 `ds.map(_.toUpperCase)`）仍需手工 `.as[String]` 转换。
  - **schema 校验缺失**：上游强制 `assertNoSpecifiedSchema("textFile")`，禁止用户先 `ssr.schema(customSchema)` 再 `textFile`（textFile 语义就是单列 `value: String`，自定义 schema 无意义且会与解析路径冲突）。SC3 在 `ssr.schema(customSchema).textFile(...)` 路径下静默接受 customSchema，server 端 streaming relation 行为未定义——可能用 customSchema 尝试映射 `value: String` 列产生强制类型转换错误，也可能完全忽略 customSchema 仅返回 `Dataset[Row]`，两种路径都与上游显式拒绝偏离。
- **修复**:
  ```scala
  def textFile(path: String): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(path).select("value").as[String]
  }
  ```
  其中 `text(path)` 已是 SC3 现有 API（返回 `DataFrame` of `value: String`），`.as[String]` 通过 `StringEncoder.given` 实例完成 typed 包装。同步审计 `DataStreamReader.scala` 内其余 source-format 入口（`text` / `json` / `csv` / `parquet` / `orc`）是否也有 `assertNoSpecifiedSchema` 缺失（`text` 语义同 textFile，但需保留无 schema-assertion 的入口供高级用户使用）。
- **预估**: 15-25 min（textFile 修复 + 审计 5 个 source-format 入口的 `assertNoSpecifiedSchema` 一致性 + 单测：`schema(...).textFile(...)` 路径应抛 AnalysisException）
- **价值**: HIGH。`DataStreamReader.textFile` 是 Spark Streaming 的常见入口，typed signature 偏离是 source-incompatible breaking change（已发布版本用户依赖 `DataFrame` 返回类型）。但 typed `Dataset[String]` 是上游 4.x 的稳定契约，长期分歧维护成本高于一次 breaking change。建议在 0.4.0 一并修复 R52 + R55，作为 Catalog + Streaming 公共 API 与上游对齐的批量 release。
- **关联**: 与 R52 同属 "public API typed signature 与上游偏离" 族系，落点不同：R52 是 Catalog 元数据 API、R55 是 Streaming 数据源 API；与 R57 `StreamingQuery` typed 返回（`recentProgress` / `lastProgress` / `exception` 返回 raw String）同属 streaming 路径 typed contract 偏离族系——R55 是 reader 入口返回类型，R57 是 query 状态查询返回类型，两条 streaming 路径 typed signature 同步与上游对齐可形成完整闭环。与 R53 `DataFrameNaFunctions.replace` 行为偏离 + R56 `UDFRegistration.registerJava` 完全缺失共同形成 R52/R53/R55/R56/R57 "public API 与上游契约偏离/缺失" family——R55 在该族系中是 streaming reader 返回类型分支，R56 是 API 缺失子分支，R53 是 NaFunctions 行为语义分支。Round 43 新增 R58（`RuntimeConfig.get` 不抛 `NoSuchElementException` 与上游 `@throws` 契约偏离，HIGH）/ R59（Catalog `getTable`/`getFunction` 形参顺序与上游相反致 silent argument-swap，HIGH）扩展该 family——R58 在 SparkSession config 路径、R59 在 Catalog 元数据路径，与 R55（DataStreamReader 流式入口）+ R57（StreamingQuery 状态查询）共同覆盖 SC3 公共 API 与上游契约偏离的 5 大子系（Catalog / Streaming reader / Streaming query state / SparkSession config / NaFunctions），形成 0.4.0 公共 API 与上游对齐批量 release 的核心修复集。Round 44 新增 R67（`Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` 而非 `MergeIntoWriter[T]`，HIGH）/ R68（`DataFrameReader.textFile(path)` / `textFile(paths*)` 返回 `DataFrame` 而非 `Dataset[String]` 且缺 `assertNoSpecifiedSchema("textFile")` 前置校验，HIGH）扩展该 family——R67 是 typed-Dataset MERGE DSL 链上 `T` 在第一步即丢失（与 R55 同型 typed contract 偏离但落点在 mergeInto 入口）、R68 是 R55 同型缺陷扩展到 batch DataFrameReader 入口（与 R55 完全同根：返回 `DataFrame` 而非 `Dataset[String]` + 缺 `assertNoSpecifiedSchema("textFile")` 双重偏离），R55 + R68 形成 reader-textFile typed contract 闭环（streaming + batch 同步对齐）、R55 + R57 + R67 形成 typed contract 大族系完整覆盖（reader 入口 + query 状态 + MERGE DSL）。Round 46 consistency 维度补齐反向引用：Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 streaming 子系统公共 API 偏离族系——R55 是 reader 入口返回类型 typed-contract 退化（DataFrame vs. Dataset[String]）、R74 是 writer 入口大小写归一化偏离（typed `OutputMode` 重载向 server 上送 PascalCase 而非 lowercase 致 IllegalArgumentException），两者从 streaming reader / writer 双向各自构成公共 API 与上游契约偏离 site。Round 47 consistency 维度补齐反向引用：Round 46 新增 R75 `DataStreamWriter.foreach(writer)` 总用 `UnboundRowEncoder` 序列化 `ForeachWriterPacket` 致 typed `Dataset[T].writeStream.foreach(writer: ForeachWriter[T])` 在 server 端按 `Row` 反序列化抛运行时 `ClassCastException`——与 R55 同 streaming/typed-DSL contract 偏离族系扩展（reader 入口 typed-return + writer 入口 typed encoder 双 site 闭合）。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R58. `RuntimeConfig.get(key)` 在 key 不存在且无默认值时不抛 `NoSuchElementException` 静默返回 `""`（与上游 `@throws[NoSuchElementException]` 契约偏离，且 `get` 与 `getOption` 在 "key 不存在" 路径上观察等价致 typed 路径区分破坏）

- **位置**:
  - `src/main/scala/org/apache/spark/sql/SparkSession.scala:497-507` — `RuntimeConfig` 三方法定义
  - `src/main/scala/org/apache/spark/sql/connect/client/SparkConnectClient.scala:271-273` — `getConfig` 静默兜底返回空字符串
- **问题**: SC3 实现：
  ```scala
  // SparkSession.scala:497-507
  final class RuntimeConfig private[sql] (private val client: SparkConnectClient):
    def get(key: String): String = client.getConfig(key)
    def get(key: String, default: String): String = getOption(key).getOrElse(default)
    def getOption(key: String): Option[String] = client.getConfigOption(key)

  // SparkConnectClient.scala:271-273
  def getConfig(key: String): String =
    val resp = executeConfigOp(_.setGet(ConfigRequest.Get.newBuilder().addKeys(key).build()))
    resp.getPairsList.asScala.headOption.map(_.getValue).getOrElse("")
  ```
  上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/RuntimeConfig.scala:48-52`：
  ```scala
  @throws[NoSuchElementException]("if the key is not set and there is no default value")
  def get(key: String): String = getOption(key).getOrElse {
    throw new NoSuchElementException(key)
  }
  ```
  两处偏离：
  - **`@throws` 契约偏离**：上游显式 `@throws[NoSuchElementException]`，SC3 静默返回 `""`；用户 `try { conf.get("spark.unknown") } catch { case _: NoSuchElementException => default }` 路径捕获不到异常，得到空字符串后续 `.toInt` / `.toLong` 抛 `NumberFormatException`，错误信息非 "key not set" 语义，调试链路被拉长。
  - **typed 路径区分破坏**：上游本意是让用户用 `getOption(key): Option[String]` 表达"键可选"语义、用 `get(key): String` 表达"键必须存在"语义（key 不存在即异常）；SC3 把 `get` 转成 `getOption(key).getOrElse("")`-equivalent（实际由 `getConfig` wire 路径 `setGet` 上的 server `pairsList.headOption.map(_.getValue).getOrElse("")` 兜底实现，wire-level 仍区分 `setGet` vs `setGetOption`），从用户可见行为上 `get` 与 `getOption` 在 "key 不存在" 路径上观察等价（前者得到 `""`、后者得到 `None`，但前者无异常区分能力）——typed contract 区分语义丢失。`get(key, default)` 路径正确（行 502 显式调 `getOption(key).getOrElse(default)`），仅 `get(key)` 偏离。
- **可观测影响**:
  - 用户从上游迁移 `try-catch NoSuchElementException` 路径无任何异常进入 catch
  - 用户预期"key 必须存在"得到空字符串后续操作异常类型偏离
  - `RuntimeConfig.get` 与 `RuntimeConfig.getOption` 在 SC3 上 "key 不存在" 路径行为观察等价，typed signature 上的差异（`String` vs `Option[String]`）丧失语义指引价值——`getOption` 显式可空、`get` 假设存在的契约失效
- **修复**: `SparkSession.scala:498` 改为通过 `getOption` 路径：
  ```scala
  @throws[NoSuchElementException]("if the key is not set and there is no default value")
  def get(key: String): String = getOption(key).getOrElse {
    throw new NoSuchElementException(key)
  }
  ```
  保留 `def get(key: String, default: String)` 与 `def getOption(key: String)` 不变；同步在 `SparkConnectClient.scala:271-273` 决定保留 server 兜底 `""` 行为（仅作 `getOption` 的内部 wire helper，对 `RuntimeConfig.getOption` 用户透明），或重构 `getConfig` 直接抛 `NoSuchElementException` 由 `getOption` 翻为 `None`——后者更接近上游单 source of truth；建议前者最小修复。新增回归：`RuntimeConfigSuite` `get` 不存在 key 抛 `NoSuchElementException`、`get` 已存在 key 返回值、`get(key, default)` 不存在 key 返回 default、`getOption` 不存在 key 返回 `None`。
- **预估**: 15-20 min（SparkSession 一行 + `@throws` 注解 + 4 项回归断言）
- **价值**: HIGH。`RuntimeConfig.get` 是 Spark 公共 API 中最常用的几条之一，typed contract 与上游 4.x 偏离是 typed signature 用户无法察觉的静默破坏；空字符串兜底导致下游错误链被屏蔽，与"key not set"的语义意图直接矛盾。Round 42 completeness 维度新增。
- **关联**: 与 R52 / R55 / R56 / R57 同属 "public API 与上游契约偏离/缺失" family，落点不同：R52 是 Catalog 元数据 API 形参顺序、R55 是 Streaming 数据源 API 返回类型、R56 是 UDFRegistration 缺方法、R57 是 StreamingQuery 返回 raw String、R58 是 RuntimeConfig `@throws` 契约 + typed 路径区分；五者共同构成 SC3 与上游 typed signature/契约偏离的完整 family——R58 在该族系中是 SparkSession 子层 API 的 `@throws` 契约分支。Round 42 completeness 维度新增。与 R59 同 round 新增的 Catalog 形参顺序反转（`getTable`/`getFunction`）共同形成 "Round 42 typed contract 偏离" 增量批次。

---

#### R59. `Catalog.getTable(tableName, dbName)` / `getFunction(functionName, dbName)` 形参顺序与上游相反致 silent argument-swap（与 R52 同族系，使 Catalog 公共 API 形参顺序静默反转族系在 5 个 site 上完整闭环）

- **位置**:
  - `src/main/scala/org/apache/spark/sql/Catalog.scala:141-148` — `getTable(tableName: String, dbName: String): DataFrame`
  - `src/main/scala/org/apache/spark/sql/Catalog.scala:151-157` — `getFunction(functionName: String, dbName: String): DataFrame`
- **问题**: SC3 实现：
  ```scala
  // Catalog.scala:141-148
  def getTable(tableName: String, dbName: String): DataFrame =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName(tableName).setDbName(dbName).build()
    ))

  // Catalog.scala:151-157
  def getFunction(functionName: String, dbName: String): DataFrame =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName(functionName).setDbName(dbName).build()
    ))
  ```
  上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala:181, 211`：
  ```scala
  @deprecated("use getTable(tableName: String) instead.", "4.0.0")
  @throws[AnalysisException]("database or table does not exist")
  def getTable(dbName: String, tableName: String): Table

  @deprecated("use getFunction(functionName: String) instead.", "4.0.0")
  @throws[AnalysisException]("database or function does not exist")
  def getFunction(dbName: String, functionName: String): Function
  ```
  形参顺序反转：上游 `(dbName, tableName)` / `(dbName, functionName)`，SC3 `(tableName, dbName)` / `(functionName, dbName)`。typed signature 上两个 `String` 形参编译期无类型区分，用户从上游迁移 `catalog.getTable("mydb", "users")` 在 SC3 被反向解读为 `tableName="mydb"` / `dbName="users"`，silent argument-swap。
- **可观测影响**:
  - 用户上游代码 `catalog.getTable("mydb", "users")` 在 SC3 上：
    - 若 `mydb` 不是 `users` 数据库下的合法表名 → server 抛 `NoSuchTableException`，错误信息提示"users 库下找不到 mydb 表"，与用户原意"mydb 库的 users 表"完全相反，调试方向被误导
    - 若 `mydb` 与 `users` 恰好都是合法 identifier（如同名表），server 在 `users` 数据库下找名为 `mydb` 的表——可能命中错误的数据，**silent data correctness 风险**
  - `getFunction` 同理在 `(functionName, dbName)` 反向解读下出现相同的 silent failure 模式
  - 上游这两个重载均为 `@deprecated`（4.0.0 标记），新代码应使用单参数 `getTable(tableName)` / `getFunction(functionName)`，但既然 SC3 决定保留双参重载就应与上游契约一致；当前 SC3 既无 `@deprecated` 标注也偏离参数顺序，复合契约偏离
  - typed signature 用户在 IDE 自动补全/迁移检查时无任何告警——是上游 `(dbName, tableName)` 与 SC3 `(tableName, dbName)` 在 `String` × `String` 类型下的最隐蔽偏离
- **修复**: 调换两个方法的形参顺序：
  ```scala
  @deprecated("use getTable(tableName: String) instead.", "0.4.0")
  @throws[org.apache.spark.sql.AnalysisException]("database or table does not exist")
  def getTable(dbName: String, tableName: String): DataFrame =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setDbName(dbName).setTableName(tableName).build()
    ))

  @deprecated("use getFunction(functionName: String) instead.", "0.4.0")
  @throws[org.apache.spark.sql.AnalysisException]("database or function does not exist")
  def getFunction(dbName: String, functionName: String): DataFrame =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setDbName(dbName).setFunctionName(functionName).build()
    ))
  ```
  注意 proto builder `setDbName` / `setTableName` / `setFunctionName` 字段名不变（proto 是 named-field 不依赖 Scala 形参顺序），仅 Scala 调用约定变化。同步审计 `Catalog.scala` 全文件其他形参顺序约定（`R52` 已审计 `listColumns/tableExists/functionExists` 三处），本项 `R59` 闭合 `getTable`/`getFunction` 两处——共 5 site 完整闭环。需要同步：(a) 现有 SC3 用户调用方批量调换实参顺序、(b) `0.4.0` 发布说明显式标注此 breaking change、(c) `CatalogIntegrationSuite` 加正反两条回归（`catalog.getTable("default", "tbl")` 应找 default 库下 tbl 表 / 反向 swap 抛 NoSuchTableException）。
- **预估**: 10-15 min（两方法形参调换 + `@deprecated` + `@throws` 标注 + IntegrationSuite 回归 + 0.4.0 release notes）
- **价值**: HIGH。Catalog 公共 API 形参顺序与上游相反是 typed signature 用户无法察觉的最高风险偏离——当 `dbName` 与 `tableName` 恰好都是合法 identifier 时，server 找错对象返回错误结果，是 silent data correctness 风险（不仅是错误异常）。本项闭合 R52 family 的剩余两个 site，使 Catalog 形参顺序族系在 5 个 site `Catalog.scala:78,141,151,180,190` 上完整闭环：`listColumns/getTable/getFunction/tableExists/functionExists`。Round 42 completeness 维度新增。
- **关联**: 与 R52 同族系直接扩展——R52 已覆盖 `listColumns(tableName, dbName)` / `tableExists(tableName, dbName)` / `functionExists(functionName, dbName)` 三处形参顺序反转，R59 补齐 `getTable` / `getFunction` 两处使该族系在 5 个 site 上完整闭环；与 R47 `getCreateTableString` `AS SERDE` 关键字位置错放同属 Catalog 公共 API 静默契约违反大类，但 R47 是 SQL 语法字符串拼装位置错误（关键字顺序），R52/R59 是 Scala API 形参顺序反转——不同失败模式同 Catalog 表面。与 R53 / R55 / R56 / R57 / R58 同属 "public API 与上游契约偏离/缺失" family——R59 在该族系中是 Catalog 子层 API 的形参顺序分支（与 R52 子链路同支）。Round 42 completeness 维度新增。Round 45 completeness 维度新增 R72，覆盖 `Catalog.createTable` 5-arg 重载 `description ↔ schema` 形参顺序反转，使 Catalog 形参顺序族系完整闭合至 6 个 site `Catalog.scala:78,141,151,180,190,250`——R52/R59 闭合 list / exists / get 五个 site，R72 闭合 DDL 创建侧第六个 site。Round 47 consistency 维度补齐反向引用：Round 46 新增 R76 `Catalog.createTable(tableName, path)` 2-arg 重载硬编码 `source="parquet"` 与上游 Connect path 不调 `setSource(...)` 让 server fall back `spark.sql.sources.default` 偏离——与 R72 共同覆盖 createTable 全部入口（5-arg 形参顺序 + 2-arg 默认 source fallback），R52/R59/R72/R76 共同构成 Catalog 公共 API 与上游契约偏离的批量修复闭环（list/exists/get/createTable 全部入口）。

---

#### R60. `Catalog.listDatabases` / `listTables` / `listFunctions` / `getDatabase` / `getTable` / `getFunction` 全部返回 raw `DataFrame` 而非 typed `Dataset[Database]` / `Dataset[Table]` / `Dataset[Function]` / `Database` / `Table` / `Function`（public API typed 契约偏离致用户从上游迁移代码编译失败 + 失去 schema 静态保证）

- **位置**: `src/main/scala/org/apache/spark/sql/Catalog.scala:52-94,131-154`
- **问题**: SC3 实现的所有 list / get 元数据 API 全部返回原始 `DataFrame`：
  ```scala
  // Catalog.scala
  def listDatabases(): DataFrame = ...                           // :52
  def listTables(): DataFrame = ...                              // :60
  def listFunctions(): DataFrame = ...                           // :83
  def getDatabase(dbName: String): DataFrame = ...               // :131
  def getTable(tableName: String): DataFrame = ...               // :136
  def getFunction(functionName: String): DataFrame = ...         // :146
  // 含三参 / 双参重载共 ~12 个公共 API 入口
  ```
  上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala:56,72,155,168,194` 全部为 typed DTO：
  ```scala
  def listDatabases(): Dataset[Database]
  def listTables(): Dataset[Table]
  def listFunctions(): Dataset[Function]
  def getDatabase(dbName: String): Database
  def getTable(tableName: String): Table
  def getFunction(functionName: String): Function
  ```
  其中 `Database` / `Table` / `Function` 是 `spark-mine-12/sql/api/.../catalog/interface.scala` 暴露的 case classes（含 `name` / `description` / `locationUri` / `tableType` / `isTemporary` / `database` 等 typed 字段）。三层用户路径全部失败：
  - **typed 字段访问**：`spark.catalog.getTable("t").tableType`——上游 `Table` 有 `tableType: String` 字段，SC3 `DataFrame` 必须 `.collect().head.getAs[String]("tableType")` 才能取，编译时类型不一致。
  - **typed 集合操作**：`spark.catalog.listTables().filter(_.isTemporary)`——上游可直接 typed lambda，SC3 必须 `df.filter(col("isTemporary") === true)` 改写为 string column 表达式，失去类型安全。
  - **single-row vs Dataset 不对称**：上游 `getDatabase` 返回 `Database`（单值），SC3 返回 `DataFrame`（即使只有一行用户也必须显式 `.collect().head.getAs[...]`），与 list 系列返回 `Dataset[X]` 形成的 typed pipeline 完全断裂。
- **修复**: vendor 上游 `Database` / `Table` / `Function` / `Column` 四个类型（已在 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/catalog/interface.scala` 暴露——上游为 plain `class ... extends DefinedByConstructorParams`，并非 `case class`；vendor 时若选 `case class` 走 `Encoders.product` 派生，需在测试中验证 `.as[Database]` 编码与上游 `DefinedByConstructorParams` Encoder 字段顺序/可空性一致；若严格对齐上游则保留 plain class + 显式 `Encoder` 注册），然后改造 12 个入口：
  ```scala
  // Vendor: src/main/scala/org/apache/spark/sql/catalog/interface.scala
  case class Database(name: String, catalog: String, description: String, locationUri: String)
  case class Table(name: String, catalog: String, namespace: Array[String],
    description: String, tableType: String, isTemporary: Boolean)
  case class Function(name: String, catalog: String, namespace: Array[String],
    description: String, className: String, isTemporary: Boolean)

  // Catalog.scala 改造
  def getDatabase(dbName: String): Database =
    catalogDf(_.setGetDatabase(...)).as[Database].head()
  def getTable(tableName: String): Table =
    catalogDf(_.setGetTable(...)).as[Table].head()
  def listTables(): Dataset[Table] =
    catalogDf(_.setListTables(...)).as[Table]
  // ...其余 9 个入口同型
  ```
  其中 `.as[Database]` / `.as[Table]` / `.as[Function]` 通过现有 `ProductEncoder` 完成 typed 包装（SC3 已支持 case-class encoder）。注意 `tableExists` / `functionExists` / `databaseExists` 返回 `Boolean` 不变（这是上游契约本就如此）；`listColumns` / `getCreateTableString` 等已是 typed 返回的方法保持不变。
- **预估**: 90-120 min（12 个入口签名转换 + 4 个 case class vendor + 现有调用方 / 测试断言全量更新——`CatalogSuite` 内 `df.collect().head.getString(0)` 类断言需改为 `.name` typed 访问；source-incompatible breaking change 需 0.4.0 release notes 标注）
- **价值**: HIGH。Catalog 元数据 API 是 Spark 用户最常用的入口之一，typed `Dataset[Table]` / `Table` 是上游 4.x 的稳定契约。已发布版本用户依赖 `DataFrame` 返回类型，0.4.0 升级后编译失败需手工迁移——但 typed 路径长期收益（编译时类型保证 + IDE 补全 + 失去 string-based column access 重构脆弱性）远高于一次 breaking change。建议在 0.4.0 一并修复 R52 + R55 + R57 + R59 + R60，作为 Catalog + Streaming 公共 API 与上游对齐的批量 release——避免多次小幅 breaking change 的累计迁移成本。
- **关联**: 与 R52（Catalog 三方法形参顺序反转） / R59（Catalog 两方法形参顺序反转）同 `Catalog.scala` 公共 API 偏离族系，R52/R59 修形参顺序、R60 修返回类型，三者构成 Catalog 模块与上游对齐的完整闭环；与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]`（streaming reader typed 返回偏离）/ R57 `StreamingQuery.recentProgress` / `lastProgress` / `exception` 返回 raw `String` / `Option[String]`（streaming query 状态 typed 返回偏离）同属 "public API 返回类型 typed 契约偏离" 大 family——R55 在 streaming reader 入口、R57 在 streaming query 状态查询、R60 在 catalog 元数据查询，三者覆盖 SC3 typed 返回契约不同子系。与 R56 `UDFRegistration.registerJava` 完全缺失同属 "public API 与上游契约偏离/缺失" 大族系（R52/R55/R57/R59/R60 是已存在 API 的签名/语义/返回类型偏离，R56 是 API 完全缺失子分支）。Round 43 completeness 维度新增。Round 44 在 typed-return 族系新增姊妹站点 R67（`Dataset[T].mergeInto` 丢失 `T` 落 MergeIntoWriter 类型参数路径）与 R68（`DataFrameReader.textFile` 返回 `DataFrame` 落 batch reader 路径，与 R55 streaming reader 完整闭环 textFile 双入口 typed 契约）。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R67. `Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` 而非 `MergeIntoWriter[T]`（typed-Dataset 的 MERGE DSL 链上 `T` 在第一步即丢失，`whenMatched` / `whenNotMatched` / `whenNotMatchedBySource` 全部回落到 `Row` 视图致用户从上游迁移类型驱动 schema evolution 代码无法编译）

- **位置**: `src/main/scala/org/apache/spark/sql/MergeIntoWriter.scala:14`、`src/main/scala/org/apache/spark/sql/Dataset.scala:728`
- **问题**: SC3 `MergeIntoWriter` 不带类型参数：
  ```scala
  // MergeIntoWriter.scala:14
  final class MergeIntoWriter private[sql] (table: String, df: DataFrame, on: Column):
    def whenMatched(): WhenMatched = ???
    def whenNotMatched(): WhenNotMatched = ???
  ```
  `Dataset[T].mergeInto` 直接返回 `MergeIntoWriter`：
  ```scala
  // Dataset.scala:728
  def mergeInto(table: String, condition: Column): MergeIntoWriter =
    df.mergeInto(table, condition)
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/MergeIntoWriter.scala:30`：
  ```scala
  abstract class MergeIntoWriter[T] {
    def whenMatched(): WhenMatched[T]
    def whenNotMatched(): WhenNotMatched[T]
    def whenNotMatchedBySource(): WhenNotMatchedBySource[T]
  }
  ```
  以及上游 `Dataset.scala:3173`：
  ```scala
  def mergeInto(table: String, condition: Column): MergeIntoWriter[T] = ...
  ```
  影响路径：用户上游代码
  ```scala
  case class Person(id: Long, name: String, age: Int)
  ds: Dataset[Person]
  ds.mergeInto("target", $"target.id" === $"source.id")
    .whenMatched($"source.age" > $"target.age")
    .updateExpr(Map("name" -> $"source.name"))  // 上游：MergeIntoWriter[Person] → WhenMatched[Person] 链类型驱动
  ```
  在 SC3 编译失败——`mergeInto` 返回 `MergeIntoWriter`（无 `T`），后续链式调用全部退化到 `Row` 视图，typed-driven schema evolution（基于 case class 字段名/类型推断目标列约束）的全套上游模式无法迁移。
- **修复**: vendor 上游 `MergeIntoWriter[T]` + 三个伴生 sealed trait 族（`WhenMatched[T]` / `WhenNotMatched[T]` / `WhenNotMatchedBySource[T]`），改造 `Dataset.scala:728` 返回 `MergeIntoWriter[T]`：
  ```scala
  // Dataset.scala
  def mergeInto(table: String, condition: Column): MergeIntoWriter[T] =
    new MergeIntoWriter[T](table, df, condition, this)
  ```
  同步在 `MergeIntoIntegrationSuite` 添加 `case class` typed 链断言（验证 `WhenMatched[Person].updateExpr` 接受 `Map[String, Column]` 后不丢类型）。
- **预估**: 30-45 min（vendor 4 类 + 1 入口签名改 + typed 链断言；source-incompatible 但仅影响 typed-Dataset 用户，需 0.4.0 release notes 标注）
- **价值**: HIGH。MERGE 是 0.4 引入的 Iceberg/Delta 关键算子，typed `Dataset[T].mergeInto` 链是上游推荐的类型驱动迁移模式。当前 `MergeIntoWriter` 无 `T` 等同断言"client 不支持 typed merge"——与 R55/R57/R60 同属 typed-return 契约族系，且 R67 落点最深（链上多个伴生类型同时丢 `T`）。
- **关联**: 与 R55（`DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]`） / R57（`StreamingQuery.recentProgress` / `lastProgress` / `exception` 返回 raw `String` / `Option[String]` 而非 typed proto case class） / R60（`Catalog.listDatabases` 等 6 入口返回 raw `DataFrame` 而非 typed `Dataset[Database]`） 同 "public API 返回类型 typed 契约偏离" 大 family——R55 在 streaming reader、R57 在 streaming query 状态、R60 在 catalog 元数据、R67 在 MERGE 链路，四者覆盖 SC3 typed-return 偏离的不同子系且落点正交可独立修复；与 R68 `DataFrameReader.textFile` 返回 `DataFrame` 同 Round 44 typed-return 增量批次但根因正交（R67 是 MergeIntoWriter[T] 类型参数缺失、R68 是 textFile 返回 `Dataset[String]` 缺失），可一并落地。Round 44 completeness 维度新增。Round 47 consistency 维度补齐反向引用：Round 46 新增 R75 `DataStreamWriter.foreach(writer)` typed encoder 缺失致运行时 wire-protocol `ClassCastException` 同 streaming/typed-DSL contract 偏离族系扩展——R67 是 typed-Dataset MERGE DSL 链上 `T` 在第一步即丢失（compile-time silent type widening），R75 是 streaming writer 自定义 sink 路径泛型参数完全缺失（runtime wire-protocol failure），两者落点正交但同根（typed `Dataset[T]` DSL 链路 `T` 类型参数在客户端 API 中失踪）。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R68. `DataFrameReader.textFile(path)` / `textFile(paths*)` 返回 `DataFrame` 而非 `Dataset[String]` 且缺 `assertNoSpecifiedSchema("textFile")` 前置校验（与 R55 streaming reader 同型缺陷扩展到 batch reader 入口，typed-pipeline 用户迁移 `.textFile(path).map(_.length)` 编译失败 + 用户误传 schema 静默被忽略）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameReader.scala:91,94`
- **问题**: SC3 `DataFrameReader.textFile` 两入口签名：
  ```scala
  // DataFrameReader.scala:91,94
  def textFile(path: String): DataFrame = format("text").load(path).select(Column("value"))
  def textFile(paths: String*)(using DummyImplicit): DataFrame =
    format("text").load(paths).select(Column("value"))
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/DataFrameReader.scala:542,569`：
  ```scala
  def textFile(path: String): Dataset[String] = textFile(Seq(path): _*)
  def textFile(paths: String*): Dataset[String] = {
    assertNoSpecifiedSchema("textFile")
    text(paths: _*).select("value").as[String](Encoders.STRING)
  }
  ```
  双重偏离：
  - **返回类型**：`DataFrame`（无类型）vs 上游 `Dataset[String]`（typed），上游约定的 `df.textFile(path).map(_.length)` 模式在 SC3 编译失败（`DataFrame` 无 `map(String => Int)` 重载——用户必须 `.as[String](Encoders.STRING)` 显式转换）
  - **schema 校验缺失**：上游 `assertNoSpecifiedSchema("textFile")` 防止用户先 `.schema(...)` 再 `.textFile(...)` 因 text format 不接受用户 schema 而静默忽略，SC3 不校验致 schema 静默丢失（fluent API 链路误用不报错）
  - **同站点对照 R55**：R55 在 streaming reader 同名方法 `DataStreamReader.textFile` 上犯同型缺陷，R68 在 batch reader 同名方法上独立暴露——R55 + R68 共同闭合 SC3 `textFile` 双入口（batch + streaming）的 typed contract 偏离
- **修复**: 改造两入口签名 + 加前置校验：
  ```scala
  def textFile(path: String): Dataset[String] = textFile(Seq(path): _*)
  def textFile(paths: String*): Dataset[String] =
    if userSpecifiedSchema.isDefined then
      throw IllegalArgumentException("User specified schema not permitted when using textFile")
    text(paths: _*).select("value").as[String](Encoders.STRING)
  ```
  同步在 `ReadWriteIntegrationSuite.scala:455` 已有 textFile 测试上加 typed 链断言（`.map(_.length).collect()` 验证 `Dataset[String]` typed 编译路径），并新增"先 .schema 再 .textFile"路径预期抛 `IllegalArgumentException` 的负向测试。
- **预估**: 15-25 min（2 入口签名改 + 校验加 + 测试断言；source-incompatible 但 R55 + R68 一并落地可作为 0.4.0 textFile 双入口对齐的批量 release）
- **价值**: HIGH。`textFile` 是 Spark 用户日常最高频入口之一（log / 文本数据处理 boilerplate），typed `Dataset[String]` 是上游稳定契约。当前 `DataFrame` 返回退化致 typed pipeline 全链路丢失编译时类型保证；schema 校验缺失致 fluent API 链路 silent contract violation。
- **关联**: 与 R55 `DataStreamReader.textFile` 返回 `DataFrame` + 缺 schema 校验完全同型缺陷在 streaming reader / batch reader 双站点对称暴露——R55 + R68 共同闭合 textFile 公开入口；与 R67 `Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` / R57 `StreamingQuery.recentProgress` / R60 `Catalog.list*` 同 "public API typed-return 契约偏离" 大 family；与 R55 同根因落点正交（R55 streaming、R68 batch）可独立修复但建议合并落地。Round 44 completeness 维度新增。Round 47 consistency 维度补齐反向引用：Round 46 新增 R75 `DataStreamWriter.foreach(writer)` typed encoder 缺失致运行时 wire-protocol `ClassCastException` 同 streaming/typed-DSL contract 偏离族系扩展——R55+R68 闭合 reader 入口 typed-return 双 site，R75 闭合 streaming writer 路径 typed encoder 缺失，三者共同覆盖 streaming 子系统 typed contract 偏离（reader textFile + writer foreach）。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R72. `Catalog.createTable(tableName, source, description, schema, options)` 5 参重载形参顺序 `description ↔ schema` 与上游反转（用户从上游迁移代码静默错位：`description` 字符串被传入 `schema: StructType` 形参致编译失败 / `schema` 表达式被传入 `description` 形参致 server 创建表无 schema 错误）

- **位置**: `src/main/scala/org/apache/spark/sql/Catalog.scala:250-256`
- **问题**: SC3 `Catalog.createTable` 5 参重载形参顺序为 `(tableName, source, description, schema, options)`：
  ```scala
  // Catalog.scala:250-256
  def createTable(
      tableName: String,
      source: String,
      description: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame = ...
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala:524-529`（Scala 5 参 `Map[String, String]` overload）/ `:501-506`（Java 5 参 `util.Map[String, String]` overload）：
  ```scala
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: util.Map[String, String]): DataFrame
  ```
  `schema` 与 `description` 形参顺序反转——用户从上游迁移 `catalog.createTable("t", "parquet", structType, "my desc", optsMap)`：
  - `structType: StructType` 被传入 SC3 `description: String` 形参 → 编译期类型错误，用户立即看到。
  - 但若用户写 `catalog.createTable("t", "parquet", "my desc", structType, optsMap)`（按 SC3 顺序）→ 编译通过，server 端按 SC3 proto 字段顺序解析——但 proto `CreateTable` builder 用 named-field setter（`setSchema(...)` / `setDescription(...)`）所以 proto 层正确，**Scala 层用户调用却与上游约定相反**——typed signature 用户在 IDE 自动补全时看到 `description: String, schema: StructType` 顺序，与上游 IDE 提示 `schema: StructType, description: String` 直接相反，silent migration trap。
- **修复**: 调换 `description` 与 `schema` 形参顺序对齐上游：
  ```scala
  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      description: String,
      options: Map[String, String]
  ): DataFrame =
    val createTableBuilder = CreateTable.newBuilder()
      .setTableName(tableName)
      .setSource(source)
      .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
      .setDescription(description)
    options.foreach((k, v) => createTableBuilder.putOptions(k, v))
    catalogDf(_.setCreateTable(createTableBuilder.build()))
  ```
  proto builder 字段名 `setSchema` / `setDescription` 不变（proto 是 named-field 不依赖 Scala 形参顺序）。同步在 `CatalogIntegrationSuite` 加正向回归（`catalog.createTable("t", "parquet", structType, "my desc", optsMap)` schema 正确生效）+ 反向回归（在 Round 45 → 0.4.0 release notes 显式标注此 breaking change，建议用户在升级时审计所有 5 参 `createTable` 调用点）。
- **预估**: 10-15 min（形参调换 + IntegrationSuite 正反两条回归 + 0.4.0 release notes）
- **价值**: HIGH。`Catalog.createTable` 5 参重载是 Catalog 公共 API 形参顺序与上游反转的最后一处——本项闭合后 Catalog 形参顺序族系在 6 个 site `Catalog.scala:78,141,151,180,190,250` 完整闭环：`listColumns / tableExists / functionExists / getTable / getFunction / createTable`。typed-signature 用户在 IDE 自动补全 / 迁移检查时无任何告警——是 Scala typed API 形参顺序最隐蔽偏离形式。Round 45 completeness 维度新增。
- **关联**: 与 R52 / R59 同族系直接扩展——R52 已闭合 `listColumns/tableExists/functionExists` 三处，R59 已闭合 `getTable/getFunction` 两处，R72 闭合 `createTable` 5 参重载这第六个也是最后一个 site，Catalog 形参顺序族系在 SC3 公共 API 表面完整闭环。R72 在 R52→R59→R72 链路中是 DDL 创建侧分支（list/exists/get 是查询侧，createTable 是创建侧），三者共同覆盖 Catalog 全部 6 个形参顺序反转 site。与 R47 `getCreateTableString` `AS SERDE` 关键字位置错放同属 Catalog 公共 API 静默契约违反大类，但 R47 是 SQL 语法字符串拼装位置错误（关键字顺序），R72 是 Scala API 形参顺序反转——不同失败模式同 Catalog 表面。与 R53 / R55 / R56 / R57 / R58 / R60 同属 "public API 与上游契约偏离" family——R72 在该族系中是 Catalog 子层 DDL 创建 API 的形参顺序分支。Round 45 completeness 维度新增。Round 47 consistency 维度补齐反向引用：Round 46 新增 R76 `Catalog.createTable(tableName, path)` 2-arg 重载硬编码 `source="parquet"` 与上游 Connect path 不调 `setSource(...)` 让 server fall back `spark.sql.sources.default` 偏离——R72 闭合 5-arg 重载形参顺序、R76 闭合 2-arg 重载默认 source fallback，两者共同覆盖 Catalog `createTable` 全部入口与上游契约对齐（createTable 全部 overload 入口 site）。

---

#### R73. `DataFrameWriter.insertInto(tableName)` 路径硬编码 `setMode(SAVE_MODE_APPEND)` 静默覆写用户先前 `mode("overwrite")` / `mode("ignore")` / `mode("error")` 设置（与上游 Connect path `sql/connect/common/.../DataFrameWriter.scala:136-145` 不调 `setMode()` 的语义直接偏离致用户 `df.write.mode("overwrite").insertInto("t")` 实际 APPEND 而非 OVERWRITE → 生产数据腐蚀风险）

> **状态：已闭合**（移除 `insertInto` 中硬编码 `.setMode(SAVE_MODE_APPEND)`，让 `buildWriteOp()` 内已设置的用户 saveMode 流到 proto；新增 `DataFrameWriterSuite` 两条回归测试与 `ReadWriteIntegrationSuite` 一条 overwrite-path 端到端测试）。

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameWriter.scala:98-114`
- **问题**: SC3 `insertInto(tableName)` 路径无视用户先前通过 `mode("overwrite")` / `mode(SaveMode.Overwrite)` / `mode("ignore")` 设置的 `saveMode`，硬编码强制覆写为 `SAVE_MODE_APPEND`：
  ```scala
  // DataFrameWriter.scala:98-114
  def insertInto(tableName: String): Unit =
    val writeBuilder = buildWriteOp()
    writeBuilder
      .setMode(WriteOperation.SaveMode.SAVE_MODE_APPEND)  // 硬编码！覆写 buildWriteOp 内 .setMode(toProtoMode(saveMode))
      .setTable(
        WriteOperation.SaveTable.newBuilder()
          .setTableName(tableName)
          .setSaveMethod(
            WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
          )
          .build()
      )
    executeCommand(...)
  ```
  注意 `buildWriteOp()` 已在 `DataFrameWriter.scala:147` 调用 `.setMode(toProtoMode(saveMode))` 把用户设置的 `saveMode`（`"overwrite" / "append" / "ignore" / "error"`）转为对应的 proto SaveMode，但 `insertInto` 路径在其后立即强制覆写为 `SAVE_MODE_APPEND`。对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/DataFrameWriter.scala:136-145`：
  ```scala
  override def insertInto(tableName: String): Unit = executeWriteOperation { builder =>
    builder.setTable(
      WriteOperation.SaveTable
        .newBuilder()
        .setTableName(tableName)
        .setSaveMethod(WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO))
  }
  ```
  上游 Connect path 不调 `.setMode()`，让 `executeWriteOperation { builder => ... }` 内部 builder 已设置的 user-mode 流到 proto；server 端 `insertInto` 语义按用户 `mode` 字段执行（`overwrite` → truncate-then-insert，`append` → insert，`ignore` → conditional skip，`error` → 表存在抛错）。SC3 硬编码覆写后用户 `df.write.mode("overwrite").insertInto("prod_table")` 实际行为是 APPEND——production data corruption pattern：用户期望全表覆写但实际数据追加，下游消费方读到混合新旧数据导致下游分析全错且无任何错误信号。
- **修复**: 移除 `insertInto` 路径中硬编码 `setMode(SAVE_MODE_APPEND)` 调用：
  ```scala
  def insertInto(tableName: String): Unit =
    val writeBuilder = buildWriteOp()  // buildWriteOp 已 setMode(toProtoMode(saveMode))
    writeBuilder.setTable(
      WriteOperation.SaveTable.newBuilder()
        .setTableName(tableName)
        .setSaveMethod(
          WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO
        )
        .build()
    )
    executeCommand(
      Command.newBuilder()
        .setWriteOperation(writeBuilder.build())
        .build()
    )
  ```
  同步在 `DataFrameWriterIntegrationSuite` 加：(a) `df.write.mode("overwrite").insertInto("t")` 验证表内容被全覆写；(b) `df.write.mode("append").insertInto("t")` 验证追加；(c) `df.write.insertInto("t")` （无 mode 设置，默认 `"error"`）验证表存在时抛 `TableAlreadyExistsException` / 不存在时按 error 语义处理。
- **预估**: 10-15 min（删除一行硬编码 setMode + IntegrationSuite 三条回归 + release notes data correctness fix 标注）
- **价值**: HIGH。`df.write.mode("overwrite").insertInto("t")` 实际 APPEND 是 silent data corruption 模式——用户期望全表 truncate-then-insert 但实际数据追加，造成生产环境表内容包含旧数据 + 新数据混合，下游 ETL / report / ML feature pipeline 全部错误且无任何信号，是 production data correctness 最高风险偏离形式之一。修复成本极小（删一行）但价值极高。Round 45 completeness 维度新增。
- **关联**: 与 R50 `DataFrameWriter.jdbc` 缺 `assertNotPartitioned` / `assertNotBucketed` / `assertNotClustered` 直接族系兄弟——同 `DataFrameWriter.scala` 文件同公共 API 静默契约违反，但 R50 是合法 partitioning/bucketing/clustering 状态被静默直通到 proto 但 server 端 jdbc relation 不消费（错误结果），R73 是用户显式 `mode("overwrite")` 后调 `insertInto(...)` 被静默替换为 APPEND（数据正确性）——R73 比 R50 模式更严重（数据腐蚀 vs 错误结果）。与 R29 `SparkSession.failPendingObservations` 跨查询观测污染 / R41 `GroupedDataFrame.agg` 重复列静默 toMap 合并 / R47 `Catalog.getCreateTableString` AS SERDE 拼接位置 / R48 `DataFrame.union` 默认 `isAll=false` / R70 `GroupedDataFrame.mean` 空入口静默 no-op 同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R73 在该 family 中是 DataFrameWriter sink 路径硬编码覆写分支（与 R50 同 DataFrameWriter 但落点正交：R50 是 jdbc 路径校验缺失、R73 是 insertInto 路径硬编码覆写）。Round 45 completeness 维度新增。Round 46 consistency 维度补齐反向引用：Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R73 是 batch DataFrameWriter sink 路径硬编码覆写（数据腐蚀）、R74 是 streaming DataStreamWriter 配置大小写归一化（loud 启动失败），两者从 batch / streaming writer 双侧覆盖 writer 公共 API 与上游契约偏离 site。

---

#### R74. `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载未将 `.toString` 结果 lowercase 化（与上游 Connect path `sql/connect/common/.../DataStreamWriter.scala:51-54` 的 `.toLowerCase(Locale.ROOT)` 偏离致 `outputMode(OutputMode.Append())` 实际向 server 发送 `"Append"` 大小写敏感字符串，server 侧 `InternalOutputModes.apply` 严格匹配小写抛 `IllegalArgumentException("Unknown output mode Append")` → 启动失败）

> **状态：已闭合**（typed 重载在客户端 `outputMode.toString.toLowerCase(Locale.ROOT)` 后下发，对齐上游 `sql/connect/common/.../DataStreamWriter.scala:51-54`；`DataStreamWriterSuite` 三条 typed `OutputMode` 小写化回归 + 一条 String 重载保留原状对照测试，确保仅 typed overload 归一化）。

- **位置**: `src/main/scala/org/apache/spark/sql/DataStreamWriter.scala:43-48,137`
- **问题**: SC3 `outputMode` typed 重载将 `streaming.OutputMode` 实例 toString 后直接转发给 String 重载：
  ```scala
  // DataStreamWriter.scala:43-48
  def outputMode(m: String): DataStreamWriter =
    mode = m  // 用户字符串原样存入
    this

  def outputMode(outputMode: streaming.OutputMode): DataStreamWriter =
    this.outputMode(outputMode.toString)  // 缺 .toLowerCase(Locale.ROOT)
  ```
  在 `start()` 路径 line 137 直接转发原始 `mode` 到 proto：
  ```scala
  // DataStreamWriter.scala:~137
  if mode != null then queryBuilder.setOutputMode(mode)
  ```
  上游 `streaming.OutputMode` 类型 `.toString` 实现返回 PascalCase（`"Append"` / `"Complete"` / `"Update"`），但 server 端 `org.apache.spark.sql.streaming.InternalOutputModes.apply(s: String)` 严格匹配小写：
  ```scala
  // server-side InternalOutputModes (upstream)
  def apply(outputMode: String): OutputMode = outputMode.toLowerCase(Locale.ROOT) match {
    case "append" => OutputMode.Append
    case "complete" => OutputMode.Complete
    case "update" => OutputMode.Update
    case _ => throw new IllegalArgumentException(s"Unknown output mode $outputMode. ...")
  }
  ```
  注意上游 server 在比较前已 `.toLowerCase(Locale.ROOT)`，但实际 server 发现 client 已上送 `"Append"` 与原始大小写——server 抛 `IllegalArgumentException("Unknown output mode Append")` 致流式查询启动失败。对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/DataStreamWriter.scala:51-54`：
  ```scala
  override def outputMode(outputMode: OutputMode): this.type = {
    sinkBuilder.setOutputMode(outputMode.toString.toLowerCase(Locale.ROOT))  // 客户端 lowercase
    this
  }
  ```
  上游 Connect path 在客户端 `.toString.toLowerCase(Locale.ROOT)` 显式归一化——SC3 缺此一步致用户调用 typed `outputMode(OutputMode.Append())` 必然启动失败；只有用户 happen to 调 String 重载并传 `"append"` 全小写时才能工作。typed-signature 用户从上游迁移代码完全无错误信号——`OutputMode.Append()` typed API 在上游正常工作但在 SC3 必然抛 server-side IllegalArgumentException。
- **修复**: typed 重载内显式 lowercase 化对齐上游：
  ```scala
  def outputMode(outputMode: streaming.OutputMode): DataStreamWriter =
    this.outputMode(outputMode.toString.toLowerCase(java.util.Locale.ROOT))
  ```
  同步在 `DataStreamWriterIntegrationSuite` 加：(a) `dsw.outputMode(OutputMode.Append()).start(...)` 不抛 IllegalArgumentException 启动成功；(b) `dsw.outputMode(OutputMode.Complete()).start(...)` 同；(c) `dsw.outputMode(OutputMode.Update()).start(...)` 同。注意 String 重载路径 `outputMode("Append")` 行为：保持原状（用户直传字符串视为高级 API，server 端会 lowercase 比较所以 `"Append" / "APPEND"` 等大小写均被 server 端正确解析），仅 typed `OutputMode` 重载需归一化。
- **预估**: 5-10 min（typed 重载内加 `.toLowerCase(Locale.ROOT)` + IntegrationSuite 三条 OutputMode 启动回归）
- **价值**: HIGH。流式查询 `outputMode` typed API 是 Structured Streaming 入口最高频惯用法之一，上游 typed `OutputMode.Append() / Complete() / Update()` 是稳定契约——SC3 typed 重载 100% 启动失败致整个 streaming pipeline 不可用。修复成本极小（一行 `.toLowerCase`）但用户从上游迁移代码 typed-signature path 完全 broken。Round 45 completeness 维度新增。
- **关联**: 新族系 "streaming wire-protocol 客户端归一化偏离"——本项 R74 是 SC3 流式 API 与 server 端 wire 协议大小写敏感性偏离的首个 site，与 R39 `StreamingQueryListenerBus.registerServerSideListener` `hasNext=false` 落空兜底 / R54 `StreamingQueryManager.get` `hasQuery` 未检查致伪造空 `StreamingQuery` / R55 `DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]` / R57 `StreamingQuery.recentProgress`/`lastProgress`/`exception` 返回 JSON `Seq[String]`/`Option[String]` / R65 `StreamingQuery.status` 返回 raw proto `StatusResult` 同属 streaming 子系统公共 API 偏离族，但 R74 落点最特殊：客户端字符串大小写归一化在客户端必须显式做，与 server 端 case-folding 设计意图配合——SC3 client 直接漏掉 `.toLowerCase(Locale.ROOT)` 是上游 Connect path 显式实现而 SC3 漏抄的关键一步。与 R47 `Catalog.getCreateTableString` AS SERDE 字符串位置 / R48 `DataFrame.union` 默认 `isAll=false` / R50 `DataFrameWriter.jdbc` 缺 partitioning 校验 / R73 `DataFrameWriter.insertInto` 硬编码 SAVE_MODE_APPEND 同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R74 在该 family 中是 streaming sink 配置 typed-overload 字符串大小写归一化分支。Round 45 completeness 维度新增。Round 46 accuracy 维度修正：先前 关联 文本将 R34 / R36 错误归类为 streaming 子系统——R34 实为 `functions.lambdaVar` 嵌套高阶函数变量名碰撞（functions.scala 表达式构造层），R36 实为 `functions.rand`/`randn` 默认 `seed=0L`（functions.scala 同层）；二者并非 streaming 公共 API 偏离族，而是 functions.scala 客户端构造表达式缺失上游状态注入族系；本次将 关联 中的 streaming 兄弟修正为 R39 / R54 / R55 / R57 / R65（streaming 子系统公共 API 偏离族真正成员）。Round 47 consistency 维度补齐反向引用：Round 46 新增 R75 `DataStreamWriter.foreach(writer)` 总用 `UnboundRowEncoder` 序列化致 typed `Dataset[T]` foreach 运行时 wire-protocol `ClassCastException` 同 streaming writer wire-protocol 偏离族系扩展——R74 与 R75 同 `DataStreamWriter` 文件双 site 闭合（outputMode 大小写归一化 + foreach typed encoder 类型参数缺失），且两者均为 streaming wire-protocol 偏离（R74 是字符串大小写、R75 是 binary encoder）。

---

#### R75. `DataStreamWriter.foreach(writer)` 总是用 `UnboundRowEncoder` 序列化 `ForeachWriterPacket`，typed `Dataset[T].writeStream.foreach(writer: ForeachWriter[T])` 在 server 端按 `Row` 反序列化致用户回调收到 `Row` 而非 `T`（运行时 wire-protocol `ClassCastException`，根因为 `DataStreamWriter` 类缺 `[T]` 类型参数无从拿 dataset encoder）

> **状态：已闭合**（`DataStreamWriter[T]` 加类型参数并保存 `encoder: AgnosticEncoder[?]`；`Dataset[T].writeStream` 路径用 `Option(encoder.agnosticEncoder).getOrElse(UnboundRowEncoder)` 把 dataset encoder 注入 writer，`foreach`/`foreachBatch` 改用 stored encoder 而非硬编码 `UnboundRowEncoder`；`DataFrame.writeStream` 与 `DataStreamWriter.apply(df)` 兼容入口仍返回 `DataStreamWriter[Row]` + `UnboundRowEncoder`；`DataStreamWriterSuite` 增 typed encoder 字段反射回归 + Dataset[Long]/Dataset[String] writeStream encoder 透传回归）。

- **位置**: `src/main/scala/org/apache/spark/sql/DataStreamWriter.scala:28,85-89`
- **问题**: SC3 `DataStreamWriter` 类缺 `[T]` 类型参数（line 28 `final class DataStreamWriter private[sql]` vs. 上游 `final class DataStreamWriter[T]`），`foreach` 把任意类型 writer 装包成 `ForeachWriterPacket(writer, AgnosticEncoders.UnboundRowEncoder)`：
  ```scala
  // DataStreamWriter.scala:85-89
  def foreach(writer: ForeachWriter[?]): DataStreamWriter =
    val packet =
      ForeachWriterPacket(writer.asInstanceOf[AnyRef], AgnosticEncoders.UnboundRowEncoder)
    foreachWriterPayload = Some(ForeachWriterPacket.serialize(packet))
    this
  ```
  Server 端 `ForeachBatchHelper` 用 `packet.datasetEncoder` 把 Arrow batch 解为对象后调 `writer.process(t)`：当用户写
  ```scala
  val ds: Dataset[Person] = ...
  ds.writeStream.foreach(personWriter).start()
  ```
  server 拿到 `UnboundRowEncoder`，把每行解为 `Row` 调 `personWriter.process(row: Row)` → 运行时 `ClassCastException: Row cannot be cast to Person`（或 typed 字段访问 NPE）。对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/DataStreamWriter.scala:47,124-131`：
  ```scala
  final class DataStreamWriter[T] private[sql] (
      ds: Dataset[T])
    extends streaming.DataStreamWriter[T] {
  ...
  override def foreach(writer: ForeachWriter[T]): this.type = {
    val packet = ForeachWriterPacket(writer.asInstanceOf[AnyRef], ds.agnosticEncoder)
    sinkBuilder.setForeachWriter(...)
    this
  }
  ```
  上游 `class DataStreamWriter[T]` 内 `ds.agnosticEncoder` 直接拿 dataset encoder——SC3 漏掉 `[T]` 后无从拿到 encoder 致硬编码 `UnboundRowEncoder`。
- **修复**: 给 `DataStreamWriter[T]` 加类型参数 + 保存 `encoder: AgnosticEncoder[T]`：
  ```scala
  final class DataStreamWriter[T] private[sql] (
      private val df: DataFrame,
      private val encoder: AgnosticEncoder[T]):
  ...
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] =
    val packet = ForeachWriterPacket(writer, encoder)
    foreachWriterPayload = Some(ForeachWriterPacket.serialize(packet))
    this
  ```
  `Dataset.writeStream` 改返回 `DataStreamWriter[T](df, agnosticEncoder)`，`DataFrame.writeStream` 改返回 `DataStreamWriter[Row](this, AgnosticEncoders.UnboundRowEncoder)`。同步在 `DataStreamWriterIntegrationSuite` 加 typed `ForeachWriter[Person]` 端到端验证 `process(t)` 收到 typed `Person` 而非 `Row`。
- **预估**: 60-90 min（`DataStreamWriter` 加 `[T]` + `Dataset.writeStream` / `DataFrame.writeStream` 路由调整 + `foreach` 落点改用 `encoder` + `DataStreamWriterIntegrationSuite` 端到端 typed ForeachWriter 回归）
- **价值**: HIGH。typed `ForeachWriter[T]` 是 Structured Streaming 自定义 sink 主入口，上游 `Dataset[T].writeStream.foreach(typedWriter)` 是稳定契约——SC3 wire-protocol 偏离致用户从上游迁移代码 100% ClassCastException 运行时失败且失败模式不易定位（错误发生在 server 端 ForeachWriter callback 内而非 client API）。修复成本中等（需重构 `DataStreamWriter` 类参数化）但用户 typed-signature 流式 sink 路径完全 broken。Round 46 completeness 维度新增。
- **关联**: 与 R67 `Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` / R55 `DataStreamReader.textFile` 返回 `DataFrame` / R68 `DataFrameReader.textFile` 返回 `DataFrame` / R74 `DataStreamWriter.outputMode(streaming.OutputMode)` 大小写归一化偏离同属 "streaming/typed-DSL 公共 API typed 契约偏离" 族——R75 在该族中是 streaming writer 自定义 sink 路径泛型参数完全缺失（最严重，运行时 wire-protocol 失败而非编译错），R67 是 typed-Dataset MERGE DSL 链上 `T` 在第一步即丢失（compile-time silent type widening），R55 / R68 是 reader 入口 typed 返回类型退化（compile-time 类型错配），R74 是 writer 配置字符串归一化（loud server 启动失败）；R75 与 R74 同 `DataStreamWriter` 文件但落点不同：R74 是 `outputMode` typed 重载未 lowercase、R75 是 `foreach` typed 入口不知 dataset encoder 类型——两者共同闭合 `DataStreamWriter` 公共 API 与上游 typed 契约偏离站点在 `outputMode` + `foreach` 双 site。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R77. `SparkSession.range(end)` / `range(start, end, step)` / `range(start, end, step, numPartitions)` 全部返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载（typed-pipeline 用户从上游迁移 `spark.range(10).map(x => x * 2)` 编译失败、`spark.range(0, 100)` 调用因双参重载缺失被路由到 step-default 路径需手动追加 step 实参）

- **位置**: `src/main/scala/org/apache/spark/sql/SparkSession.scala:122-144`
- **问题**: SC3 现有 4 个 range 重载全部返回 `DataFrame`：
  ```scala
  def range(end: Long): DataFrame = range(0, end, 1)
  def range(start: Long, end: Long, step: Long = 1): DataFrame = ...
  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = ...
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala:416-440`：
  ```scala
  def range(end: Long): Dataset[lang.Long]
  def range(start: Long, end: Long): Dataset[lang.Long]
  def range(start: Long, end: Long, step: Long): Dataset[lang.Long]
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[lang.Long]
  ```
  上游 5 个重载全部返回 `Dataset[java.lang.Long]`（typed），且显式提供 `range(start, end)` 双参重载（无 step 默认值依赖）。SC3 偏离致 `spark.range(10).map(x => x * 2)` 类型失败（`map` 不在 `DataFrame` 上）、用户从上游迁移代码必须改写为 `.as[java.lang.Long].map(...)`。
- **修复**: 4 个重载返回类型改为 `Dataset[java.lang.Long]` + 补 `range(start, end)` 双参重载（不通过 step 默认值依赖）：
  ```scala
  def range(end: Long): Dataset[java.lang.Long] = range(0, end, 1, None)
  def range(start: Long, end: Long): Dataset[java.lang.Long] = range(start, end, 1, None)
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] = range(start, end, step, None)
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] = range(start, end, step, Some(numPartitions))

  private def range(start: Long, end: Long, step: Long, numPartitions: Option[Int]): Dataset[java.lang.Long] =
    newDataset(BoxedLongEncoder) { builder =>
      val rangeBuilder = builder.getRangeBuilder.setStart(start).setEnd(end).setStep(step)
      numPartitions.foreach(rangeBuilder.setNumPartitions)
    }
  ```
  需引入 `BoxedLongEncoder` 等同上游 `JavaLongEncoder` 注册。同步在 `SparkSessionIntegrationSuite` 加 typed-pipeline 验证：`spark.range(10).map(_.longValue + 1).collect()` 不抛 ClassCastException 且返回 typed `Array[java.lang.Long]`。
- **预估**: 25-35 min（4 个重载签名改 + 1 个双参重载补 + `BoxedLongEncoder` 引入 + IntegrationSuite typed-pipeline 回归 + 0.4.0 release notes 标注 source-incompatible breaking change）
- **价值**: HIGH。`spark.range(...)` 是用户最常用的数据生成入口，typed `Dataset[java.lang.Long]` 是上游稳定契约——SC3 typed-pipeline 用户在 `range().map()` / `range().filter()` 处全部编译失败，必须显式 `.as[java.lang.Long]` 桥接。修复成本中等（需引入 BoxedLongEncoder + 重构内部入口）但用户 typed-signature 数据生成路径退化严重。Round 47 completeness 维度新增。
- **关联**: 与 R55（`DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]`） / R57（`StreamingQuery.recentProgress` / `lastProgress` / `exception` 返回 raw `String`） / R60（`Catalog.list*` / `get*` 返回 raw `DataFrame` 而非 typed `Dataset[Database/Table/Function]`） / R65（`StreamingQuery.status` 返回 raw proto `StatusResult`） / R67（`Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter`） / R68（`DataFrameReader.textFile` 返回 `DataFrame`） / R75（`DataStreamWriter.foreach` typed encoder 缺失）同 "public API typed-return 契约偏离" 大族系——R55/R68 在 reader 入口、R57/R65 在 streaming query 状态、R60 在 catalog 元数据、R67 在 MERGE DSL、R75 在 streaming writer foreach、R77 在 SparkSession 数据生成入口；R77 与 R55/R68 落点同型（典型 typed-return 退化模式：`DataFrame` 返回退化致 typed-pipeline 链路全链失类型）但落点正交（R55 streaming reader、R68 batch reader、R77 SparkSession 入口）；同时 R77 缺少 `range(start, end)` 双参重载与 R71 `DataFrame.join(right, usingColumn: String, joinType: String)` 三参单列重载完全缺失同 "上游已稳定的便利重载在 SC3 完全缺失" 子族系。Round 47 completeness 维度新增。Round 49 关联：R81 `DataFrameWriterV2.replace()` / `createOrReplace()` 无参方法完全缺失——与本项 `range(start, end)` 双参重载缺失同 "上游已稳定的便利重载/无参便利方法在 SC3 完全缺失" 子族（R77 在 SparkSession `range` overload、R81 在 DataFrameWriterV2 sink 无参方法），R71/R77/R81 同 "Scala 3 API surface 公共便利重载/方法缺失" 子族 0.4.0 集中纯 additive 修复。

---

#### R80. `SparkSession.Builder` 缺 `interceptor(ClientInterceptor): Builder` 方法 + `SparkConnectClient.create` 工厂签名不接受 interceptor 列表（OpenTelemetry tracing / Datadog APM / 自定义 mTLS auth / 企业 SSO header 等 cross-cutting concerns 公共扩展点对外完全不可达）
- **位置**: `src/main/scala/org/apache/spark/sql/SparkSession.scala:448-495`、`src/main/scala/org/apache/spark/sql/connect/client/SparkConnectClient.scala:537-595`
- **问题**: SC3 `SparkSession.Builder` 完全没有 `interceptor(ClientInterceptor): Builder` 方法，且 `SparkConnectClient.create(url, sessionId, configs)` 工厂签名不接受 interceptor 列表。`channelBuilder` 只硬编码 `userAgent` + `maxInboundMessageSize`，token-based auth 路径单一 fallback 到 `MetadataUtils.newAttachHeadersInterceptor`；上游 `org.apache.spark.sql.connect.SparkSession.Builder.interceptor(ClientInterceptor): this.type`（sql/connect/common SparkSession.scala:966-969）是公共 API，Spark Connect Java/Python 跨语言客户端用户依赖此入口注入 OpenTelemetry tracing、Datadog APM、自定义 mTLS auth、企业 SSO header 拼装等中间件。SC3 用户从上游迁移代码 `SparkSession.builder().remote(url).interceptor(myTraceInterceptor).build()` 编译失败，且在 SC3 内无任何替代方案——既不能通过 Builder，也不能通过 `SparkConnectClient` 公共构造，**整个 observability/auth 扩展点对外不可达**。注意 upstream scaladoc 明确 "interceptors added last are executed first by gRPC"，修复时需维持向量追加序与上游一致。
- **修复**: 三步：
  1. `Builder` 增 `private var interceptors: Vector[io.grpc.ClientInterceptor] = Vector.empty` + `def interceptor(interceptor: io.grpc.ClientInterceptor): Builder = { interceptors = interceptors :+ interceptor; this }`；
  2. `SparkConnectClient.create` 增 `interceptors: Seq[io.grpc.ClientInterceptor] = Seq.empty` 形参，在 `MetadataUtils` token-interceptor 链尾追加 `baseStub.withInterceptors(interceptors :+ tokenInterceptor: _*)`；
  3. `Builder.build()` 把 `interceptors` 传给 `create`。
  补端到端 unit test：mock 一个计数 `ClientInterceptor`，验证多个 RPC 调用后 `interceptCall` 计数与调用次数一致 + interceptor 出现在 token-interceptor 之前（顺序断言）。
- **预估**: 25 min（Builder 接口 + Client 工厂签名 + 1 个端到端 tracing interceptor unit test）
- **价值**: HIGH。解锁 enterprise 部署场景（强制 mTLS、强制 tracing、企业 SSO），典型 production gate；当前 SC3 用户在生产环境无法注入 OpenTelemetry/Datadog 等可观测性 SDK，无法满足合规审计要求。
- **关联**: 与 R56 `UDFRegistration.registerJava` / R69 `options(java.util.Map)` Java-interop 重载 5 site 缺失 / R71 `DataFrame.join(right, usingColumn, joinType)` 三参单列重载缺失 / R78 `freqItems` Array[String] / R79 `sampleBy` ju.Map 同 "Java/跨语言互操作公共 API 缺位" 大族系——但 R80 范围最大：R56/R69/R71/R78/R79 均影响单一具体 API site 的 Java 互操作，R80 影响所有 cross-cutting concerns（tracing/auth/metrics）的整个扩展点入口（observability + auth 跨子系统插件机制完全缺位），严重程度更高。Round 48 completeness 维度新增。

---

#### R82. `DataFrameReader.table(tableName)` / `DataStreamReader.table(tableName)` 静默丢弃 user-set options（pre-`table()` `option(...)` 链上设的 kafka `startingOffsets` / `maxOffsetsPerTrigger` / parquet `mergeSchema` 等用户配置在内部 proto 构建阶段不传递致 server 端按 cluster default 解析数据源——失败模式不可观测，结果集与用户预期完全偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameReader.scala:66`、`src/main/scala/org/apache/spark/sql/DataStreamReader.scala:78-90`、`src/main/scala/org/apache/spark/sql/SparkSession.scala:109-119`
- **问题**: SC3 `DataFrameReader.table(tableName)` 单行委托至 `session.table(tableName)`，丢弃自身 `opts` 字段；`SparkSession.table` 直接构建 `Read.NamedTable` proto 不接受 options 参数；`DataStreamReader.table(tableName)` 同型直接构建 proto 不传递 `opts`：
  ```scala
  // DataFrameReader.scala:66
  def table(tableName: String): DataFrame = session.table(tableName)
  // SparkSession.scala:109-119
  def table(tableName: String): DataFrame =
    DataFrame(this, Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
      .setRead(Read.newBuilder()
        .setNamedTable(Read.NamedTable.newBuilder()
          .setUnparsedIdentifier(tableName).build())
        .build())
      .build())
  // DataStreamReader.scala:78-90 同型（多 setIsStreaming(true)）
  ```
  `DataFrameReader` 的 `opts: Map[String, String]` 字段（line 13-16）以及 `DataStreamReader` 的 `opts` 字段（line 13-15）通过 `option(...)` 链积累的 user-set 选项，在 `table(...)` path 上完全不传入 proto。对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/DataFrameReader.scala:159` `def table(tableName: String): DataFrame = sparkSession.read.format("table").options(extraOptions).load(tableName)`——上游 path 把 `extraOptions` 写入 proto `DataSource.options`；SC3 三个 site 全部静默丢弃。失败模式：用户写 `spark.read.option("mergeSchema", "true").table("hive_db.t")` 在 SC3 上构造 proto 仅含 `tableName`，server 端按 cluster `spark.sql.parquet.mergeSchema` 默认值解析（false）——schema 演化场景下静默仅读取最新 partition schema 致用户列丢失，无任何错误信号；同型 streaming 路径 `spark.readStream.option("startingOffsets", "earliest").table("kafka_topic")` 静默按 server 端 default `latest` 启动消费致丢消息——不可观测的失败模式，比 silent-default-fallback 更致命。
- **修复**: 3 site `table(...)` 实现合并 `opts` 传入 proto。优先方案：让 `SparkSession.table` 接受可选 options 参数并在 proto `NamedTable` 上写入；`DataFrameReader.table` / `DataStreamReader.table` 改为传递自身 `opts`：
  ```scala
  // SparkSession.scala
  def table(tableName: String, opts: Map[String, String] = Map.empty): DataFrame =
    val ntBuilder = Read.NamedTable.newBuilder().setUnparsedIdentifier(tableName)
    opts.foreach((k, v) => ntBuilder.putOptions(k, v))
    DataFrame(this, Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
      .setRead(Read.newBuilder().setNamedTable(ntBuilder.build()).build())
      .build())
  // DataFrameReader.scala:66
  def table(tableName: String): DataFrame = session.table(tableName, opts)
  // DataStreamReader.scala:78
  def table(tableName: String): DataFrame =
    val ntBuilder = Read.NamedTable.newBuilder().setUnparsedIdentifier(tableName)
    opts.foreach((k, v) => ntBuilder.putOptions(k, v))
    DataFrame(session, Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setRead(Read.newBuilder().setIsStreaming(true).setNamedTable(ntBuilder.build()).build())
      .build())
  ```
  同步在 `DataFrameReaderIntegrationSuite` 加 `spark.read.option("mergeSchema", "true").table(t)` proto 断言：`assert(proto.getRead.getNamedTable.getOptionsMap.containsKey("mergeSchema"))`。
- **预估**: 1 小时（3 site 修复 + 3 个测试断言 + Connect proto path `Read.NamedTable.options` field 验证）
- **价值**: HIGH。`table(...)` 是用户最常用的数据源入口之一，静默丢弃 user-set options 致 schema 演化 / streaming offset 控制 / 数据格式微调全部失效——失败模式不可观测（无错误、结果集偏离），用户难以排查根因。
- **关联**: 与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 而非 `Dataset[String]` / R68 `DataFrameReader.textFile` 返回 `DataFrame` 同 `DataFrameReader` / `DataStreamReader` 公共 API 偏离族系——R55/R68 是 reader 路径 typed-return 契约偏离（用户从上游迁移代码编译失败），R82 是 reader 路径 user-set options 静默丢失（用户预期数据格式偏离），三者覆盖 reader 入口契约偏离的不同维度（typed-return + options pass-through）。与 R29 `SparkSession.failPendingObservations` 跨查询观测污染 / R47 `Catalog.getCreateTableString(asSerde=true)` 文法位置错放 / R48 `DataFrame.union` 默认 `isAll=false` / R50 `DataFrameWriter.jdbc` 缺 partitionBy 校验 / R70 `GroupedDataFrame.{mean,max,min,sum}()` 无参 no-op 同 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R82 + R50 / R70 同属 "user-set 配置/调用静默丢失" 子分支，0.4.0 集中修复可形成 silent-violation family 完整批量 release。Round 49 completeness 维度新增。

---

#### R86. `Dataset[T].writeTo(table)` 返回 untyped `DataFrameWriterV2` —— 类型参数 `T` 在 V2 写入路径被丢弃（`DataFrameWriterV2` 类未参数化为 `[T]`，与上游 `Dataset[T].writeTo` 返回 `DataFrameWriterV2[T]` 契约偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/Dataset.scala:649`、`src/main/scala/org/apache/spark/sql/DataFrameWriterV2.scala:15`
- **问题**: SC3 `Dataset[T].writeTo(table)` 委托至底层 `DataFrame.writeTo(table)`：
  ```scala
  // Dataset.scala:649
  def writeTo(table: String): DataFrameWriterV2 = df.writeTo(table)
  ```
  返回类型 `DataFrameWriterV2`（非泛型），且 `DataFrameWriterV2` 类签名为：
  ```scala
  // DataFrameWriterV2.scala:15
  final class DataFrameWriterV2 private[sql] (table: String, df: DataFrame) extends OptionBuilder[DataFrameWriterV2]:
  ```
  类未参数化为 `[T]`。对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala:3202` `def writeTo(table: String): DataFrameWriterV2[T]` 与 `DataFrameWriterV2[T]`，SC3 缺失类型参数链路。后果：用户 `ds.as[Person].writeTo("t").append()` 链路上 `DataFrameWriterV2` 不再持有 `Person` 类型信息，下游 `partitionedBy(...)` / `using(...)` 等若需 typed Column（上游通过 `Dataset[T]` 隐式提供 typed 列引用语义）则 SC3 无法支持；用户从上游迁移代码 `val w: DataFrameWriterV2[Person] = ds.writeTo("t")` 直接编译失败。
- **建议**: 改造 `DataFrameWriterV2` 为参数化类（`final class DataFrameWriterV2[T] private[sql] (...)`），同时调整 `Dataset[T].writeTo` 与 `DataFrame.writeTo` 各自返回 `DataFrameWriterV2[T]` / `DataFrameWriterV2[Row]`：
  ```scala
  // DataFrameWriterV2.scala
  final class DataFrameWriterV2[T] private[sql] (table: String, df: DataFrame)
      extends OptionBuilder[DataFrameWriterV2[T]]:
    // 现有方法签名调整为返回 DataFrameWriterV2[T]
  // Dataset.scala:649
  def writeTo(table: String): DataFrameWriterV2[T] = new DataFrameWriterV2[T](table, df)
  // DataFrame.scala（DataFrame 是 Dataset[Row] 的 type alias）
  // 链路自动收敛为 DataFrameWriterV2[Row]
  ```
  补 `DataFrameWriterV2Suite` 编译期类型断言：`val w: DataFrameWriterV2[Person] = ds.as[Person].writeTo("t")` 编译通过。
- **预估**: 1 小时（DataFrameWriterV2 改泛型 + 现有调用点签名传播 + 类型断言测试）
- **价值**: HIGH。`writeTo`(V2 path) 是 Spark 4.x 推荐的 Iceberg/Delta 写入入口，typed return 是上游公开 API 契约——SC3 偏离致使用户从上游 / Spark 主线迁移 `DataFrameWriterV2[T]` 代码 100% 编译失败，且无法用 typed `Column` 调用 `partitionedBy`。
- **关联**: 与 R55 `DataStreamReader.textFile` / R57 `Dataset.toJavaRDD` typed-elision / R60 `DataStreamWriter.toTable` typed-loss / R65 `Dataset.checkpoint` typed-loss / R67 `Dataset.mergeInto` 返回 untyped `MergeIntoWriter` / R68 `DataFrameReader.textFile` typed-elision / R75 `Dataset.{filter,where,as[U]}` typed-loss / R77 `Dataset.transform` typed-elision 同 "Dataset/Reader/Writer 公共 API typed-return 契约偏离" 大 family——R86 是 V2 writer 路径 typed-class 缺失，R67 是 V1 mergeInto writer 路径 typed-elision，二者共同覆盖 SC3 writer 子系统 typed-API 缺失子族系。0.4.0 集中修复可闭合 typed-return 大 family 完整子集（reader / streaming / V1 V2 writer / dataset 转换）。Round 50 completeness 维度新增。

---

#### R87. `Encoders` 工厂缺失 `bean(Class[T])` / `kryo[T]` / `javaSerialization[T]` / `row(StructType)` 4 个上游公开入口（用户依赖 `Encoders.bean(classOf[Person])` 等模式从上游迁移代码 100% 编译失败，且无法对自定义 POJO/Java Bean 类型构造 Encoder）

- **位置**: `src/main/scala/org/apache/spark/sql/Encoders.scala:1-280`（仅 line 203 `def row: Encoder[Row] = wrap(AgnosticEncoders.UnboundRowEncoder)`）
- **问题**: SC3 `Encoders` 当前仅提供 primitive / time / Product / Tuple / `row` 等内置 encoder 工厂方法，未实现：
  - `def bean[T](beanClass: Class[T]): Encoder[T]`（上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/Encoders.scala:264`）：基于 Java Bean 反射 introspect getter/setter 构造 Encoder。
  - `def kryo[T: ClassTag]: Encoder[T]`（上游同文件 line 218）：基于 Kryo 序列化的 fallback Encoder。
  - `def javaSerialization[T: ClassTag]: Encoder[T]`（上游同文件 line 238）：基于 Java 内置序列化的 fallback Encoder。
  - `def row(schema: StructType): Encoder[Row]`（上游同文件 line 251）：带 schema 绑定的 Row Encoder，区别于 SC3 现有无参 `row: Encoder[Row]`（上游 line 211 的 unbound row）。
  对照上游 4 个工厂入口 SC3 全部缺失。后果：（1）用户从上游迁移 `import org.apache.spark.sql.Encoders; val e = Encoders.bean(classOf[Person])` 直接编译失败；（2）无法对没有 `Product` trait 的纯 Java Bean 类构造 typed Encoder（SC3 现有 `Encoders.product[T <: Product: TypeTag]` 仅适配 case class / Tuple，无 bean 路径）；（3）跨服务边界经 Kryo/Java serialization fallback 的高级用法（如 `Dataset[T].as[Other](Encoders.kryo[Other])`）完全不可用。
- **建议**: 补齐 4 个工厂入口；`bean` 通过 `JavaTypeInference.encoderFor(Class[T])` 构造 `JavaBeanEncoder`，`kryo` / `javaSerialization` 通过 `AgnosticEncoders.{KryoEncoder,JavaSerializationEncoder}` wrapper 构造，`row(StructType)` 通过 `AgnosticEncoders.RowEncoder(schema)` wrapper：
  ```scala
  // Encoders.scala（在现有 row 工厂附近添加）
  def bean[T](beanClass: Class[T]): Encoder[T] =
    wrap(JavaTypeInference.encoderFor(beanClass))
  def kryo[T: ClassTag]: Encoder[T] =
    wrap(AgnosticEncoders.KryoEncoder(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]))
  def javaSerialization[T: ClassTag]: Encoder[T] =
    wrap(AgnosticEncoders.JavaSerializationEncoder(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]))
  def row(schema: StructType): Encoder[Row] =
    wrap(AgnosticEncoders.RowEncoder(schema))
  ```
  补 `EncodersSuite` 测试断言每个工厂返回的 Encoder schema 与上游一致（bean Person → schema 含 name/age 字段、kryo 任意 Person → schema 为 binary、row(StructType) 含 explicit fields）。
- **预估**: 4-5 小时（4 个工厂 + AgnosticEncoders 端 KryoEncoder/JavaSerializationEncoder/RowEncoder wrapper 检验 + JavaTypeInference 端 bean 反射可用性 + 单元测试 schema 断言）
- **价值**: HIGH。`Encoders.{bean,kryo,javaSerialization,row}` 是上游公开 API 用户最常用的 Encoder 构造入口（SQL programming guide / Spark Examples 多处引用），SC3 全部缺失致使（1）从上游迁移 Java/Scala 代码 100% 编译失败、（2）跨语言互操作（Java POJO + Scala client）无法构造 Encoder、（3）fallback 序列化路径（用户自定义类型不属于 Product / case class）无可用方案——直接堵死大量真实生产场景。
- **关联**: 与 R56 `UDFRegistration.registerJava` Java 互操作公共 API 缺失 / R69 `DataFrameReader.options(ju.Map[String,String])` Java-interop 缺失 / R71 `DataFrameWriter.partitionBy` 单元素重载缺失 / R78 `DataFrameWriter.options(ju.Map[String,String])` Java-interop 缺失 / R79 `Dataset.{drop,withColumns}(ju.Map[String,Column])` Java-interop 缺失 / R80 `Catalog.{tableExists,functionExists,databaseExists}(database, name)` 双参重载缺失 同 "Java-interop / 公共 API 入口缺失" 大 family——R56 / R69 / R71 / R78 / R79 / R80 是单 site 入口缺失，R87 是 Encoders 工厂入口批量缺失（4 个 site 同时缺失）。0.4.0 集中补齐可闭合 Java-interop family 完整子集（UDF / Reader / Writer / Catalog / Encoders 五大子系统）。Round 50 completeness 维度新增。

---

#### R88. `Encoder[T]` / `Dataset[T]` / `Row` 均未 `extends Serializable` —— 上游 `Aggregator extends Serializable` 声明 `Encoder[T]` 字段时，SC3 用户实例化 `Aggregator` 子类经任何序列化通道（哪怕 Aggregator 自定义 client-only 路径）触发 NotSerializableException（结构性 missing-Serializable 系统性偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/Encoder.scala:15`、`src/main/scala/org/apache/spark/sql/Dataset.scala:37`、`src/main/scala/org/apache/spark/sql/Row.scala:12`
- **问题**: SC3 三个核心类型均未 extends `Serializable`：
  ```scala
  // Encoder.scala:15
  trait Encoder[T]:
  // Dataset.scala:37
  final class Dataset[T: ClassTag] private[sql](...):
  // Row.scala:12
  final class Row private (private val values: IndexedSeq[Any], val schema: Option[StructType] = None):
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/Encoder.scala:69` `trait Encoder[T] extends Serializable`、`Dataset.scala:125` `class Dataset[T] ... extends Serializable`、`Row.scala:144` `trait Row extends Serializable`，上游三者 `extends Serializable` 是契约性声明。后果：（1）用户继承 `org.apache.spark.sql.expressions.Aggregator[IN, BUF, OUT]`（上游 `Aggregator extends Serializable` 且 `def bufferEncoder: Encoder[BUF]` / `def outputEncoder: Encoder[OUT]` 声明 `Encoder` 字段）实例化时，JVM 序列化检查链路触发 `NotSerializableException: org.apache.spark.sql.Encoder` 即时失败；（2）`Row` 通过 `collect()` 拿回客户端后用户经 `ObjectOutputStream` 经任何 RPC / 缓存 / 持久化通道（即便客户端 client-only 模式仍可能用 Java 序列化做缓存层 fallback）触发 `NotSerializableException: org.apache.spark.sql.Row`；（3）`Dataset` 实例传入用户自定义高阶函数（client-only `mapPartitions` 客户端预处理钩子）触发同样错误。注：尽管 SC3 是 Connect client（lambda 实际通过 proto 跨 RPC 序列化、不依赖 JVM 内置 Java serialization），但 `Aggregator extends Serializable` 是上游 trait declaration——子类继承后只要触碰 JVM 默认序列化路径（如用户自定义缓存 / Map 容器存放 Aggregator 实例 / Hibernate-style 持久化）即触发，且与上游契约不一致致用户从上游迁移代码无法在客户端层做任何对象图序列化测试（即便不经 Spark RPC）。
- **建议**: 三者均添加 `extends Serializable` 与上游契约对齐：
  ```scala
  // Encoder.scala:15
  trait Encoder[T] extends Serializable:
  // Dataset.scala:37
  final class Dataset[T: ClassTag] private[sql](...) extends Serializable:
  // Row.scala:12
  final class Row private (...) extends Serializable:
  ```
  补 `EncoderSerializableSuite` / `DatasetSerializableSuite` / `RowSerializableSuite` 用 `ObjectOutputStream` 序列化往返断言（注：`Dataset[T]` 序列化需注意 `SparkSession` 字段是否可序列化——可通过 `@transient lazy val` 等模式延迟初始化）。
- **预估**: 2-3 小时（三类型签名修改 + transient/serial-friendly 字段重构 if needed + 序列化往返测试）
- **价值**: HIGH。这是 Spark API 公共契约的结构性偏离——`Encoder` / `Dataset` / `Row` 三大核心类型未 extends `Serializable` 将所有依赖 `Aggregator` 子类 / `Row` 序列化 / `Dataset` 序列化的用户代码（即便 client-only）从契约层面切断，且不像 R86/R87 那样仅影响特定场景，而是结构性影响所有 typed-API 入口。0.4.0 必修才能让 SC3 自称为上游 API 兼容客户端。
- **关联**: 与 R55-R75-R77-R86 Dataset/Reader/Writer typed-return 契约偏离 / R56-R69-R71-R78-R79-R80-R87 Java-interop 公共 API 缺失 同 "上游 API 公共契约结构性偏离" 大主题——R86/R87 是入口签名缺失（typed return / 工厂入口缺失），R88 是入口存在但结构性 supertype 缺失（Serializable trait extension 缺失），三者覆盖 API 契约偏离的三个不同维度。0.4.0 R86/R87/R88 三项联动修复可形成 "上游 API 公共契约结构性对齐" 主题完整批量 release。Round 50 completeness 维度新增（新 family：missing-Serializable 结构性 typing 维度）。

---

### 🟡 MEDIUM

#### R3. `Row.getDecimal` 文档化 MatchError
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:58-61`
- **问题**: 当前 scaladoc 描述了三种 case，但未说明非 `Number` / `BigDecimal` 输入会抛 `MatchError`（无 `case _` 兜底）。
- **建议**: scaladoc 增加 `@throws MatchError if value is not BigDecimal or Number`。
- **预估**: 2 min
- **价值**: API 契约清晰；与其他 `getXxx` 方法（用 `asInstanceOf` 抛 `ClassCastException`）行为不同需说明。

#### R4. RetryPolicySuite deadline 测试稳定性
- **文件**: `src/test/scala/org/apache/spark/sql/connect/client/RetryPolicySuite.scala:135-159`
- **问题**: 使用真实 `Thread.sleep(50)` + 30ms budget。CI 慢时（GC/调度抖动）首次 sleep 前已超 30ms，可能导致 `attempts == 1` 而测试断言 `attempts >= 2`。同时 `maxRetries=100` 配真 sleep，最坏路径会跑数秒。
- **建议**: 改为注入式时钟（`currentTimeMillis: () => Long`），budget 与 sleep 都在测试控制下；并加上界断言（如 `attempts < 5`）以便预算被忽略时快速失败。
- **预估**: 10-15 min
- **价值**: 消除 flake 风险，保留 deadline 优先于 maxRetries 的回归保护。

#### R5. `Row.copy()` schema 丢失 + `equals`/`getAs(name)` 语义不一致
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:158`、`Row.scala:182-184`、`Row.scala:49-53`
- **问题**: `copy()` 静默丢弃 schema；`equals` 也不比较 schema；但 `getAs(fieldName)` 必须有 schema。结果：两个 `equals` 为 true 的 Row 可能一个能按字段名访问、另一个抛 `UnsupportedOperationException`。
- **选项**:
  - A. 保留 schema：`copy()` 改为 `Row.fromSeqWithSchema(toSeq, schema.orNull)` 或分支处理
  - B. 文档化为已知行为（在 `copy` 与 `equals` 的 scaladoc 中互相引用）
- **预估**: 5 min（A）/ 3 min（B）
- **价值**: 避免 `row.copy().getAs("x")` 抛异常的 surprise；显式契约。
- **关联**: 与 R27 在 "`copy()` 丢失 schema" 维度根因重叠——R5 把该缺陷置于 `equals`/`getAs(name)` 三联不对称的更广语境中讨论，R27 独立给出最小修复 `def copy(): Row = this`（因 `Row` 为 `final` + immutable）并附 upstream `GenericRowWithSchema.copy()` 对照；二者保留以分别承载"语境"与"修复路径"两个维度。与 R30 区分——R30 是 deserializer 在嵌套构造时单边丢 schema（外层 Row 仍带 schema），本项围绕外层 `Row.copy()` 主动丢 schema，落点不同。

#### R6. 属性测试缺少 allocator 泄漏断言
- **文件**: `src/test/scala/org/apache/spark/sql/ArrowRoundTripSuite.scala`
- **问题**: 27 个 property × ~30 次 = ~800 次 `fromArrowBatchWithSchema` 调用，但 suite 结束未断言 root allocator `getAllocatedMemory == 0`。R2 提到的 256GB cap 是安全控制，property test 正是触发 allocator 泄漏的最佳场所。
- **建议**: `BeforeAndAfterAll` 中持有共享 allocator，`afterAll` 断言已分配字节归零。
- **预估**: 15 min
- **价值**: 回归保护——decoder 资源管理改动会立即在 property test 失败。

#### R7. `Row.equals`/`hashCode` 对 `Array[Byte]` 字段走引用相等
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:182-186`
- **问题**: `equals` 委托给 `IndexedSeq.equals`，对 `Array[Byte]` 元素使用 JVM 引用比较；两行二进制内容相同的 Row 仍 `!=`，`hashCode` 也不同。`ArrowRoundTripSuite` 之所以引入自定义 `deepEqual`（`ArrowRoundTripSuite.scala:32-44`）即为绕开此问题。R5 仅覆盖 schema 不对称，不涵盖 Array 元素语义。
- **建议**: 二选一：
  - A. `equals`/`hashCode` 内部对 `Array[Byte]`（及任意 `Array`）改用 `java.util.Arrays.equals` / `Arrays.hashCode`
  - B. scaladoc 显式说明限制，并将 `deepEqual` 提升为公共辅助方法
- **预估**: 20 min（A）/ 5 min（B）
- **价值**: 避免 `Set[Row]` / `Map[Row, _]` 在 `BinaryType` 字段下静默失效。
- **关联**: 与 R30 区分——R30 是 deserializer 嵌套 struct 行丢 schema 导致 by-name 访问失败，本项是 `Row.equals`/`hashCode` 在 `Array[Byte]` 元素上引用相等，根因与影响面不同。

#### R8. `GrpcRetryHandler` `RetryException` 分支无 deadline 检查 + `lastException` 可能丢失
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/GrpcRetryHandler.scala:30-32`
- **问题 1（deadline 违反）**: `RetryException` 立即重试且无 backoff、无 `System.nanoTime() >= deadline` 检查。恶意/抖动 server stream 持续抛 `RetryException` 时，会在 `maxRetries`（默认 15）以内空转，违反 scaladoc 行 18-21 声称的 "always enforces `maxTotalDurationMs`" 契约。
- **问题 2（异常丢失）**: 行 31 仅在 `re.getCause != null` 时更新 `lastException`。若所有重试均为 `RetryException(null)`，最终 fallthrough（行 50）抛出的可能是更早一次失败的 stale `lastException`，或在第一次重试即 `RetryException(null)` 时退化为行 49 的 `RuntimeException("Retry loop exhausted...")`，丢失原始失败链。
- **建议**: `RetryException` 分支也应检查 deadline；`re.getCause == null` 时把 `re` 自身记入 `lastException`。
- **预估**: 10 min
- **价值**: 与 R11 对称，消除 retry 路径的两条静默缺陷。

#### R9. `Row.json` / `Row.prettyJson` 对非 String 值产出非法 JSON
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:128-130`、`Row.scala:148-150`
- **问题**: 两处都仅对 `String` 加引号，其余值走裸 `other.toString`。这会对常见类型产生不可解析的输出：
  - `java.sql.Date` → `2026-05-19`（缺引号，不是 JSON 字面量）
  - `java.sql.Timestamp` → `2026-05-19 12:34:56.0`（缺引号，且空格）
  - `Array[Byte]` → `[B@deadbeef`（含 `@`、缺引号，非法）
  - 嵌套 `Row` → `[a,b]`（裸 toString，非法对象/数组语法）
  - `Map` / `Seq` → Scala 集合 toString，与 JSON 不兼容
  - `java.math.BigDecimal` 大数可能输出科学计数 `1E+2`（在 JSON 中 `E+2` 合法但 `1E+2` 写法在某些严格 parser 下需 `+` 移除）
  下游任何 JSON parser 都无法解析这些行，"render this row as a JSON object" 的契约对非 String 字段事实上失效。
- **建议**: 类型感知编码：
  - 数值/布尔：直接 `toString`（已合法）
  - 字符串/Date/Timestamp/UUID：引号包裹并 escape（Date/Timestamp 用 ISO-8601 字符串）
  - `Array[Byte]`：base64 字符串
  - `BigDecimal`：`.toPlainString` 后裸输出（合法 JSON 数字）
  - 嵌套 `Row`：递归 `json`
  - `Seq` / `Map`：递归生成 JSON 数组 / 对象
  - `null` 字面量：`null`（已正确）
- **预估**: 30-45 min（含测试）
- **价值**: 修复公共 API 的正确性；当前任何含 Date/Timestamp/Binary/嵌套 Struct 字段的 Row 序列化即破坏下游消费者。

#### R18. `GrpcRetryHandler` `Thread.sleep` 抛 `InterruptedException` 后未恢复中断标志
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/GrpcRetryHandler.scala:43,45-46`
- **问题**: 行 43 `sleep(backoff + jitter)` 默认绑定到 `Thread.sleep`。Java 规范下 `Thread.sleep` 抛出 `InterruptedException` 时会**清除**当前线程的 interrupt 标志位。该异常不匹配 `policy.canRetry`（默认仅匹配 `StatusRuntimeException`），落入行 45 的 `case e: Throwable => throw e` catch-all，直接 rethrow 但**未调用 `Thread.currentThread.interrupt()` 恢复标志**。后续在 cooperative-cancellation 调用栈（gRPC executor / structured concurrency / `Future` 取消传播）上的 `isInterrupted()` 检查会看到 false，导致中断信号丢失——同一线程随后的可中断操作将不再响应原中断请求。
- **建议**: 在行 45 catch-all 之前插入 `case ie: InterruptedException => Thread.currentThread.interrupt(); throw ie`；或在 catch-all 中对 `InterruptedException` 做特判。注意：`re: RetryException` 分支（行 30）与 `canRetry` 分支（行 33）也应在调用 `sleep` 前后保持中断标志可见性，但因这两条分支与中断无 sleep 交互，最小修复仅作用于 catch-all。
- **预估**: 5 min（含测试：注入 `sleep` 回调抛 `InterruptedException`，断言外层 catch 后 `Thread.interrupted()` 为 true）
- **价值**: 防御性。Java 并发编程的标准 idiom；当前丢标志位是隐式行为，对依赖中断协议的调用栈造成静默副作用。

#### R19. `Row.fromSeqWithSchema` 不校验 `values.size == schema.fields.size`（JSON 静默丢字段 / 越界 IOOB）
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:197-199,208-209`（构造器）；触发位点 `Row.scala:124,144,161-166`（json/prettyJson/getValuesMap）
- **问题**: 公共构造器 `fromSeqWithSchema(values, schema)` 与包私 `fromSeqDirectWithSchema` 直接持有 `values` 与 `schema`，不校验长度一致。后续行为分两路静默退化：
  - **`values.size > schema.fields.size`（多值）**：`json`/`prettyJson` 在行 124、144 上对 `s.fields.zipWithIndex` 迭代，仅渲染前 `fields.size` 个值——尾部超出字段个数的 `values` 在 JSON 输出中静默丢失，且无任何警告。`getValuesMap` 同理仅遍历用户给定的 `fieldNames`，超出值同样消失。
  - **`values.size < schema.fields.size`（少值）**：`json`/`prettyJson` 在 `i == values.size` 时调 `get(i)` 抛 `IndexOutOfBoundsException`；`anyNull`、`getAs(fieldName)` 在访问越界字段时同样炸——错误延迟到访问时刻而非构造时刻，调试链路被拉长。
  与 R5（`copy()` 丢 schema）/ R7（`Array[Byte]` 引用相等）属同一组 Row 契约缺陷家族，但此项是真实数据丢失而非语义不对称，因此独立列出。
- **建议**: 在 `fromSeqWithSchema` 与 `fromSeqDirectWithSchema` 入口加 `require(values.size == schema.fields.size, s"values.size=${values.size} does not match schema.fields.size=${schema.fields.size}")`；同步在 `RowSuite` 加正/负两条回归（多值丢失、少值抛 IOOB → require）。`ArrowDeserializer` 内部调用始终长度一致，新增校验对热路径无影响。
- **预估**: 10 min（含测试）
- **价值**: 公共 API 的 fail-fast 契约。当前 `Row.fromSeqWithSchema(Seq("a", "b", "c"), schemaWith2Fields).json` 静默吞掉 "c"，下游消费者无从察觉；构造时立即拒绝是唯一防止数据丢失的稳态修复。

#### R21. `TimeStampNanoVector` 解码静默截断至毫秒（亚毫秒精度丢失）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:101-102`
- **问题**: `case v: TimeStampNanoVector => java.sql.Timestamp(v.get(index) / 1000000L)`：将 `nanoseconds-since-epoch` 整数除以 1_000_000 得到毫秒，再用 `Timestamp(long)` 构造（仅 millis 精度）。Java `Timestamp` 本身支持 nanosecond 精度（通过 `setNanos(int)`），但当前实现完全丢弃 0–999_999 ns 的尾数。例：
  - 输入 `nanos = 1592197200000000999` →  `/ 1_000_000` = 1592197200000 → `Timestamp.getTime` = 1592197200000，`getNanos` = 0
  - 原始 999 ns 永久丢失；同一 vector 的 round-trip（即使 server 回传相同精度）观察到精度截断。
  对照：`TimeStampMicroVector` / `TimeStampMicroTZVector` 路径走 `microsToTimestamp(micros)`，正确通过 `Instant.ofEpochSecond(seconds, nanoOfSec)` 保留 nanosecond 精度（行 92-96 与 helper）。Nano 路径与 Micro 路径不对称，纯属遗漏。
  影响范围：当前 Spark Connect server 默认走 MicroTZ，故该路径在标准 server 数据流上不触发；但 `ArrowDeserializer.fromArrowBatchWithSchema` 是公共 API，可被外部 Arrow 数据源（其他语言客户端、自构 Arrow batch、第三方 connector）调用——任何使用 `TimeStampNanoVector` 的 producer 在通过此 client 解码时丢失亚毫秒精度。`TimeStampSecVector`（行 104-105）走 `* 1000L`，秒级精度无损失，正常。
- **建议**: 改为
  ```scala
  case v: TimeStampNanoVector =>
    val n   = v.get(index)
    val sec = Math.floorDiv(n, 1_000_000_000L)
    val rem = Math.floorMod(n, 1_000_000_000L).toInt
    java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(sec, rem.toLong))
  ```
  （`floorDiv`/`floorMod` 处理负值的 epoch-pre-1970 nanos）。同步在 `ArrowDeserializerSuite` 增 1 条 `TimeStampNanoVector` 单元测试（构造一批含尾数 ns 的 nano 时间戳，断言 round-trip 后 `getNanos` 与原值一致）。
- **预估**: 15 min（含测试）
- **价值**: 公共 API 正确性。属与 R20（Date 时区）同一族"vector-specific 解码不对称"问题——前者跨 TZ 静默漂移、本项跨精度静默截断，影响范围比 R20 窄但模式相同。

#### R23. `ArrowSerializer.DecimalVector` 写入路径不归一化 `BigDecimal` scale（与上游 `ArrowWriter.DecimalWriter` 偏离，常见用户输入抛 Arrow 内部异常）
- **文件**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:184-189`
- **问题**: 当前 `DecimalVector` 写路径直接 `v.setSafe(idx, bd)`，不调用 `setScale` / `changePrecision` 归一化。Arrow 19 `DecimalVector.setSafe(int, BigDecimal)` → `set(int, BigDecimal)` → `DecimalUtility.checkPrecisionAndScale(bd, precision, scale)` 当 `bd.scale() != vectorScale` 时直接抛 `UnsupportedOperationException("BigDecimal scale must equal that in the Arrow vector: <bd-scale> != <vector-scale>")`（已对照 arrow-vector-19.0.0 源码 `DecimalUtility.java` 与 `DecimalVector.java` 验证）。常见用户输入即触发：
  - `Row(java.math.BigDecimal("99"))` against `DecimalType(10,2)`：scale=0 ≠ 2 → 抛
  - `Row(java.math.BigDecimal("99.999"))` against `DecimalType(10,2)`：scale=3 ≠ 2 → 抛
  - `Row(BigDecimal("99.9"))` against `DecimalType(10,2)`：scale=1 ≠ 2 → 抛
  - `Row("100")` against `DecimalType(10,2)`：String 兜底走 `BigDecimal(value.toString)` → scale=0 → 抛
  对照上游 `org.apache.spark.sql.execution.arrow.ArrowWriter.DecimalWriter.setValue`（`/spark-mine-12/sql/catalyst/src/main/scala/org/apache/spark/sql/execution/arrow/ArrowWriter.scala:265-282`）：先 `decimal.changePrecision(precision, scale)`（HALF_UP 重新缩放），若超精度则 `setNull` 而非抛——本 client 与上游契约背离。
  现有测试 `ArrowSerializerSuite.scala:147-166` 与 `ArrowRoundTripSuite.scala:96-99` 均使用 scale 已对齐的 fixture（`"123.45"`/`"99.99"`/`"42.00"` 与 `BigDecimal.valueOf(unscaled, 2)`），故零回归覆盖；任何用户提供整数或非对齐 scale 的 BigDecimal 都会以 confusing 的 Arrow 内部 stack trace 失败。
- **建议**: 在 `case v: DecimalVector =>` 内做 scale 归一化与 precision 检查：
  ```scala
  case v: DecimalVector =>
    val bd0 = value match
      case d: BigDecimal           => d.underlying()
      case d: java.math.BigDecimal => d
      case _                       => java.math.BigDecimal(value.toString)
    val rescaled = bd0.setScale(v.getScale, java.math.RoundingMode.HALF_UP)
    if rescaled.precision <= v.getPrecision then v.setSafe(idx, rescaled)
    else v.setNull(idx) // 与上游 changePrecision-overflow → null 一致
  ```
  同步在 `ArrowSerializerSuite` 增 4 条单元（scale<schema、scale>schema、整数 BigDecimal、String "100" against `DecimalType(10,2)`）；并在 `ArrowRoundTripSuite` 加可选生成器，刻意产出错位 scale 验证 round-trip 落到 setScale 后的规范形式。
- **预估**: 20-30 min（含测试）
- **价值**: 公共 API 正确性 + 上游契约对齐。"loud failure" 比静默腐蚀好，但用户拿到的是 Arrow 内部抛栈而非清晰的 Spark 类型错误，且与 upstream Spark `createDataFrame(Row(BigDecimal("99")), schema=DecimalType(10,2))` 行为不一致——已迁移到 Connect 客户端的应用会在常见输入上崩。

#### R24. `ArrowSerializer.setArrowValue` 缺失 `DurationVector`/`IntervalYearVector`/`TimeMicroVector` 写分支（schema 路径已声明、解码端已支持，编码端在公共 API 入口直接报"Unsupported"）
- **文件**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:102-107`、`160-260`（catch-all 257-260）；对照 `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:140-143`、`224-225`
- **问题**: `sparkTypeToArrow:102-107` 显式声明三类映射（`DayTimeIntervalType → ArrowType.Duration(MICROSECOND)`、`YearMonthIntervalType → ArrowType.Interval(YEAR_MONTH)`、`TimeType → ArrowType.Time(MICROSECOND, 64)`），`sparkTypeToArrowField:144-150` 对它们走通用 fall-through，意味着 `VectorSchemaRoot.create` 会实例化对应 vector；但 `setArrowValue:160-256` 的 case 列表只覆盖 BitVector/TinyIntVector/SmallIntVector/IntVector/BigIntVector/Float4Vector/Float8Vector/VarCharVector/DateDayVector/TimeStampMicroTZVector/TimeStampMicroVector/DecimalVector/VarBinaryVector/MapVector/ListVector/StructVector，未处理 `DurationVector`/`IntervalYearVector`/`TimeMicroVector`，最终落到 257-260 的 catch-all `throw UnsupportedOperationException(s"Unsupported Arrow vector type: ${other.getClass.getName}")`。这条路径在公共 API 完全可达：`Dataset.collect()` 不会触发（仅解码方向），但 `createDataFrame(rows, schema)` / `Encoders.DURATION` / `Encoders.PERIOD` 会——用户 `createDataFrame(Seq(Row(java.time.Duration.ofSeconds(1))), StructType(Seq(StructField("d", DayTimeIntervalType()))))` 直接抛。同时 `ArrowDeserializer:140-143` 已为 `DurationVector` 与 `IntervalYearVector` 实现了解码、`arrowTypeToSparkType:224-225` 也将 `ArrowType.Duration → DayTimeIntervalType`、`ArrowType.Interval → YearMonthIntervalType` 反向映射回去——证明 Duration/Interval 的往返意图存在，但编码端漏写（属编码端单边缺失）。`TimeType` 情况不同：编码端缺 `TimeMicroVector` 分支的同时，解码端 `arrowTypeToSparkType` 亦无 `ArrowType.Time` case（落到 line 228 的 `case _ => StringType` 默认分支）、`extractValue:70-160` 也无 `TimeMicroVector`/`TimeNanoVector`/`TimeSecVector`/`TimeMilliVector` case（落到 `getObject` 兜底）——属编/解码两端均缺失，需同步补齐。
- **建议**: 在 `setArrowValue` 加三个 case：
  ```scala
  case v: DurationVector => value match
    case d: java.time.Duration =>
      v.setSafe(idx, d.getSeconds * 1_000_000L + d.getNano / 1000L)
    case n: Number => v.setSafe(idx, n.longValue())
  case v: IntervalYearVector => value match
    case p: java.time.Period =>
      v.setSafe(idx, p.toTotalMonths.toInt)
    case n: Number => v.setSafe(idx, n.intValue())
  case v: TimeMicroVector => value match
    case t: java.time.LocalTime =>
      v.setSafe(idx, t.toNanoOfDay / 1000L)
    case n: Number => v.setSafe(idx, n.longValue())
  ```
  并在 `ArrowSerializerSuite` 加 round-trip 单元覆盖三类（与 `ArrowDeserializerSuite` 已有解码用例对齐）。
- **预估**: 30-40 min（含三类 round-trip 测试）
- **价值**: 公共 API 入口可用性。当前 `sparkTypeToArrow` 与 `ArrowDeserializer` 已为这三类铺好路径，缺这三个 case 等于 schema/解码端宣称支持但写入端在 catch-all 处崩——典型不一致。修复成本低、收益是补完编/解码闭环并解锁 `Encoders.DURATION`/`Encoders.PERIOD` 公共契约。

#### R25. `ArrowSerializer.MapVector` 不校验 null key（与上游 `Objects.requireNonNull(key)` 偏离，产出违反 Arrow 不变量的恶意 IPC buffer）
- **文件**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:131-143`（schema 声明）+ `:193-215`（写入路径）+ `:160-163`（null 短路）
- **问题**: `sparkTypeToArrowField:131-143` 在构造 `MapType` 对应的 Arrow `Field` 时显式声明 `keyField = sparkTypeToArrowField("key", mt.keyType, false)`（即 keys 子向量 `nullable=false`，line 132），同时外层 `entriesField` 也是 `nullable=false`（line 136）——这是 Arrow Map 规范的不变量。但 `setArrowValue:193-215` 对 `MapVector` 的写路径直接对每个 entry 调 `setArrowValue(keyVec, offset + i, key, mt.keyType)`：当 `key == null` 时会落到顶层 `setArrowValue:160-163` 的通用空值短路 `if value == null then vec.setNull(idx)`，在一个声明为非空的子向量上写 validity-bit=0，违反 Arrow IPC contract。这条路径在公共 API 完全可达：用户 `createDataFrame(Seq(Row(Map[String,Int]((null.asInstanceOf[String], 1), ("a", 2)))), schema=StructType(Seq(StructField("m", MapType(StringType, IntegerType)))))` 不会在 client 端报错，反而生成一个 server 端可能拒收（grpc parse error）、可能解析为合法 Map(null → 1)（解码端 `ArrowDeserializer.MapVector` 不区分 keys 的 null bit）、亦可能引发未定义行为的 batch。对照上游 `spark-mine-12/sql/connect/common/.../ArrowSerializer.scala:538-541`：
  ```scala
  private def extractKey(v: Any): Any = {
    val key = v.asInstanceOf[(Any, Any)]._1
    Objects.requireNonNull(key)
    key
  }
  ```
  上游在 serializer 入口 fail-fast，本 client 漏写。`ArrowRoundTripSuite` 与 `ArrowSerializerSuite` 均无 null-key Map 用例，零回归覆盖。
- **建议**: 在 `MapVector` 分支两条 case（Scala `Map` 与 Java `java.util.Map`）的 foreach 内、`setArrowValue(keyVec, ...)` 之前加显式校验：
  ```scala
  case v: MapVector =>
    val mt = dt.asInstanceOf[types.MapType]
    val dataVec = v.getDataVector.asInstanceOf[StructVector]
    val keyVec = dataVec.getChildByOrdinal(0).asInstanceOf[FieldVector]
    val valVec = dataVec.getChildByOrdinal(1).asInstanceOf[FieldVector]
    val offset = v.startNewValue(idx)
    value match
      case m: Map[?, ?] =>
        var i = 0
        m.foreach { (key, valu) =>
          if key == null then
            throw NullPointerException(s"Map key must not be null (column type=${mt.keyType})")
          setArrowValue(keyVec, offset + i, key, mt.keyType)
          setArrowValue(valVec, offset + i, valu, mt.valueType)
          i += 1
        }
        v.endValue(idx, i)
      // 同样处理 java.util.Map 分支
  ```
  并在 `ArrowSerializerSuite` 加单元：`createDataFrame(Seq(Row(Map[String,Int]((null.asInstanceOf[String], 1)))), schema)` 应抛 NPE。
- **预估**: 5-10 min（含测试）
- **价值**: 公共 API 正确性 + Arrow IPC 不变量保护 + 上游契约对齐。"loud failure" 比静默生成 malformed buffer 好——后者最坏情况是 server 端拿到一个看似合法实则违反 keys-nonnull 不变量的 batch，错误信息会被埋在 grpc/Arrow stack 深处而非 client 侧的清晰 NPE。

#### R26. `ArrowDeserializer` 缺失 `TimeStampMilliTZVector`/`TimeStampSecTZVector`/`TimeStampNanoTZVector` 解码 case（schema 宣称 `TimestampType`、值落到 `getObject` 兜底返回 `Long`）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:92-105`（现有 case 列表）+ `:155-160`（catch-all `getObject`）+ `:194` schema 映射
- **问题**: 现有 Timestamp 解码 case 列表为 `TimeStampMicroVector` / `TimeStampMicroTZVector` / `TimeStampMilliVector` / `TimeStampNanoVector` / `TimeStampSecVector`——只有 micro 一族提供了 TZ 兄弟（`TimeStampMicroTZVector`）。`TimeStampMilliTZVector` / `TimeStampSecTZVector` / `TimeStampNanoTZVector` 没有显式 case，落入 155-160 catch-all 的 `try v.getObject(index) catch ... null`，而 Arrow `TimeStamp{Milli,Sec,Nano}TZVector.getObject` 返回 `java.lang.Long`（毫秒/秒/纳秒原值），不是 `java.sql.Timestamp`。同时 `arrowTypeToSparkType:194` 将所有 `ArrowType.Timestamp`（无视 unit、有 timezone 即映射为 `TimestampType`）声明为 `TimestampType`——`Row.getTimestamp(i)` 后续访问会在该值上抛 `ClassCastException: class java.lang.Long cannot be cast to class java.sql.Timestamp`。这与 R21（`TimeStampNanoVector` 静默截至毫秒）是同族但不同 vector：R21 是值精度漂移、R26 是返回类型与 schema 不一致。
- **可达性**: Spark Connect server 通常将 TimestampType 归一化到 micro+UTC，但 Arrow IPC 协议本身不强制 unit；任何在 server 侧透传非 micro Timestamp（用户自定义 Arrow source、第三方 connect-extension、未来 server 优化）都会让 client 在无任何错误信息的情况下静默掉精度并在访问时崩。Round 21 已为 `TimeStampNanoVector` 做了精度截断（接受漂移），按照同一治理思路三个 TZ 兄弟也应有显式 case——保持解码行为对称。
- **建议**: 镜像现有非 TZ case：
  ```scala
  case v: TimeStampMilliTZVector =>
    java.sql.Timestamp(v.get(index))
  case v: TimeStampNanoTZVector =>
    java.sql.Timestamp(v.get(index) / 1000000L)
  case v: TimeStampSecTZVector =>
    java.sql.Timestamp(v.get(index) * 1000L)
  ```
  注：与 R21（NanoVector 截断至毫秒）保持一致；如未来 R21 改为微秒精度，三处 TZ 兄弟同步升级。`ArrowDeserializerSuite` 加 round-trip 单元覆盖三类（构造 schema 含 timezone 的 second/milli/nano Timestamp）。
- **预估**: 15-20 min（含三类单元测试）
- **价值**: 公共 API 正确性 + 解码端对称性。修复成本低、收益是补完非 micro Timestamp 的 TZ 路径——尤其当 server 侧或第三方 source emit 这类 vector 时，从静默崩溃 → 显式精度归一化。

#### R27. `Row.copy()` 丢失 schema（与 upstream `GenericRowWithSchema.copy()` 偏离，副本上的 schema-dependent 方法无法工作）
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:157-158`
- **问题**: `def copy(): Row = Row.fromSeq(toSeq)` 走 `Row.fromSeq` 路径——该 factory 只接 `values`、不接 schema，因此返回的 `Row` 内 `schema = None`。所有依赖 schema 的方法（`getAs(name)` / `fieldIndex` / `json` / `prettyJson` / `getValuesMap`）在副本上都会抛 `UnsupportedOperationException("... requires a Row with schema")`，即便原始 Row 是从 `Dataset.collect()` / `Row.fromSeqWithSchema` 构造、确实带 schema。当前 doc comment 明示 "The schema is dropped"——是 documented 行为但与 upstream Spark `GenericRowWithSchema.copy()`（返回 `this` 因 Row 不可变、保留 schema）契约不一致。任何把 collect 结果做 transformation pipeline 中包含 `.map(_.copy())` 的代码都会在下游 `getAs("name")` 处崩。
- **建议**: 因 `Row` 在本 client 是 `final class` 且字段全 immutable（`values: IndexedSeq[Any]`、`schema: Option[StructType]`），`copy()` 可直接返回 `this`：
  ```scala
  /** Shallow copy of this row. The original is returned because [[Row]] is immutable. */
  def copy(): Row = this
  ```
  或保守路径：保留 schema 显式构造新实例：
  ```scala
  def copy(): Row =
    schema match
      case Some(s) => Row.fromSeqWithSchema(toSeq, s)
      case None    => Row.fromSeq(toSeq)
  ```
  并更新 `RowSuite` 加单元：`Row.fromSeqWithSchema(Seq(1), schema).copy().json` 应正常返回 JSON 而非抛。
- **预估**: 5-10 min（含测试 + doc comment 更新）
- **价值**: 公共 API 正确性 + upstream 契约对齐。当前 doc comment 已自承 "The schema is dropped" ——但消费者从 upstream Spark 迁移时按约定假设 `.copy()` 是身份操作（保留所有元数据），这种"已文档化的行为偏离"是隐式陷阱。
- **关联**: 与 R5 在 "`copy()` 丢失 schema" 维度根因重叠——R5 把同一缺陷与 `equals`（不比较 schema）+ `getAs(name)`（必须有 schema）一并讨论为三联不对称问题；本项独立列出以承载 (a) 最小修复 `def copy(): Row = this` 路径与 (b) upstream `GenericRowWithSchema.copy()` 对照的细节。修复任一项时建议同步检查另一项以保契约一致。与 R30 区分——R30 是 deserializer 在嵌套构造时单边丢 schema，本项围绕外层 `Row.copy()` 丢 schema，根因不重叠。

#### R30. `ArrowDeserializer` 嵌套 struct 行丢失 schema（按字段名访问内层 struct 抛 `UnsupportedOperationException`，与外层行 `fromSeqDirectWithSchema` 路径不对称）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:138`（嵌套构造）vs. `:55`（外层构造）
- **问题**: `extractValue` 在 `StructVector` 分支上用 `Row.fromSeq(meta.children.map(...))`（行 138）构造内层 struct 行——该路径走 `Row.fromSeq`，不传 schema，故返回的 `Row` 内 `schema = None`。而同文件外层 row 走 `Row.fromSeqDirectWithSchema(values, schema)`（行 55）携带完整 schema。结果：用户 `df.collect().head.getStruct(0)` 拿到无 schema 的内层 Row，调用 `.getAs[String]("name")` / `.fieldIndex("name")` / `.json` / `.prettyJson` / `.getValuesMap(Seq("a"))` 全部抛 `UnsupportedOperationException("... requires a Row with schema")`（出处 `Row.scala:53/107/135/155/169`）。任何 DataFrame 含 `StructType` 列且下游按字段名访问内层结构的代码都触发——这是嵌套数据建模常见路径（`select(struct("a","b").as("nested")).collect().head.getStruct(0).getAs[Int]("a")`）。与 R5/R7/R27 的根因不同：那三项都围绕 `Row.copy()` 在外层 Row 上丢 schema，本项是 deserializer 在嵌套构造时单边丢 schema，外层 Row 自身是 schema-attached 的。
- **建议**: 在 `StructVectorMeta` 缓存中一并缓存 inner `StructType`（一次性从 StructVector 的 Arrow `Field` 经 `arrowTypeToSparkType` 推导即可，因 `arrowTypeToSparkType` 已能 dispatch 到 struct 子节点构造），将 `case class StructVectorMeta(isVariant: Boolean, children: Seq[FieldVector])` 扩为 `case class StructVectorMeta(isVariant: Boolean, children: Seq[FieldVector], schema: StructType)`，并把行 138 改为 `Row.fromSeqWithSchema(meta.children.map(f => extractValue(f, index, structMetaCache)), meta.schema)`。Variant 分支不变。同步在 `ArrowDeserializerSuite` 加 1 条嵌套 struct 单元：构造 `StructType(Seq(StructField("outer", StructType(Seq(StructField("inner", StringType))))))` 的 IPC，断言 `decoded.head.getStruct(0).getAs[String]("inner")` 不抛、`decoded.head.getStruct(0).json` 返回有效 JSON。
- **预估**: 20-30 min 实现 + 15-20 min 单元测试（含至少 1 条嵌套 struct + by-name 访问 + JSON 渲染断言）
- **价值**: 公共 API 正确性 + 外/内 Row 构造路径对称化。当前不对称使 `.json` / `getAs(name)` 在嵌套场景"看似 OK 实则不可用"，与 upstream Spark `GenericRowWithSchema` 的嵌套 row 也带 schema 的契约偏离。

#### R32. `ArrowDeserializer.arrowTypeToSparkType` 对 `ArrayType` / `MapType` 的子节点 `containsNull` / `valueContainsNull` 硬编码为 `true`（schema 保真度损失）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:200,221`
- **问题**: Arrow → Spark 类型推导中：
  ```scala
  case _: ArrowType.List =>
    ArrayType(arrowTypeToSparkType(children.head), containsNull = true)   // 行 200，忽略 children.head.isNullable

  case _: ArrowType.Map =>
    MapType(
      arrowTypeToSparkType(children(0)),
      arrowTypeToSparkType(children(1)),
      valueContainsNull = true                                              // 行 221，忽略 children(1).isNullable
    )
  ```
  对照 `Struct` 分支（行 209）已正确传播 `child.isNullable` 到 `StructField`。`Array` / `Map` 分支硬编码 `true` 导致 schema 在 round-trip 后失真：server 显式声明 `ArrayType(IntegerType, containsNull = false)` 的列在 client 端被还原为 `ArrayType(IntegerType, containsNull = true)`。下游基于 schema 做 "保证元素非空" 推断的代码（如 typed Dataset / 用户自定义 schema 校验 / 第三方 datasource 持久化重映射）会丢失非空保证，可能跳过原本应做的 null check 或反向触发不必要的 null-handling 路径。MapType 上同理影响 value-containsNull 不变量。
- **建议**: 与 Struct 分支对称传播 child nullability：
  ```scala
  case _: ArrowType.List =>
    ArrayType(arrowTypeToSparkType(children.head), containsNull = children.head.isNullable)

  case _: ArrowType.Map =>
    MapType(
      arrowTypeToSparkType(children(0)),
      arrowTypeToSparkType(children(1)),
      valueContainsNull = children(1).isNullable
    )
  ```
  并在 `ArrowDeserializerSuite` 加 schema fidelity 单元：构造 `ArrayType(IntegerType, containsNull = false)` / `MapType(StringType, IntegerType, valueContainsNull = false)` 的 Arrow IPC（writer 端显式写 non-null child field），断言 `fromArrowBatchWithSchema` 还原 schema `containsNull == false` / `valueContainsNull == false`。
- **预估**: 5-10 min（含测试）
- **价值**: 关闭 ArrayType / MapType schema round-trip 的 nullability 保真度损失，与 Struct 分支对称化；同时与 R26（缺失 TZ-bearing Timestamp vector case，schema 还原但解码兜底）形成 schema-fidelity 一对——R26 是值还原失真，本项是 schema 元信息失真。
- **关联**: 与 R30（嵌套 struct 行 schema 丢失）区分——R30 是 Row 实例的 schema 关联缺失，本项是 schema 结构内部 `containsNull` flag 失真，落点不同维度。与 R35 区分——R35 是 schema → DDL 字符串渲染缺字段（公共 API 输出层），本项是 Arrow round-trip 中 nullability 元信息失真（deserializer 内部）；两者均属 schema-fidelity 族系但落点不同。

#### R33. `ExecutePlanResponseReattachableIterator.next()` 不检查 `resultComplete`（与 `hasNext` 不对称，post-completion `next()` 行为未定义）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ExecutePlanResponseReattachableIterator.scala:78-109`
- **问题**: `hasNext`（行 78-96）正确短路 `if closed then return false; if resultComplete then return false`；`next()`（行 98-109）仅检查 `closed`，缺失 `resultComplete` gate：
  ```scala
  override def next(): ExecutePlanResponse =
    if closed then throw java.util.NoSuchElementException("Iterator is closed")
    // ← 缺失：if resultComplete then throw NoSuchElementException("ResultComplete already received")
    retryHandler.retry { ... callIter(_.next()) ... }
  ```
  正常 `hasNext` / `next` 配对的调用方不会触达此路径——`hasNext` 在 `resultComplete = true` 后返回 `false`，循环退出。但直接调用 `next()` 而不先 `hasNext`（如手写 `while (true) { try iter.next() catch case _: NoSuchElementException => break }`）的代码，在收到 `ResultComplete` 后再次调用 `next()` 时：行 105 `releaseAll()` 已释放底层资源，`callIter(_.next())` 触发 `if iter == null then iter = rawReattachExecute()`（行 124-126），可能再次发起 `ReattachExecute` gRPC——浪费 server 资源 + 可能引发 `OPERATION_NOT_FOUND` 异常 + `RetryException` 重试，最终错误信息指向 retry 链路而非 "iterator already exhausted" 本意。即使 `iter` 未被 nullify，调用 `iter.next()` 也会抛底层 gRPC `NoSuchElementException`，错误消息不同于 `hasNext` 的 "已完成" 语义。
- **建议**: 与 `hasNext` 对称添加 gate：
  ```scala
  override def next(): ExecutePlanResponse =
    if closed then throw java.util.NoSuchElementException("Iterator is closed")
    if resultComplete then
      throw java.util.NoSuchElementException("ResultComplete already received; iterator exhausted")
    retryHandler.retry { ... }
  ```
  并在测试中加：构造一个 `ResultComplete` 已经被消费的 iterator，断言后续 `next()` 抛 `NoSuchElementException` 且消息含 "ResultComplete"，而不是触发 reattach 或下游 gRPC 异常。
- **预估**: 5 min（含测试）
- **价值**: 关闭 `hasNext` / `next` 状态视图不对称，避免 post-completion `next()` 触发非预期 reattach；错误消息从 "底层 gRPC 抛异常" 收窄为 "iterator 已耗尽"，调用方错误处理路径更清晰。
- **关联**: 与 R29 区分——R29 是 SparkSession-level 跨查询观测污染，本项是 iterator-level 状态视图不对称；并发 ↔ 顺序、共享 ↔ 局部，落点不同。

#### R35. `StructType` 未 override `def sql`，嵌套 struct 列 DDL 渲染丢字段（与 `ArrayType` / `MapType` 不对称，破坏 `df.schema.toDDL` round-trip）
- **文件**: `src/main/scala/org/apache/spark/sql/types/DataType.scala:130-131`
- **问题**: Base `trait DataType` 的 `sql` 默认实现为 `def sql: String = typeName.toUpperCase`（`:9`）；同文件 `ArrayType.sql = s"ARRAY<${elementType.sql}>"`（`:120`）、`MapType.sql = s"MAP<${keyType.sql},${valueType.sql}>"`（`:126`）正确 override；但 `StructType`（`:130-131`）只声明 `def typeName = "struct"`，未 override `sql`，继承默认实现返回字面字符串 `"STRUCT"`——丢失全部字段：
  ```scala
  final case class StructType(fields: Seq[StructField]) extends DataType:
    def typeName = "struct"
    // 缺失：override def sql = s"STRUCT<${fields.map(...).mkString(", ")}>"
  ```
  `StructType.toDDL`（`:176-181`）逐字段调用 `f.dataType.sql` 拼装 DDL，所以含嵌套 struct 的列渲染为 `` `b` STRUCT `` 等不合法字符串，无法 round-trip 经 `DataType.fromDDL` 解析。`MAP<STRING, STRUCT<...>>` / `ARRAY<STRUCT<...>>` 等外层复合类型的 `sql` 实现内部也调用 `valueType.sql` / `elementType.sql`，递归把嵌套 struct 的字段丢失传播到外层 DDL。常见用户路径 `df.select(struct("a","b").as("nested")).schema.toDDL` 直接受影响。
- **建议**: 在 `StructType` 加 override 与同文件 `ArrayType`/`MapType` 对称：
  ```scala
  final case class StructType(fields: Seq[StructField]) extends DataType:
    def typeName = "struct"
    override def sql: String =
      s"STRUCT<${fields.map(f => s"${f.name}: ${f.dataType.sql}").mkString(", ")}>"
  ```
  并在 `DataTypeSuite` 加回归：(a) `StructType(Seq(StructField("a", IntegerType), StructField("b", StringType))).sql == "STRUCT<a: INT, b: STRING>"`；(b) `MapType(StringType, StructType(...))` / `ArrayType(StructType(...))` 嵌套场景；(c) `StructType(...).toDDL` 含嵌套 struct 列的字段名 / 类型完整保留。
- **预估**: 5-10 min（含测试）
- **价值**: 关闭 schema DDL 渲染保真度损失，恢复 `df.schema.toDDL` 在含 struct 列的 DataFrame 上的 round-trip 能力。当前 `toDDL` 输出是事实上的 silent corruption——下游基于 DDL 字符串 reconstruct schema / 写入 metadata store / 生成 CREATE TABLE 语句的代码全部受影响。
- **关联**: 与 R32 区分——R32 是 schema 内部 `containsNull` flag 在 Arrow round-trip 中失真（运行时元信息）；本项是 schema → DDL 字符串渲染缺字段（公共 API 输出）。两者均为 schema-fidelity 族系但落点不同：R32 在 deserializer 内部，本项在用户 API 边界。

#### R36. `functions.rand` / `functions.randn` 默认 `seed=0L` 致不同查询产出相同伪随机序列（与上游 `random.nextLong` 偏离）
- **文件**: `src/main/scala/org/apache/spark/sql/functions.scala:200-201`
- **问题**: 两签名都用 Scala 默认参数 `seed: Long = 0L`：
  ```scala
  def rand(seed: Long = 0L): Column  = callFn("rand",  Column.lit(seed))
  def randn(seed: Long = 0L): Column = callFn("randn", Column.lit(seed))
  ```
  无参调用 `rand()` / `randn()` 始终展开为 `rand(0L)` / `randn(0L)`，server 端 `Rand(0L)` 在每个 query 起点重置同一 seed，产出**完全相同**的伪随机序列。对照上游 `spark-mine-12/sql/api/.../functions.scala:2416-2452`：
  ```scala
  def rand(seed: Long): Column = Column.fn("rand", lit(seed))
  def rand(): Column           = rand(SparkClassUtils.random.nextLong)
  def randn(seed: Long): Column = Column.fn("randn", lit(seed))
  def randn(): Column           = randn(SparkClassUtils.random.nextLong)
  ```
  上游每次无参调用注入新随机 seed。可观测影响：(a) `df.sample(0.1).collect()` 客户端多次调用得到固定行子集（与上游不一致）；(b) `withColumn("noise", randn())` 多 epoch 注入恒定噪声，破坏 ML 正则化语义；(c) `df.withColumn("a", rand()).withColumn("b", rand())` 两列 a/b 完全相等（同 seed=0），用户期望两列独立；(d) `when(rand() < 0.5, "A").otherwise("B")` A/B 划分跨执行不变。注意 `random()`（行 235-236）走 `callFn("random")` 无 seed 参数，server 自行生成——该路径无问题；仅 `rand`/`randn` 显式 `lit(0L)`。
- **建议**: 移除默认值并改为重载，与上游对齐：
  ```scala
  def rand(seed: Long): Column  = callFn("rand",  Column.lit(seed))
  def rand(): Column            = rand(scala.util.Random.nextLong())
  def randn(seed: Long): Column = callFn("randn", Column.lit(seed))
  def randn(): Column           = randn(scala.util.Random.nextLong())
  ```
  默认参数转重载是源兼容变更（call-site `rand()` 仍可编译），但二进制不兼容（默认参数 synthetic accessor `rand$default$1` 消失）——可借下一个 minor bump 同步落地。同步在 `FunctionsSuite` 加回归：两次构造 `select(rand())` proto，断言 `lit` 参数 seed 不同。
- **预估**: 10-15 min（两行替换 + 单测 + `MiMaFiltering` 排除条目）
- **关联**: 与 R34 同属 "functions.scala 客户端构造表达式缺失上游随机/唯一化注入" 族系——R34 是命名碰撞（每层嵌套需唯一 ID），R36 是 seed 复用（每次调用需新随机数），两者均根源于 SC3 将客户端表达式构造做得过于无状态而丢失上游 `nextLong` / `nextId` 状态注入；R34 修复路径用 `AtomicLong` 单调计数器，R36 用 `scala.util.Random` 全局实例，工具不同但定位一致。与 R37 同族系延展（`DataFrame.sample`/`randomSplit` + `Dataset.sample`/`randomSplit` 五个 API 入口同型缺陷），可与 R36 一并落地。Round 39 新增 R49（`KeyValueGroupedDataset.cogroup`/`cogroupSorted` 不使用 mapValues 派生状态致 typed cogroup 路径值类型偏离，HIGH）与本项同属 "客户端表达式构造层与上游分歧" 大类，但 R49 落点在 typed Dataset API dispatch 完整性（缺 `MapValuesCoGroupAdaptor` / `UDFAdaptors.coGroupWithMappedValues` 包装）而非 functions.scala seed 随机化注入；Round 40 consistency 反向引用补齐。

#### R37. `DataFrame.sample` / `DataFrame.randomSplit` / `Dataset.sample`(无 seed) / `Dataset.randomSplit`(无 seed) 默认 `seed=0L` 致无显式 seed 调用产出确定性子集（与上游 `SparkClassUtils.random.nextLong` 偏离）
- **文件**: `src/main/scala/org/apache/spark/sql/DataFrame.scala:261, 621`；连带 `src/main/scala/org/apache/spark/sql/Dataset.scala:260-261, 266-267, 426-427`
- **问题**: `DataFrame` 两签名用 Scala 默认参数 `seed: Long = 0L`：
  ```scala
  // DataFrame.scala:261
  def sample(fraction: Double, withReplacement: Boolean = false, seed: Long = 0L): DataFrame = ...
  // DataFrame.scala:621
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataFrame] = ...
  ```
  `Dataset` 包装层对无 seed 重载也兜底 `0L`：
  ```scala
  // Dataset.scala:260-261
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    sample(withReplacement, fraction, 0L)
  // Dataset.scala:266-267
  def sample(fraction: Double): Dataset[T] =
    Dataset(df.sample(fraction), encoder)   // 落 DataFrame.sample 默认 seed=0L
  // Dataset.scala:426-427
  def randomSplit(weights: Array[Double]): Array[Dataset[T]] =
    randomSplit(weights, 0L)
  ```
  对照上游 `spark-mine-12/sql/api/.../Dataset.scala:2079-2081`：
  ```scala
  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] = {
    sample(withReplacement, fraction, SparkClassUtils.random.nextLong)
  }
  ```
  上游 `sample(fraction)` (`:2043-2045`) 同样递归到 `sample(withReplacement = false, fraction)` 走随机 seed 路径；`randomSplit(weights)` 抽象方法在 connect 子类内部用 random seed 实现。SC3 让所有无 seed 调用 collide 到 `seed=0L`：(a) `df.sample(0.1)` 客户端多次得到**固定行子集**——A/B 划分稳定但 A/B 实验不再随机；(b) `df.randomSplit(Array(0.7, 0.3))` 训练 / 评估集划分跨执行不变，破坏 cross-validation；(c) `df.sample(0.1).join(df2.sample(0.1), ...)` 两侧抽样若 server 同时优化 deterministic 路径，结果跟无随机抽样语义偏离；(d) ML pipeline 重训每次得到相同 train/test split，掩盖 split 敏感的 generalization 问题。与 R36 落 `functions.rand`/`randn` 同根因，仅 API 入口不同。
- **建议**: 移除 `seed: Long = 0L` 默认值，分两签名重载，无参重载内部用 `scala.util.Random.nextLong()`：
  ```scala
  // DataFrame.scala
  def sample(fraction: Double, withReplacement: Boolean, seed: Long): DataFrame = ...
  def sample(fraction: Double, withReplacement: Boolean): DataFrame =
    sample(fraction, withReplacement, scala.util.Random.nextLong())
  def sample(fraction: Double): DataFrame = sample(fraction, withReplacement = false)
  def randomSplit(weights: Array[Double], seed: Long): Array[DataFrame] = ...
  def randomSplit(weights: Array[Double]): Array[DataFrame] =
    randomSplit(weights, scala.util.Random.nextLong())
  ```
  Dataset 层的 `sample(withReplacement, fraction)` / `sample(fraction)` / `randomSplit(weights)` 顺路改 delegate 到对应 DataFrame 无参重载（其内部已经注入随机 seed），不再显式传 `0L`。同源兼容（call-site `df.sample(0.1)` 仍编译），二进制不兼容（默认参数 synthetic accessor 消失）——下个 minor bump 同步落地，并加回归测试断言两次 `df.sample(0.1)` 的 proto seed 不同。
- **预估**: 20-30 min（5 处 API 重载调整 + 单测 + MiMaFiltering 排除条目）
- **关联**: 与 R36 同属 "客户端表达式构造缺失上游随机状态注入" 族系——R36 覆盖 `functions.rand`/`randn` 两个 API 入口，R37 覆盖 `DataFrame.sample`/`randomSplit` + `Dataset.sample`/`randomSplit` 五个 API 入口（共 7 处缺陷点同型修复）；R37 比 R36 影响面更广（每个 ML pipeline / A/B 实验 / cross-validation 都路过这两个 API），但修复路径同构（移除 default 参数 + 无参重载注入随机种子），可与 R36 一并落地。Round 39 新增 R49（`KeyValueGroupedDataset.cogroup`/`cogroupSorted` 不使用 mapValues 派生状态致 typed cogroup 路径值类型偏离，HIGH）与本项同属 "客户端 typed Dataset API 路径与上游分歧" 大类，但 R49 落点在 dispatch 完整性（adaptor 包装缺失）而非 seed 随机化注入；Round 40 consistency 反向引用补齐。

#### R38. `LiteralValueProtoConverter.toScalaValue` TIME 分支返回原始 `Long` 而非 `LocalTime`（`DataFrame.observe` 路径触发，typed accessor / 序列化两条用户可见失真）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala:74-75`
- **问题**: `toScalaValue` 在 `LiteralTypeCase.TIME` 分支直接返回 `literal.getTime.getNano`（一个 `Long`，纳秒计数），而非按 `TimeType` 语义构造 `java.time.LocalTime`：
  ```scala
  // LiteralValueProtoConverter.scala:65-89 (相关分支)
  def toScalaValue(literal: proto.Expression.Literal): Any = literal.getLiteralTypeCase match
    case LiteralTypeCase.NULL      => null
    case LiteralTypeCase.BOOLEAN   => literal.getBoolean
    case LiteralTypeCase.DATE      => DateTimeUtils.daysToLocalDate(literal.getDate)
    case LiteralTypeCase.TIMESTAMP => DateTimeUtils.microsToInstant(literal.getTimestamp)
    case LiteralTypeCase.TIME      => literal.getTime.getNano   // ← 返回 Long，未走 nanosToLocalTime
    ...
  ```
  对照同方法的 `DATE` / `TIMESTAMP` 分支均显式构造正确的 JSR-310 类型，`TIME` 分支孤独返回 raw `Long`。Round 25 曾以 "reachability 极窄" 为由拒绝列入；本轮重新论证发现真实可达路径——`DataFrame.observe` 的 `processObservedMetrics`（`DataFrame.scala:1037`）对每条观测指标值逐条调用 `toScalaValue(lit)` 并材料化进 Row：
  ```scala
  // DataFrame.scala:1032-1042
  val vs = new Array[Any](n)
  ...
  while i < n do
    val lit = valuesList.get(i)
    vs(i) = LiteralValueProtoConverter.toScalaValue(lit)
    fields(i) = StructField(keysList.get(i), LiteralValueProtoConverter.toDataType(lit))
    i += 1
  val schema = StructType(ArraySeq.unsafeWrapArray(fields))
  val row = Row.fromSeqWithSchema(ArraySeq.unsafeWrapArray(vs), schema)
  ```
  用户调用 `df.observe("m", first(timeCol)).collect()` 时 server 返回 TIME literal，客户端 schema 通过 `toDataType` 仍声明为 `TimeType(precision)` 但 row 内放置 `Long`——下游 typed accessor `row.getLocalTime(0)` / `row.getAs[java.time.LocalTime]("m")` 抛 `ClassCastException: java.lang.Long cannot be cast to java.time.LocalTime`；`row.json` 输出整数 token 而非合法 TIME 字符串字面量；`Observation.get()` 返回的 `Map[String, Any]` 含 `Long` 而非 LocalTime，破坏其 typed-extraction 契约。除 observe 外，`Column.expr.literal` / 任何 server-driven literal 通信路径若涉 TIME 同被影响。
- **建议**: TIME 分支构造 LocalTime：
  ```scala
  case LiteralTypeCase.TIME => java.time.LocalTime.ofNanoOfDay(literal.getTime.getNano)
  ```
  或调用上游 `DateTimeUtils.nanosOfDayToLocalTime`（如已 vendored）。同步补回归测试：构造一个 `df.observe("m", first(timeCol))` 的 stub server 返回 TIME literal、断言 `row.getLocalTime(0)` 不抛、值与 server 端原始 LocalTime 一致；并对 `LiteralValueProtoConverter.toScalaValue(timeLit)` 加 unit test。
- **预估**: 10-15 min（一行替换 + observe stub 测试 + LiteralValueProtoConverter unit test）
- **关联**: 与 R20（DateType 编解码 TZ 不对称）/ R26（TZ-bearing TimestampVector 解码缺 case）同属 vector-specific 解码不对称族系，但失败点位于 LiteralValueProtoConverter（observe / retrieved-literal 路径）而非 ArrowDeserializer；R20/R26 影响 schema-claimed 列的 round-trip，R38 影响 observe metrics 与 literal 通信，三者修复路径独立但可视作 "TIME / TZ-bearing 类型解码族系" 一并审视。Round 25 拒绝决策由本项 supersede。Round 36 新增 R44（同方法 INTERVAL 三分支同型缺陷——CALENDAR_INTERVAL / YEAR_MONTH_INTERVAL / DAY_TIME_INTERVAL 返回 raw tuple/Int/Long 而非 JSR-310 / `CalendarInterval`），与本项 / R43 形成 LiteralValueProtoConverter 类型契约族系完整闭环，可一并落地。

#### R39. `StreamingQueryListenerBus.registerServerSideListener` `hasNext=false` 落空兜底返回未确认 iterator（silent registration failure）
- **文件**: `src/main/scala/org/apache/spark/sql/streaming/StreamingQueryListenerBus.scala:68-89`
- **问题**: `registerServerSideListener` 启动 long-running gRPC stream 等待 server 发送 `ListenerBusListenerAdded=true` 确认事件，循环结构如下：
  ```scala
  // StreamingQueryListenerBus.scala:68-89
  private[sql] def registerServerSideListener(): Iterator[ExecutePlanResponse] =
    val cmdBuilder = ...
    val plan = ...
    val iterator = session.client.execute(plan)
    val deadline = System.nanoTime() + RegisterListenerTimeoutNanos
    try
      while iterator.hasNext do
        if System.nanoTime() > deadline then
          ... close iterator ...
          throw java.util.concurrent.TimeoutException("Timed out waiting for server to confirm listener registration")
        val response = iterator.next()
        val result = response.getStreamingQueryListenerEventsResult
        if result.hasListenerBusListenerAdded && result.getListenerBusListenerAdded then
          return iterator    // ← 找到确认 → 正常返回
      iterator               // ← (line 89) hasNext=false 落空 → 返回未确认 iterator
    catch ...
  ```
  当 server 在发送确认前关闭流（panic / EOF / 网络中断 / 服务器主动 cancel），`iterator.hasNext` 转 false，循环正常退出，line 89 直接返回**已耗尽**的 iterator——caller 在 `append`（行 39-44）拿到后启动 `queryEventHandler` 处理线程，该线程的 `while iter.hasNext` 立即 false → 走 finally 关闭 → daemon 线程退出，listener 永不收到任何事件。`listeners.add(listener)` 已经成功，从用户视角注册成功（`StreamingQueryListenerBus.list()` 包含该 listener），但事件流早已断开。与 line 82-83 的 timeout 路径不对称（timeout 抛异常被 line 41-43 catch 并 `listeners.remove(listener)` 回滚，silent fallthrough 不抛异常因此不回滚）。
- **建议**: 在 line 89 显式抛"server closed stream before sending registration confirmation" 异常：
  ```scala
  while iterator.hasNext do
    ... 现有逻辑 ...
  // hasNext=false 落空：server 关闭流但未发确认
  (iterator: Any) match
    case c: AutoCloseable => c.close()
    case _                => ()
  throw java.io.IOException(
    "Server closed listener registration stream before confirmation"
  )
  ```
  补回归：mock client 提供一个空 iterator（无任何 response，hasNext=false），断言 `bus.append(listener)` 抛异常且 `bus.list()` 不含该 listener。
- **预估**: 10-15 min（替换 fallthrough + mock client 单测）
- **价值**: 防御性 + 可观测性。silent failure 会让用户在 streaming pipeline 中调试很久才意识到注册失败；显式异常落 caller 异常处理路径直观。注：与 streaming 路径耦合，coverage 已排除（`build.sbt:142`），属"server-dependent" 类目，但本项是注册阶段同步路径，stub 测试可覆盖。
- **关联**: 与 R22（`SparkSession.close` 不清理 streaming bus + `append` 锁内 30s gRPC）同属 "streaming bus 生命周期 / 错误路径" 族系——R22 是关闭 / 阻塞维度，R39 是注册失败路径未传播维度，三者均围绕 `StreamingQueryListenerBus` 公共 API 的边界条件。与 R54 `StreamingQueryManager.get` 不做 `hasQuery` 检查致返回伪造 `StreamingQuery` 同属 "streaming 公共 API silent fallthrough" 族系——R39 在 listener 注册路径漏抛"未收到确认"异常致 silent 注册失败，R54 在 query lookup 路径漏检 `hasQuery` 致 silent 伪对象返回，二者修复模式同型（增加 `hasXxx` 检查 → 抛错或返回 null）。与 R57 `StreamingQuery.recentProgress`/`lastProgress`/`exception` 返回 raw String 而非 typed 类型同属 streaming 子系统但维度不同（R22 是 lifecycle / R39 是注册路径 / R54 是 query lookup / R57 是返回类型契约），R22/R39/R54/R57 形成 streaming 公共 API 边界条件 + typed contract 完整族系。

#### R42. `ArrowDeserializer.extractValue` `IntervalYearVector` 分支返回原始 `Int` 而非 `java.time.Period`（schema 宣称 `YearMonthIntervalType`、值落到 raw int，typed accessor 抛 `ClassCastException`）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:143-144`
- **问题**: `extractValue` 对 `IntervalYearVector` 的分支只返回原始 int：
  ```scala
  // ArrowDeserializer.scala:143-144
  case v: IntervalYearVector =>
    v.get(index)
  ```
  `IntervalYearVector.get(index)` 返回的是底层 little-endian int（month 数），而同文件 `arrowTypeToSparkType` 对 `ArrowType.Interval` 的映射为 `YearMonthIntervalType`：
  ```scala
  // ArrowDeserializer.scala:225
  case _: ArrowType.Interval    => YearMonthIntervalType
  ```
  schema 把列声明为 `YearMonthIntervalType`，但每行该列的值是 `java.lang.Integer`，与上游 Spark `IntervalYearVectorReader.getPeriod(i) = vector.getObject(i).normalized()` 返回 `java.time.Period` 偏离。下游用户按 schema 期望调用 `row.getAs[java.time.Period](colIdx)` / `row.getAs[Period](colName)` 立即抛 `ClassCastException: java.lang.Integer cannot be cast to java.time.Period`，而 `row.get(colIdx)` 拿到 raw int 也无法直接做"年-月"语义运算，round-trip 编码端再次写入则因类型不匹配抛 `ArrowSerializer` 端异常。
- **建议**: 让 `IntervalYearVector` 分支返回 `Period`：
  ```scala
  case v: IntervalYearVector =>
    if v.isNull(index) then null
    else java.time.Period.ofMonths(v.get(index)).normalized()
  ```
  或直接走 `v.getObject(index).normalized()`（与上游一致）。同步补回归测试：构造 `YearMonthIntervalType` schema + IPC stream，断言 `row.getAs[java.time.Period](0)` 不抛、值与 server 端原始 Period 一致；并补 round-trip 测试验证 encode-decode 同构。
- **预估**: 5-10 min（一行替换 + decoder 单测 + round-trip 回归）
- **价值**: 公共 API 类型契约一致性。schema 声明 `YearMonthIntervalType` 是用户契约，typed accessor 必须工作。
- **关联**: 与 R26（TZ-bearing TimestampVector 解码 case 缺失，schema 声明 `TimestampType` / 值兜底为 raw `Long`）/ R38（`LiteralValueProtoConverter.toScalaValue` TIME 分支返回 raw `Long`、observe 路径用户拿不到 `LocalTime`）同属 "vector-specific decoder 类型不对称族系"——schema 契约声明的逻辑类型与 decoder 返回的物理类型不匹配；R26 缺 case 走 `getObject` 兜底回 `Long`，R42 有 case 但用了"raw `get` 而非 typed `getObject().normalized()`"，R38 在 `LiteralValueProtoConverter` 而非 `ArrowDeserializer`；三者修复路径独立但可视作同族系一并审视。Round 35 completeness 维度新增。Round 36 新增 R44（`LiteralValueProtoConverter.toScalaValue` 同方法 `CALENDAR_INTERVAL`/`YEAR_MONTH_INTERVAL`/`DAY_TIME_INTERVAL` 三分支返回 raw tuple/Int/Long）—— R42 在 `ArrowDeserializer` 路径暴露 INTERVAL year-month decode 类型不对称，R44 在 `LiteralValueProtoConverter` observe / literal 路径暴露同语义不对称，两者共享 INTERVAL 类型契约缺陷，可与 R26/R38 一并审视。

#### R43. `LiteralValueProtoConverter.toScalaValue` STRUCT 分支构造 schema-less `Row.fromSeq(values)`（与 R30 同源但落点在 LiteralValueProtoConverter 路径，外层 `df.observe` 即使把 metric Row 包装为 schema-bearing，其 STRUCT 字段值仍为 schema-less `Row`）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala:54-57`
- **问题**: `toScalaValue` 处理 `LiteralTypeCase.STRUCT` 时直接走 `Row.fromSeq(values)`，丢失内层 struct 的 schema：
  ```scala
  // LiteralValueProtoConverter.scala:54-57
  case LiteralTypeCase.STRUCT =>
    val s = literal.getStruct
    val values = s.getElementsList.asScala.map(toScalaValue).toSeq
    Row.fromSeq(values)
  ```
  下游 `DataFrame.scala:1037` 在 `observe` 路径上对每个 metric value 调用 `toScalaValue`，line 1042 把外层 metric Row 用 `Row.fromSeqWithSchema` 包装为 schema-bearing：
  ```scala
  // DataFrame.scala:1032-1043
  val vs = new Array[Any](n)
  ...
  while i < n do
    val lit = valuesList.get(i)
    vs(i) = LiteralValueProtoConverter.toScalaValue(lit)   // ← line 1037：内层 STRUCT 已是 schema-less Row
    fields(i) = StructField(keysList.get(i), LiteralValueProtoConverter.toDataType(lit))
    i += 1
  val schema = StructType(ArraySeq.unsafeWrapArray(fields))
  val row = Row.fromSeqWithSchema(ArraySeq.unsafeWrapArray(vs), schema)  // ← 外层包装，但内层未传 schema
  ```
  外层 `Row.fromSeqWithSchema` 不会递归进入嵌套 struct value，因此 reachable case 形如 `df.observe("m", first(struct("a","b").as("nested")))`：用户拿到 `obs.get()("m")` 是 schema-bearing Row 没问题，但进一步 `.asInstanceOf[Row].getAs[Int]("a")` 立即抛 `UnsupportedOperationException("getAs by field name requires a Row with schema")`（与 `Row.scala:53` 一致）；只有 `getAs[Int](0)` / `get(0)` 这类按位置访问的路径能工作。与 R30 同源——在 `ArrowDeserializer` 的 collect 路径，嵌套 struct 也会丢 schema，由 R30 描述并独立修复；R43 是同族系在 `LiteralValueProtoConverter` 的 observe / retrieved-literal 路径。
- **建议**: 让 STRUCT 分支构造 schema-bearing Row：
  ```scala
  case LiteralTypeCase.STRUCT =>
    val s = literal.getStruct
    val values = s.getElementsList.asScala.map(toScalaValue).toSeq
    // 同时构造 inner schema，与 toDataType 路径对齐
    val fields = s.getElementsList.asScala.zip(s.getNamesList.asScala).map { (lit, name) =>
      StructField(name, toDataType(lit))
    }.toArray
    Row.fromSeqWithSchema(values.toIndexedSeq, StructType(fields))
  ```
  注意需要确认 STRUCT proto 是否 carry 字段名（`s.getNamesList`，与 `DataFrame.scala:1042` 外层包装路径同构）；如不可用则 fallback 用位置编号 `_i` 命名。同步补回归测试：observe + first(struct) 路径，断言 `nestedRow.getAs[Int]("a")` 不抛。
- **预估**: 10-15 min（STRUCT 分支重写 + observe 嵌套 struct 单测 + 与 R30 共享的 schema-bearing Row 路径回归）
- **价值**: 公共 API 类型契约对称性。R30 在 `ArrowDeserializer` 路径修复后，observe 路径若不修则用户依然踩坑；同族系问题应一并修复。
- **关联**: 与 R30（`ArrowDeserializer` 嵌套 struct 行丢失 schema）同属 "嵌套 struct schema-bearing Row 族系"——R30 在 collect 路径，R43 在 observe / literal 路径；与 R31（`Row.getList`/`getJavaMap` null 字段间接 NPE）/ R5（`Row.copy()` schema 丢失）/ R27（`Row.copy()` schema 丢失，与 upstream `GenericRowWithSchema.copy()` 偏离）同属 "Row 公共 API schema 契约族系"；与 R38（`LiteralValueProtoConverter.toScalaValue` TIME 分支 raw Long）同文件，可一并审视 LiteralValueProtoConverter 各分支的类型契约对称性。Round 35 completeness 维度新增。Round 36 新增 R44（`LiteralValueProtoConverter.toScalaValue` 同方法 `CALENDAR_INTERVAL`/`YEAR_MONTH_INTERVAL`/`DAY_TIME_INTERVAL` 三分支返回 raw tuple/Int/Long）—— R43 在 STRUCT 分支暴露 schema 缺失问题，R44 在 INTERVAL 三分支暴露类型契约偏离问题，两者同处 `LiteralValueProtoConverter.toScalaValue` 方法体，与 R38 共同构成"LiteralValueProtoConverter 类型契约族系"，可一并审视该方法体所有分支。

#### R44. `LiteralValueProtoConverter.toScalaValue` `CALENDAR_INTERVAL`/`YEAR_MONTH_INTERVAL`/`DAY_TIME_INTERVAL` 三分支返回原始 tuple/Int/Long 而非 JSR-310 类型（与 schema 宣称的 `CalendarIntervalType`/`YearMonthIntervalType`/`DayTimeIntervalType` 异步，typed accessor 抛 `ClassCastException`，observe 路径用户可见）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/common/LiteralValueProtoConverter.scala:58-64`（连带 `:140-142` 的 schema 映射）
- **问题**: `toScalaValue` 在三个 INTERVAL 分支均返回 proto 原始字段，未按对应 Spark 类型语义构造：
  ```scala
  // LiteralValueProtoConverter.scala:58-64
  case LiteralTypeCase.CALENDAR_INTERVAL =>
    val ci = literal.getCalendarInterval
    (ci.getMonths, ci.getDays, ci.getMicroseconds)   // ← 返回 (Int, Int, Long) tuple
  case LiteralTypeCase.YEAR_MONTH_INTERVAL =>
    literal.getYearMonthInterval                      // ← 返回 Int（month 数）
  case LiteralTypeCase.DAY_TIME_INTERVAL =>
    literal.getDayTimeInterval                        // ← 返回 Long（microsecond 数）
  ```
  而同方法的 schema 映射（`toDataType:140-142`）声明这些 literal 类型为正式的 Spark interval 类型：
  ```scala
  // LiteralValueProtoConverter.scala:140-142
  case LiteralTypeCase.CALENDAR_INTERVAL   => CalendarIntervalType
  case LiteralTypeCase.YEAR_MONTH_INTERVAL => YearMonthIntervalType
  case LiteralTypeCase.DAY_TIME_INTERVAL   => DayTimeIntervalType
  ```
  对照上游 `spark-mine-12/sql/.../LiteralValueProtoConverter.scala:405-415` 走 `SparkIntervalUtils.microsToDuration` / `monthsToPeriod` / `new CalendarInterval(months, days, micros)` 构造正确的 JSR-310 / `CalendarInterval` 对象。reachable 路径与 R38/R43 同走 `DataFrame.observe`：用户调用 `df.observe("m", first(yearMonthIntervalCol)).collect()`，client 端 schema 通过 `toDataType` 声明为 `YearMonthIntervalType` 但 row value 是 `java.lang.Integer`，下游 `obs.get()("m").asInstanceOf[Row].getAs[java.time.Period]("ymInterval")` 抛 `ClassCastException: java.lang.Integer cannot be cast to java.time.Period`；`DayTimeIntervalType` 同型抛 `Long → Duration` ClassCastException；`CalendarIntervalType` 抛 `Tuple3 → CalendarInterval` ClassCastException。除 observe 外，任何 server-driven literal 通信路径若涉 INTERVAL 同被影响。
- **建议**: 三分支构造正确类型：
  ```scala
  case LiteralTypeCase.CALENDAR_INTERVAL =>
    val ci = literal.getCalendarInterval
    new CalendarInterval(ci.getMonths, ci.getDays, ci.getMicroseconds)
  case LiteralTypeCase.YEAR_MONTH_INTERVAL =>
    java.time.Period.ofMonths(literal.getYearMonthInterval).normalized()
  case LiteralTypeCase.DAY_TIME_INTERVAL =>
    java.time.Duration.of(literal.getDayTimeInterval, java.time.temporal.ChronoUnit.MICROS)
  ```
  或调用 vendored `SparkIntervalUtils.microsToDuration` / `monthsToPeriod`（如已可用）。同步补回归测试：构造 observe 路径含三种 INTERVAL 列的 stub server 返回，断言 `obs.get()("m").asInstanceOf[Row].getAs[java.time.Period|Duration|CalendarInterval](...)` 不抛、值与 server 端原始类型一致；并对 `LiteralValueProtoConverter.toScalaValue` 三 INTERVAL literal 各加 unit test。
- **预估**: 15-20 min（三分支构造 + observe 三路径 stub 测试 + LiteralValueProtoConverter unit tests）
- **价值**: 公共 API 类型契约一致性。schema 声明 INTERVAL 类型是用户契约，typed accessor 必须工作；与 R38（TIME 分支同方法）/ R43（STRUCT 分支同方法）形成 LiteralValueProtoConverter 类型契约族系完整闭环。
- **关联**: 与 R38（`LiteralValueProtoConverter.toScalaValue` TIME 分支返回 raw `Long`）/ R43（同方法 STRUCT 分支构造 schema-less Row）同方法、同根因——LiteralValueProtoConverter 各 case 在 typed-accessor 入口未走对应 JSR-310 / Spark 类型构造，三者修复路径同型可一并落地；与 R42（`ArrowDeserializer` `IntervalYearVector` 分支返回 raw Int 而非 Period）同属 "INTERVAL 类型解码 schema/值类型不对称族系"——R42 在 ArrowDeserializer Arrow 路径，R44 在 LiteralValueProtoConverter observe / literal 路径，落点不同但 typed-accessor 失败模式一致。Round 36 completeness 维度新增。

#### R47. `Catalog.getCreateTableString` 拼接 `SHOW CREATE TABLE AS SERDE \`name\`` 把 `AS SERDE` 放在表名之前（与 `SqlBaseParser.g4:334` 文法 `SHOW CREATE TABLE identifierReference (AS SERDE)?` 偏离，server 端解析失败抛 `ParseException`）
- **文件**: `src/main/scala/org/apache/spark/sql/Catalog.scala:159-161`
- **问题**: `getCreateTableString` 把 `asSerde=true` 时的 SQL 关键词整体前置：
  ```scala
  // Catalog.scala:159-161
  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    val keyword = if asSerde then "SHOW CREATE TABLE AS SERDE" else "SHOW CREATE TABLE"
    session.sql(s"$keyword ${quoteIdent(tableName)}").collect().head.getString(0)
  ```
  `asSerde=true` 路径产生的 SQL 字面量为：
  ```sql
  SHOW CREATE TABLE AS SERDE `myDb.myTable`
  ```
  但上游 `spark-mine-12/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4:334` 的文法是：
  ```
  | SHOW CREATE TABLE identifierReference (AS SERDE)?      #showCreateTable
  ```
  `AS SERDE` 必须作为后缀跟在 `identifierReference`（表名）之后。SC3 当前拼接顺序违反文法，`session.sql` 调用一发送到 server 立即抛 `ParseException: Syntax error at or near 'AS'`，对应 SQLState `42000`。用户调用 `spark.catalog.getCreateTableString("hiveTbl", asSerde=true)`（hive serde 表常见入口）会立即看到 SQL 解析失败错误，不能用于 hive serde 路径（这是该方法 `asSerde` 参数存在的唯一目的）。
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala`：upstream 该方法走 `session.sql(s"SHOW CREATE TABLE ${quoteIdent(tableName)} ${if (asSerde) "AS SERDE" else ""}")`，把 `AS SERDE` 后置；与 g4 文法一致。
- **建议**: 调整拼接顺序：
  ```scala
  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    val suffix = if asSerde then " AS SERDE" else ""
    session.sql(s"SHOW CREATE TABLE ${quoteIdent(tableName)}$suffix").collect().head.getString(0)
  ```
  补回归：构造 hive serde 表 + asSerde=true，断言调用不抛 `ParseException`、返回字符串以 `CREATE TABLE` 起始；asSerde=false 路径同步保留单测。
- **预估**: 5 min（一行拼接 + 双路径单测）
- **价值**: MEDIUM。修复后 `asSerde=true` 路径才真正可用。当前实现属于"参数存在但路径不通"，是公共 API 上的功能性缺陷。
- **关联**: 与 R29 / R34 / R36 / R37 / R41 同属 "客户端 SQL / 表达式构造与上游分歧" 大类——但 R47 是 Catalog 层 SQL 字面量拼接顺序错误，根因独立于 functions / GroupedDataFrame 表达式构造分支；与 R39（StreamingQueryListenerBus.registerServerSideListener silent registration failure）同属 "客户端发起的 server 调用因构造错误静默失败"族系，但 R47 是显式 ParseException 而非 silent failure，用户层易于发现。Round 37 completeness 维度新增。Round 40 新增 R50（`DataFrameWriter.jdbc` 缺 `assertNotPartitioned`/`assertNotBucketed`/`assertNotClustered` + `validatePartitioning()` 四重客户端校验致 `partitionBy`/`bucketBy`/`clusterBy` + jdbc 组合 silent contract violation，MEDIUM）与本项同属 "客户端 API silent contract violation" 模式——R47 是 SQL 字面量拼接顺序错误（asSerde=true 路径不可用），R50 是 sink format 与 partition/bucket/cluster 校验缺失（partitionBy + jdbc 组合静默丢失），两者均为客户端构造请求时未对参数组合做 fail-fast 校验导致用户路径功能性偏离；Round 40 consistency 反向引用补齐。Round 41 新增 R52（`Catalog.listColumns` / `tableExists` / `functionExists` 形参顺序与上游相反致 silent argument-swap，HIGH）同 `Catalog.scala` 公共 API 静默契约违反族系——R47 是 SQL 字面量层 loud ParseException、R52 是 typed signature 层 silent argument-swap，可一并审计 Catalog 模块全部 public API 与上游签名一致性。Round 43 新增 R59（`Catalog.getTable`/`Catalog.getFunction` 形参顺序与上游相反致 silent argument-swap，HIGH）扩展 R52 形参顺序闭环——R59 在 `Catalog.scala:141,151` 两处覆盖 `getTable(tableName, dbName)` / `getFunction(functionName, dbName)`，与 R52 三个站点（`:78,180,190`）合计 5 个 Catalog 形参顺序与上游一致性站点；R47（SQL 文法层）+ R52/R59（typed signature 层）三者共同形成 Catalog 模块公共 API 与上游契约偏离的批量修复闭环。Round 46 consistency 维度补齐反向引用：与 Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R47 是 SQL 文法层字面量拼接顺序错误（asSerde=true ParseException loud failure）、R74 是 streaming sink 配置 typed-overload 字符串大小写归一化（IllegalArgumentException loud failure），两者均在客户端构造请求时未对参数做 fail-fast 归一化致 server 端报错失败。

#### R50. `DataFrameWriter.jdbc` 跳过 `assertNotPartitioned` / `assertNotBucketed` / `assertNotClustered` + `validatePartitioning()` 校验（用户 `partitionBy` / `bucketBy` / `clusterBy` 状态被静默直通进 proto，但 server jdbc relation 不消费这些字段）
- **文件**: `src/main/scala/org/apache/spark/sql/DataFrameWriter.scala:134-141`
- **问题**: `jdbc(url, table, props)` 直接 `format("jdbc").option(...).save()`，没有 fail-fast 校验：
  ```scala
  def jdbc(url: String, table: String, connectionProperties: java.util.Properties): Unit =
    import scala.jdk.CollectionConverters.*
    val propsMap = connectionProperties.asScala.toMap
    format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .options(propsMap)
      .save()
  ```
  而 `buildWriteOp`（`:143-157`）会把 `partitionCols` / `numBuckets` / `bucketColNames` / `sortColNames` / `clusteringCols` 全部写入 `WriteOperation` proto。当用户写：
  ```scala
  df.write
    .partitionBy("a")
    .bucketBy(4, "b").sortBy("c")
    .clusterBy("d")
    .jdbc(url, table, props)
  ```
  partitionCols=["a"]、numBuckets=4、bucketColNames=["b"]、sortColNames=["c"]、clusteringCols=["d"] 全部静默写入 proto——server-side `SparkConnectPlanner` 解析 jdbc relation 时仅消费 `format` + `options`，partitioning/bucketing/clustering 字段被忽略，用户 SQL 写出后表里**没有任何分桶/分区/聚簇结构**。错误只能在 query plan 重读阶段（`spark.read.jdbc(...)` 后做 `df.queryExecution.executedPlan` 观察）才能发现，是典型的 silent contract violation。
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala:327-329`：
  ```scala
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    assertNotPartitioned("jdbc")
    assertNotBucketed("jdbc")
    assertNotClustered("jdbc")
    val props = new Properties()
    props.putAll(extraOptions.asJava)
    ...
  }
  ```
  与配套 helpers 在 `:462/472/478/487` 显式拒绝 partitionBy / bucketBy / clusterBy 与 jdbc 共用：用户调用 `df.write.partitionBy("a").jdbc(...)` 立即抛 `AnalysisException("PARTITIONED_BY_NOT_SUPPORTED_FOR_JDBC")`。同时上游 `validatePartitioning()` 在 `save` / `saveAsTable` / `insertInto` 路径校验 partitionBy 与 clusterBy 不能共用（`:166`/`:184`/`:215`）——SC3 也未实现这一校验，多个 sink format 路径下静默允许冲突组合。
  本 client `DataFrameWriter` 缺这两层防御：(a) jdbc 入口的三重 assert；(b) save / saveAsTable / insertInto 入口的 `validatePartitioning()` 互斥校验。后者影响面更广（所有 sink）但因 cluster + partition 同用是 corner case 暴露概率低；前者是 jdbc 这一常用 sink 的明确公共 API 契约缺口。
- **建议**: 仿上游补三重 assert + validatePartitioning helper：
  ```scala
  // DataFrameWriter.scala
  def jdbc(url: String, table: String, connectionProperties: java.util.Properties): Unit =
    assertNotPartitioned("jdbc")
    assertNotBucketed("jdbc")
    assertNotClustered("jdbc")
    import scala.jdk.CollectionConverters.*
    val propsMap = connectionProperties.asScala.toMap
    format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .options(propsMap)
      .save()

  private def assertNotPartitioned(operation: String): Unit =
    if partitionCols.nonEmpty then
      throw IllegalArgumentException(s"'$operation' does not support partitionBy.")

  private def assertNotBucketed(operation: String): Unit =
    if numBuckets.isDefined || bucketColNames.nonEmpty then
      throw IllegalArgumentException(s"'$operation' does not support bucketBy and sortBy.")

  private def assertNotClustered(operation: String): Unit =
    if clusteringCols.nonEmpty then
      throw IllegalArgumentException(s"'$operation' does not support clusterBy.")

  private def validatePartitioning(): Unit =
    if partitionCols.nonEmpty && clusteringCols.nonEmpty then
      throw IllegalArgumentException(
        "Cannot use both partitionBy and clusterBy. Use only one or the other."
      )
  ```
  并在 `save` / `saveAsTable` / `insertInto` 入口顶部调用 `validatePartitioning()`。同步在 `DataFrameWriterSuite` 加：(a) `df.write.partitionBy("a").jdbc(...)` 抛 IllegalArgumentException；(b) `df.write.bucketBy(4, "b").jdbc(...)` 抛；(c) `df.write.clusterBy("d").jdbc(...)` 抛；(d) `df.write.partitionBy("a").clusterBy("d").save(...)` 抛（互斥校验）。
- **预估**: 20-30 min（三 helper + 四个测试 + validatePartitioning 调用点 3 处）
- **价值**: MEDIUM。修复后 jdbc 与 partitioning/bucketing/clustering 不兼容组合在客户端 fail-fast 而非 silent 直通到 proto——用户可以立即看到错误而不是经历"写出表无预期结构"的诊断噩梦。同时补齐 partitionBy + clusterBy 互斥校验，与上游对齐。
- **关联**: 与 R29 / R41 / R47 / R48 同属 "客户端 API 路径静默语义偏离 / 与上游分歧" 大类——R29 是 observation registry 范围错误、R41 是 agg toMap 静默去重、R47 是 SHOW CREATE TABLE AS SERDE 拼接顺序错误（两个语义独立但同 silent contract violation 模式）、R48 是 union 默认 isAll=false 致 server 套 Distinct（CRITICAL）；本项 R50 与 R48 模式最接近：客户端构造 proto 时未在公共 API 入口验证语义合法性，server 接收无效组合后按缺省语义执行，用户行为偏离预期。Round 39 completeness 维度新增。Round 45 completeness 维度新增 R73，覆盖 `DataFrameWriter.insertInto` 路径硬编码 `setMode(SAVE_MODE_APPEND)` 强制覆写用户先前 `mode("overwrite")` / `mode("ignore")` 设置，与上游 Connect path `sql/connect/common/.../DataFrameWriter.scala:136-145` 不调 `setMode()` 语义直接偏离——同 R50 同 family（DataFrameWriter 公共 API 静默契约违反），但 R73 模式更严重：R50 是合法 partitioning/bucketing/clustering 状态被静默丢入 proto 但 server 端 jdbc relation 不消费（错误结果而非数据写入风险），R73 是用户显式 `mode("overwrite")` 后调 `insertInto(...)` 被静默替换为 APPEND——用户期望 truncate 写但实际 append 写造成生产数据腐蚀（HIGH 数据正确性）。Round 46 consistency 维度补齐反向引用：Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R50（jdbc 路径校验缺失，silent partition/bucket/cluster 直通） + R74（streaming sink 配置大小写归一化缺失，loud IllegalArgumentException）均为客户端构造请求时未对参数做 fail-fast 归一化但失败模式不同（silent 错误结果 vs. loud 启动失败）。

#### R51. `ArtifactManager.addArtifactsImpl` 用 inline `StreamObserver` 绕过 `ResponseValidator` + 不校验 `v.getSessionId`（server 端 session swap 静默接受，与上游 sessionId mismatch guard 偏离）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArtifactManager.scala:123-161`
- **问题**: `addArtifactsImpl` 用 inline `StreamObserver[AddArtifactsResponse]` 直接注册到 `asyncStub.addArtifacts(responseHandler)`：
  ```scala
  private def addArtifactsImpl(artifacts: Iterable[Artifact]): Unit =
    val promise = Promise[Seq[AddArtifactsResponse.ArtifactSummary]]()
    val responseHandler = new StreamObserver[AddArtifactsResponse]:
      private val summaries = mutable.Buffer.empty[AddArtifactsResponse.ArtifactSummary]
      override def onNext(v: AddArtifactsResponse): Unit =
        v.getArtifactsList.forEach(s => summaries += s)        // ← 不校验 v.getSessionId
      override def onError(throwable: Throwable): Unit =
        promise.failure(throwable)
      override def onCompleted(): Unit =
        promise.success(summaries.toSeq)
    val stream = asyncStub.addArtifacts(responseHandler)        // ← 不走 responseValidator.wrapIterator
  ```
  路径上有两处保护缺失：(a) `onNext(v)` 不读取 `v.getSessionId` 与本地 `sessionId` 字段比较；(b) async streaming 注册不经过 `ResponseValidator`，无法把 server 流响应纳入 session 校验链。
  对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/client/ArtifactManager.scala:310-330` 的同位置：
  ```scala
  override def onNext(v: AddArtifactsResponse): Unit = {
    if (SparkStringUtils.isNotEmpty(v.getSessionId) && v.getSessionId != sessionId) {
      // In older versions of the Spark cluster, the session ID is not set in the response.
      // Ignore this check to keep compatibility.
      throw new IllegalStateException(
        s"Session ID mismatch: $sessionId != ${v.getSessionId}")
    }
    v.getArtifactsList.forEach { summary =>
      summaries += summary
    }
  }
  ```
  上游显式做 `getSessionId` 不空且不等的双条件检查，捕到 mismatch 即 `IllegalStateException` 翻译为 `onError(...)` 触发 `promise.failure(...)`——caller 端能感知到 server 把请求路由到了别的 session。SC3 缺这层守护：用户在 artifact 上传期间发生 server 端 session swap（K8s pod 重启 + sticky session 失效 / load balancer 重路由 / driver 重启）时，新 session 上的响应会被当作原 session 的成功响应静默接受，`promise.success(summaries.toSeq)` 在错误的 session 上 fulfilled——后续 `executePlan` / `query` 在用户认为已上传 artifact 但实际上传到旧 session 的状态下进行，触发 server-side `MISSING_ARTIFACT` / class-not-found 异常时排错路径不直观。
- **建议**: 复制上游 mismatch 守护到 inline observer，保留 backward compat（空 sessionId 跳过校验）：
  ```scala
  override def onNext(v: AddArtifactsResponse): Unit =
    val respSession = v.getSessionId
    if respSession != null && respSession.nonEmpty && respSession != sessionId then
      throw IllegalStateException(
        s"Session ID mismatch: $sessionId != $respSession"
      )
    v.getArtifactsList.forEach(s => summaries += s)
  ```
  `IllegalStateException` 经 gRPC 内部翻译为 `onError(...)`，自动 `promise.failure(...)` 并向上层传播，无需额外结构变化。同步在 `ArtifactManagerSuite` 添加 mock：构造一个返回不同 `sessionId` 的 `AddArtifactsResponse`，断言 caller 端收到 `IllegalStateException` cause 的 ExecutionException。
- **预估**: 5-10 min（4 行守护 + 1 个测试）
- **价值**: MEDIUM。防御性 + 与上游对齐：当前路径对 session swap 完全无感知，artifact 上传的成功语义在多 driver / sticky-session 失效场景下不可靠；修复后 artifact 上传同 unary RPC 路径享受同等的 session-id mismatch fail-fast。
- **关联**: 与 R45 同属 "客户端 session/retry 防护带空洞" 族系但落点不同——R45 是 `ResponseValidator.wrapIterator` 流式 RPC 路径未捕获 `INVALID_HANDLE.SESSION_CHANGED` 致 `sessionValid` 标志不更新（同 ResponseValidator 内的对称缺失），本项 R51 是 `ArtifactManager.addArtifactsImpl` 完全不接 ResponseValidator（流式 RPC 注册路径绕过整个保护层）；与 R29 不同根因——R29 是单 RPC 内 `failPendingObservations` 跨查询广播过宽，R51 是单 RPC 路径上 sessionId mismatch guard 完全缺失，二者均影响 client 端正确性但触发点与修复路径不同。Round 40 completeness 维度新增。

---

#### R53. `DataFrameNaFunctions.replace` 单列重载缺 `col == "*"` 特殊处理 + 数值类型未做 `convertToDouble` 归一化（与上游公共 API 契约偏离，常见用户调用路径下抛 AnalysisException 或 server 端类型不匹配）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameNaFunctions.scala:98-113`
- **问题**: SC3 实现：
  ```scala
  // DataFrameNaFunctions.scala:98-113
  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    replace(Seq(col), replacement)

  def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame =
    val naReplaceBuilder = NAReplace.newBuilder().setInput(df.relation)
    cols.foreach(naReplaceBuilder.addCols)
    replacement.foreach { (oldVal, newVal) =>
      val replacementProto = NAReplace.Replacement.newBuilder()
        .setOldValue(LiteralValueProtoConverter.toLiteralProto(oldVal))
        .setNewValue(LiteralValueProtoConverter.toLiteralProto(newVal))
        .build()
      naReplaceBuilder.addReplacements(replacementProto)
    }
    DataFrame(df.spark, NAReplace.newBuilder()...)
  ```
  上游 `spark-mine-12/sql/connect/common/.../DataFrameNaFunctions.scala:111-114`：
  ```scala
  def replace[T](col: String, replacement: Map[T, T]): DataFrame = {
    val cols = if (col != "*") Some(Seq(col)) else None
    buildReplaceDataFrame(cols, buildReplacement(replacement))
  }
  // buildReplacement
  private def buildReplacement[T](replacement: Map[T, T]): Iterable[NAReplace.Replacement] = {
    replacement.map { case (oldValue, newValue) =>
      val (oldVal, newVal) = (oldValue, newValue) match {
        case (k: java.lang.Number, v: java.lang.Number) => (convertToDouble(k), convertToDouble(v))
        case _ => (oldValue, newValue)
      }
      ...
    }
  }
  ```
  两处偏离：
  - **`"*"` 通配语义缺失**：上游对 `col == "*"` 显式 `cols = None`（proto 字段不设置 cols 即"对所有列"），server 端 `NAReplaceCommand` 对 `cols` 未设场景全列扫描替换。SC3 直通 `cols = Seq("*")` 进 proto，server 端把 `"*"` 当作字面列名 lookup，schema 中无 `*` 列抛 AnalysisException——`df.na.replace("*", Map(0 -> 1))` 是上游文档化的常用调用，SC3 用户复制此调用直接失败。
  - **数值类型归一化缺失**：上游 `convertToDouble` 把 `Int` / `Long` / `Float` / `Short` / `Byte` 五种数值类型统一为 `Double`。SC3 直接用原始类型走 `LiteralValueProtoConverter.toLiteralProto(0)`（`Int`）→ proto `Literal.Integer`，server 端 NAReplace 对 schema 列 `LongType` 与 literal `IntegerType` 类型不匹配抛 server-side 错误。常见路径：`df.na.replace("longCol", Map(0 -> 1))` 用户写字面 `Int` literal，但表 schema 是 `LongType`，server 端类型严格匹配下抛错；上游归一化为 `Double` 后 server 用 `cast` 处理类型转换。
- **修复**:
  ```scala
  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    val cols = if col != "*" then Some(Seq(col)) else None
    buildReplaceDataFrame(cols, buildReplacement(replacement))

  private def buildReplacement[T](replacement: Map[T, T]): Iterable[NAReplace.Replacement] =
    replacement.map { (oldValue, newValue) =>
      val (oldVal, newVal) = (oldValue, newValue) match
        case (k: java.lang.Number, v: java.lang.Number) =>
          (convertToDouble(k), convertToDouble(v))
        case _ => (oldValue, newValue)
      NAReplace.Replacement.newBuilder()
        .setOldValue(LiteralValueProtoConverter.toLiteralProto(oldVal))
        .setNewValue(LiteralValueProtoConverter.toLiteralProto(newVal))
        .build()
    }

  private def convertToDouble(v: AnyRef): java.lang.Double = v match
    case d: java.lang.Double => d
    case _ => v.asInstanceOf[java.lang.Number].doubleValue.asInstanceOf[java.lang.Double]
  ```
  同步抽 `buildReplaceDataFrame(cols: Option[Seq[String]], reps: Iterable[NAReplace.Replacement])` helper 共享 `Seq[String]` 重载与 `"*"` 路径。`DataFrameNaFunctionsSuite` 加两条测试：(a) `df.na.replace("*", Map("old" -> "new"))` 正常返回（server-mock 验证 cols 为空）；(b) `df.na.replace("longCol", Map(0 -> 1))` 不抛类型不匹配。
- **预估**: 25-35 min（双重载 + buildReplacement helper + convertToDouble + 两条单测）
- **价值**: MEDIUM。两处偏离都直接影响公共 API 用户体验：`"*"` 通配是上游文档化用法、数值归一化是 silent type-mismatch 防御。两者修复点紧凑、可一并完成。
- **关联**: 与 R52 / R55 同属 "public API 与上游契约偏离" 大类，落点不同：R52 是 Catalog 形参顺序、R55 是 DataStreamReader 返回类型、R53 是 NaFunctions 行为语义；与 R56 `UDFRegistration.registerJava` 完全缺失同属 "public API 缺失/偏离" 族系——R52/R53/R55/R57 是已存在 API 的签名/语义偏离，R56 是 API 完全缺失子分支；R53 修复入口与 `Map[T, T]` dedup 历史"不修复"决策（曾在 Round 35 完整性轮被 reviewer 重报但因上游同样行为而拒绝）独立——本项是上游存在但 SC3 缺失的两个 feature，与 dedup 是 Scala stdlib 行为非 SC3-specific 的决策维度不同。

---

#### R54. `StreamingQueryManager.get(id)` 不做 `hasQuery` 检查致返回伪造 `StreamingQuery` 对象（不存在的 query id 路径下伪对象后续调用抛远程异常，错误信息非"该 id 不存在"语义）

- **位置**: `src/main/scala/org/apache/spark/sql/StreamingQueryManager.scala:27-31`
- **问题**: SC3 实现：
  ```scala
  // StreamingQueryManager.scala:27-31
  def get(id: String): StreamingQuery =
    val result = executeManagerCmd(_.setGetQuery(id))
    val qi = result.getQuery
    val name = if qi.hasName then Some(qi.getName) else None
    StreamingQuery(session, qi.getId.getId, qi.getId.getRunId, name)
  ```
  上游 `spark-mine-12/sql/connect/common/.../StreamingQueryManager.scala`：
  ```scala
  def get(id: UUID): StreamingQuery = get(id.toString)
  def get(id: String): StreamingQuery = {
    val response = executeManagerCmd(_.setGetQuery(id))
    if (response.hasQuery) {
      RemoteStreamingQuery.fromStreamingQueryInstanceResponse(sparkSession, response.getQuery)
    } else { null }
  }
  ```
  SC3 路径下 `result.getQuery` 在 server 返回未设 `query` 字段时（query id 不存在）返回 default proto `StreamingQueryInstance.getDefaultInstance`：`getId.getId == ""`、`getId.getRunId == ""`、`hasName == false`。SC3 构造 `StreamingQuery(session, "", "", None)` 伪对象返回给用户，用户后续调用 `q.id` 得到空字符串、调用 `q.runId` 得到空字符串、调用 `q.status` / `q.recentProgress` / `q.stop()` 走 manager cmd 把空字符串作为 id 发给 server，server 端 OPERATION_NOT_FOUND 抛远程异常——错误信息"操作未找到（id="")"非"该 id 不存在"语义，且抛出时机在用户后续操作而非 `get(id)` 本身。
- **修复**:
  ```scala
  def get(id: String): StreamingQuery =
    val result = executeManagerCmd(_.setGetQuery(id))
    if result.hasQuery then
      val qi = result.getQuery
      val name = if qi.hasName then Some(qi.getName) else None
      StreamingQuery(session, qi.getId.getId, qi.getId.getRunId, name)
    else
      null
  ```
  返回 `null` 与上游一致。可考虑改为返回 `Option[StreamingQuery]` 提供更安全的 typed API（但这是公共 API breaking change，与 R52/R55 类似需在 0.4.0 协调）。同步在 `StreamingQueryManagerSuite` 加 mock：返回未设 `query` 字段的 response，断言 `mgr.get("nonexistent-id") == null`。
- **预估**: 10-15 min（4 行 if-guard + 1 个单测；若同步引入 Option 重载额外 +10 min）
- **价值**: MEDIUM。当前路径下用户对不存在 id 调用 `get` 不会立即得到错误信号，幻影对象在后续调用链中触发的远程异常被绕了一层间接，难以诊断。修复是与上游对齐的最小补丁，复杂度低、收益直接。
- **关联**: 与 R39 `StreamingQueryListenerBus.registerServerSideListener` `hasNext=false` 落空 fallthrough 同属 "streaming 公共 API silent fallthrough" 族系——R39 在 listener 注册路径漏抛"未收到确认"异常致 silent 注册失败，R54 在 query lookup 路径漏检 `hasQuery` 致 silent 伪对象返回，二者修复模式同型（增加 `hasXxx` 检查 → 抛错或返回 null）；同 streaming 路径上层 listener / 下层 query 两个入口的同型缺陷可一并修复。

---

#### R56. `UDFRegistration` 缺 `registerJava(name, className, returnDataType)` 方法（Java/PySpark 互操作场景下用户无法注册 Java 类，与 Spark Connect Java 客户端 API surface 偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/UDFRegistration.scala`
- **问题**: SC3 实现仅有 Scala 入口：
  ```scala
  // UDFRegistration.scala
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction = ...
  inline def register[R, A1...](name: String, f: (A1, ...) => R): UserDefinedFunction = ...
  ```
  上游 `spark-mine-12/sql/connect/common/.../UDFRegistration.scala:47-55`：
  ```scala
  override def registerJava(name: String, className: String, returnDataType: DataType): Unit = {
    val builder = JavaUdfBuilder(className, ...)
    register(name, ...)
  }
  ```
  SC3 完全缺失 `registerJava` 方法。Java/PySpark 互操作场景下用户需通过类名注册预编译的 Java UDF（典型路径：用户用 Java 写 UDF 类、jar 包通过 `addArtifact` 上传到 server，再用 `registerJava("myUdf", "com.example.MyUdf", IntegerType)` 注册），SC3 用户必须自行用 reflection 加载类、转 Scala 函数再 `register`，破坏跨语言互操作的标准路径。
- **修复**: 补全 `registerJava`：
  ```scala
  override def registerJava(name: String, className: String, returnDataType: DataType): Unit =
    val javaUdfProto = proto.JavaUDF.newBuilder()
      .setClassName(className)
      .setOutputType(DataTypeProtoConverter.toConnectProtoType(returnDataType))
      .build()
    val cmd = proto.Command.newBuilder()
      .setRegisterFunction(proto.CommonInlineUserDefinedFunction.newBuilder()
        .setFunctionName(name)
        .setJavaUdf(javaUdfProto)
        .build())
      .build()
    session.execute(cmd)
  ```
  同步加 `registerJavaUDAF(name, className)` 入口（上游同文件提供）以闭合 Java UDF/UDAF 互操作。`UDFRegistrationSuite` 添加测试（标 `@IntegrationTest`，依赖 live server + 预编译 jar）。
- **预估**: 30-45 min（registerJava + 可选 registerJavaUDAF + proto 路径 + 集成测试 jar 准备）
- **价值**: MEDIUM。public API 缺失而非缺陷——已有用户基础不会受影响，但 Java/PySpark 互操作场景下 SC3 是阻塞项。0.4.0 加入可补齐 Spark Connect Java 客户端 API surface 的标准入口。
- **关联**: 与 R52 / R53 / R55 / R57 同属 "与上游 public API 对齐" 族系，落点不同：R52 是 Catalog 形参顺序错放、R53 是 NaFunctions 行为偏离（"*" 通配 + 数值归一化）、R55 是 DataStreamReader 返回类型偏离、R57 是 StreamingQuery 状态返回 typed 偏离——R52/R53/R55/R57 是已存在 API 的签名/语义偏离（修），R56 是 API 完全缺失（新加）；R56 在族系中是 API 缺失子分支，与 Spark Connect 协议的 `JavaUDF` proto 字段已在 spark-upstream 子模块暴露相关，本仓客户端未消费，添加成本主要在 plumbing。Round 43 新增 R58（`RuntimeConfig.get` 不抛 `NoSuchElementException` 与上游 `@throws` 契约偏离，HIGH）/ R59（Catalog `getTable`/`getFunction` 形参顺序错放与上游相反致 silent argument-swap，HIGH）扩展该 family——R58 在 SparkSession config 路径、R59 在 Catalog 元数据路径（与 R52 形成 Catalog 形参顺序闭环），R56 在 UDFRegistration 入口、R58 在 RuntimeConfig 入口、R59 在 Catalog 元数据查询入口，三者覆盖 SC3 公共 API 与上游契约偏离/缺失的不同子系，0.4.0 集中修复可形成完整批量 release。Round 44 新增 R69（`options(java.util.Map[String, String])` Java-interop 重载在 5 个 reader/writer 入口完全缺失，MEDIUM）/ R71（`DataFrame.join(right, usingColumn: String, joinType: String)` 三参单列重载完全缺失，MEDIUM）扩展该 family 的 "public API 完全缺失" 子族——R56 在 UDF 注册路径（Java-interop 公共 API 缺失典型）、R69 在 reader/writer options 入口（Java-interop 5 site 缺失）、R71 在 DataFrame join 入口（5/9 join overload 已存在但缺单列 + joinType 重载），三者落点正交可独立修复但同属 "Java-interop / 上游 overload 完全缺失" 族系，0.4.0 与 R61 `StorageLevel` 三常量缺失共同闭合 SC3 与上游 public API 完全缺失子系（API 缺失 + 常量缺失 + Java-interop 重载缺失三大缺失子分支）。Round 48 consistency 维度补齐反向引用：Round 47 新增 R78 `DataFrameStatFunctions.freqItems` 内联构造 proto 不调 `setSupport` 默认 0 + 缺 `Array[String]` Java-friendly 重载 / R79 `DataFrameStatFunctions.sampleBy[T]` 缺 `ju.Map[T, jl.Double]` Java-friendly 重载，与本项 `UDFRegistration.registerJava` 缺失共同闭合 stat/na/UDF 子系统 Java-friendly 入口（R56/R78/R79 三 site 批量补齐 stat/na/UDF Java-interop 公共 API 缺失子族系）。Round 49 关联：新增 R80 `DataFrameNaFunctions.fill[T]` Java `ju.Map[String, T]` Java-friendly 重载完全缺失——与本项 `UDFRegistration.registerJava` / R69 reader/writer `options(ju.Map)` 5 site / R71 `DataFrame.join` 三参单列 / R78 `freqItems` `Array[String]` / R79 `sampleBy` `ju.Map[T, jl.Double]` 共同形成 SC3 → Spark Connect Java 客户端 6-site Java-interop 公共 API 完全缺失子族系（R56/R69/R71/R78/R79/R80 跨 UDF/reader/writer/join/stat/na 6 大公共 API 入口批量补齐——0.4.0 集中修复可一次性闭合 Java-from-Scala-3 互操作零阻塞）。

---

#### R57. `StreamingQuery.recentProgress` / `lastProgress` / `exception` 返回 raw `String` / `Option[String]` 而非 typed `Array[StreamingQueryProgress]` / `StreamingQueryProgress` / `Option[StreamingQueryException]`（public typed API 契约偏离，用户从上游迁移代码编译失败）

- **位置**: `src/main/scala/org/apache/spark/sql/StreamingQuery.scala:50,56,79`
- **问题**: SC3 实现：
  ```scala
  // StreamingQuery.scala
  // line 50
  def recentProgress: Seq[String] = ...
  // line 56
  def lastProgress: Option[String] = ...
  // line 79
  def exception: Option[String] = ...
  ```
  上游 `spark-mine-12/sql/api/.../streaming/StreamingQuery.scala:79,95,102`：
  ```scala
  def exception: Option[StreamingQueryException]
  def recentProgress: Array[StreamingQueryProgress]
  def lastProgress: StreamingQueryProgress
  ```
  三处典型用法均失败：
  - `q.recentProgress.head.numInputRows`：上游 typed `StreamingQueryProgress.numInputRows: Long`，SC3 raw `String` 无 `numInputRows` 方法，编译失败。
  - `q.lastProgress.timestamp`：同上，String 无 `timestamp` 字段。
  - `q.exception.foreach(e => log.error("query failed", e))`：上游 typed `StreamingQueryException extends Throwable`，SC3 `Option[String]` 无法直接作为 throwable。
  用户迁移路径下需自行 JSON 解析 SC3 raw String（server 实际发送的是 JSON 序列化的 progress 对象），代码量翻倍且失去 typed 保证。
- **修复**: 补全两个 typed 类（已在上游 `sql/api` 子模块暴露）：
  ```scala
  // StreamingQueryProgress.scala（vendor from spark-upstream/sql/api/...）
  class StreamingQueryProgress(json: String) {
    private lazy val parsed = parseJson(json)
    def id: UUID = parsed.id
    def runId: UUID = parsed.runId
    def name: String = parsed.name
    def timestamp: String = parsed.timestamp
    def numInputRows: Long = parsed.numInputRows
    ...
  }
  // StreamingQueryException.scala
  class StreamingQueryException(...) extends Throwable {
    def errorClass: String = ...
    def cause: Throwable = ...
  }
  ```
  然后 `StreamingQuery` 三个方法转 typed：
  ```scala
  def recentProgress: Array[StreamingQueryProgress] =
    rawProgressJsons.map(StreamingQueryProgress(_)).toArray
  def lastProgress: StreamingQueryProgress =
    rawLastProgressJson.map(StreamingQueryProgress(_)).orNull
  def exception: Option[StreamingQueryException] =
    rawExceptionString.map(StreamingQueryException(_))
  ```
  注意 `lastProgress` 上游返回非 Option 直接 `StreamingQueryProgress`（无 progress 时为 null），与 SC3 的 `Option` 对齐策略需协调；建议保留 `Option` 包装以契合 Scala 风格（这是 SC3 与 upstream 的小幅偏离但更安全）。
- **预估**: 60-90 min（两个 typed 类 vendor + JSON 解析层 + 三个方法签名转换 + 测试断言更新；若 `StreamingQueryException` 走 ErrorInfo proto 解析额外 +30 min）
- **价值**: MEDIUM。public API 长期与上游分歧维护成本高，typed 返回是 Spark Streaming 用户的核心契约——`numInputRows` / `processedRowsPerSecond` 等是 streaming dashboard 的标准字段。修复后 SC3 用户从上游迁移代码无需改写 progress 处理逻辑。
- **关联**: 与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 同属 "streaming 路径 typed contract 偏离" 族系——R55 是 reader 入口、R57 是 query 状态查询；与 R22 `SparkSession.close` 不清理 `StreamingQueryListenerBus` + R39 listener 注册 silent fallthrough 同 streaming 子系统但不同维度（R22 是 lifecycle、R39 是注册路径、R57 是返回类型）。R55 + R57 可作为 streaming 公共 API typed 对齐的批量修复入口。与 R56 `UDFRegistration.registerJava` 完全缺失同属 "public API 缺失/偏离" 大族系（R52/R53/R55/R57 是已存在 API 签名/语义偏离子分支，R56 是 API 完全缺失子分支），R57 在该 family 中位于 streaming + typed contract 交集点。Round 43 新增 R58（`RuntimeConfig.get` 不抛 `NoSuchElementException`，HIGH）/ R59（Catalog `getTable`/`getFunction` 形参顺序错放，HIGH）扩展该 family——R58/R59 与 R57 共同覆盖 SC3 typed 契约不同子系（R57 streaming query 状态 typed 返回 / R58 SparkSession config typed throws / R59 Catalog 形参顺序），三者修复模式不同但同属 "用户从上游迁移 typed 代码会立即/静默失败" 大类，可在 0.4.0 公共 API 与上游对齐 release 中一并处理。Round 44 新增 R65（`StreamingQuery.status` 返回 raw proto `StreamingQueryCommandResult.StatusResult`，HIGH）/ R67（`Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` 而非 `MergeIntoWriter[T]`，HIGH）扩展该 typed contract 族系——R65 在 StreamingQuery.status 入口（与 R57 `recentProgress`/`lastProgress`/`exception` 同 StreamingQuery 公共 API 子模块共 4 个 typed 返回偏离 site 形成 StreamingQuery typed contract 完整闭环）、R67 在 typed-Dataset MERGE DSL 链上 `T` 在第一步即丢失（与 R57 同型 typed contract 偏离但落点在 typed-Dataset transformations 而非 streaming），R57 + R65 + R67 形成 typed contract 大族系跨子系覆盖（StreamingQuery 状态查询 + StreamingQuery 命令查询 + Dataset MERGE DSL）。Round 46 consistency 维度补齐反向引用：Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 streaming 子系统公共 API 偏离族系——R57 是 query 状态查询返回类型 typed-contract 退化（JSON 字符串泄漏）、R74 是 writer 配置大小写归一化偏离（typed `OutputMode` 上送 PascalCase loud 启动失败），两者从 streaming query 状态查询 / writer 配置双向覆盖公共 API 与上游契约偏离 site。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R61. `StorageLevel` 缺 `DISK_ONLY_3` / `MEMORY_ONLY_SER_2` / `MEMORY_AND_DISK_SER_2` 三个常量（与上游 `common/utils/.../storage/StorageLevel.scala:152,156,160` 偏离，常见 replication=3 / 序列化 + 副本组合不可用）

- **位置**: `src/main/scala/org/apache/spark/sql/StorageLevel.scala:24-34`
- **问题**: SC3 实现的 `StorageLevel` 伴生对象仅暴露 10 个常量：
  ```scala
  // StorageLevel.scala:24-34
  object StorageLevel:
    val NONE = StorageLevel(false, false, false, false, 1)
    val DISK_ONLY = StorageLevel(true, false, false, false, 1)
    val DISK_ONLY_2 = StorageLevel(true, false, false, false, 2)
    val MEMORY_ONLY = StorageLevel(false, true, false, true, 1)
    val MEMORY_ONLY_2 = StorageLevel(false, true, false, true, 2)
    val MEMORY_ONLY_SER = StorageLevel(false, true, false, false, 1)
    val MEMORY_AND_DISK = StorageLevel(true, true, false, true, 1)
    val MEMORY_AND_DISK_2 = StorageLevel(true, true, false, true, 2)
    val MEMORY_AND_DISK_SER = StorageLevel(true, true, false, false, 1)
    val OFF_HEAP = StorageLevel(true, true, true, false, 1)
  ```
  上游 `spark-mine-12/common/utils/.../storage/StorageLevel.scala:149-161` 13 个常量，SC3 缺 3 个：
  ```scala
  val DISK_ONLY_3 = new StorageLevel(true, false, false, false, 3)         // 上游 :152
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)   // 上游 :156
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2) // 上游 :160
  ```
  影响路径：用户 `df.persist(StorageLevel.DISK_ONLY_3)`（典型 HDFS 副本对齐场景，3 副本与 HDFS 默认副本数一致）/ `df.persist(StorageLevel.MEMORY_ONLY_SER_2)`（序列化 + 2 副本，常用于内存压力大但要求高可用的中间结果）/ `df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)`（最常用的 production storage level 之一，序列化 + spill + 2 副本）在 SC3 编译失败——用户必须显式 `StorageLevel(true, false, false, false, 3)` 等手工构造，对照上游样例代码无法直接迁移。
- **修复**: 补全三个常量：
  ```scala
  object StorageLevel:
    val NONE = StorageLevel(false, false, false, false, 1)
    val DISK_ONLY = StorageLevel(true, false, false, false, 1)
    val DISK_ONLY_2 = StorageLevel(true, false, false, false, 2)
    val DISK_ONLY_3 = StorageLevel(true, false, false, false, 3)              // NEW
    val MEMORY_ONLY = StorageLevel(false, true, false, true, 1)
    val MEMORY_ONLY_2 = StorageLevel(false, true, false, true, 2)
    val MEMORY_ONLY_SER = StorageLevel(false, true, false, false, 1)
    val MEMORY_ONLY_SER_2 = StorageLevel(false, true, false, false, 2)        // NEW
    val MEMORY_AND_DISK = StorageLevel(true, true, false, true, 1)
    val MEMORY_AND_DISK_2 = StorageLevel(true, true, false, true, 2)
    val MEMORY_AND_DISK_SER = StorageLevel(true, true, false, false, 1)
    val MEMORY_AND_DISK_SER_2 = StorageLevel(true, true, false, false, 2)    // NEW
    val OFF_HEAP = StorageLevel(true, true, true, false, 1)
  ```
  同步在 `StorageLevelSuite`（若不存在则新建）添加 13 个常量的 `toProto` round-trip 断言，确保 proto 字段映射在副本数 / serialized / off-heap 各组合下与上游 `StorageLevel.fromString` 等价。
- **预估**: 5-10 min（三个 val 添加 + 单测覆盖；无 plumbing 改动）
- **价值**: MEDIUM。public API 缺失而非缺陷——已发布版本不会因添加常量受影响，但 production 用户从上游迁移 `df.persist(StorageLevel.MEMORY_AND_DISK_SER_2)` 类常见调用必须改写。修复成本极低（3 行代码 + 3 个测试断言），ROI 极高。
- **关联**: 与 R56 `UDFRegistration.registerJava` 完全缺失同属 "public API 缺失" 子分支族系——R56 在 UDF 注册路径、R61 在 storage level 常量路径，落点不同但模式同型（已存在公共 API surface 缺少必要入口/常量）；与 R52 / R53 / R55 / R57 / R58 / R59 / R60 "public API 与上游契约偏离" 大 family 形成互补——R52/R55/R57/R59/R60 修签名 / 返回类型 / 形参顺序、R56/R61 补缺失入口 / 常量。Round 43 completeness 维度新增。

---

#### R62. `Observation.get` 用 `Duration(10, "minutes")` 硬超时与上游 `Duration.Inf` 偏离（长时运行 batch / 慢 stage 路径下用户 `obs.get` 在 10 分钟时抛 `TimeoutException` 而非阻塞至 future 完成）

- **位置**: `src/main/scala/org/apache/spark/sql/Observation.scala:43,85`
- **问题**: SC3 `Observation.get` 用 10 分钟硬超时：
  ```scala
  // Observation.scala:43
  try Await.result(future, Observation.ObservationTimeout)
  catch
    case _: TimeoutException =>
      throw TimeoutException(
        s"Observation '$name' timed out after ${Observation.ObservationTimeout} waiting for metrics"
      )
  // :85
  private val ObservationTimeout: Duration = Duration(10, "minutes")
  ```
  上游 `spark-mine-12/sql/api/.../Observation.scala:155`：
  ```scala
  SparkThreadUtils.awaitResult(future, Duration.Inf)
  ```
  上游显式选择 `Duration.Inf` 阻塞至 future 完成（成功 / 失败 / 取消三种终态都解阻塞），不强加 10 分钟硬限制。SC3 偏离影响：
  - **长时 batch action**：用户在大数据集上 `df.collect()` 触发 observed metrics 计算，server 端 stage 跑 30 分钟（典型 ETL 场景），`obs.get` 在 10 分钟时本地抛 `TimeoutException`——但 server 端 metrics 计算还在继续，用户拿到失败异常但 `future` 实际未完成，后续 `obs.get` 调用每次都会再抛同一个 `TimeoutException`（promise 未 complete，每次 await 都重新计时）。
  - **stage retry / speculative execution**：server 端 stage 触发 retry / speculative execution 拖长 wall time，10 分钟硬限制下用户被错误剥夺等待权。
  - **测试 / 调试**：开发者 `Thread.sleep(11.minutes); obs.get`（手工等 server 慢路径）必抛超时，与上游行为不一致。
  本项与 R29 `failPendingObservations` 跨查询观测污染同属 observation 路径 wall-time / future 终态契约缺陷族系——R29 是错误广播完成不相关 future，R62 是默认超时过严提前剥夺等待权。
- **修复**: 改为 `Duration.Inf`，移除超时语义；如保留可观测性诉求（防止程序 bug 死锁），加显式 overload `get(timeout: Duration)`：
  ```scala
  // Observation.scala
  @throws[InterruptedException]
  def get: Map[String, Any] = await(Duration.Inf)

  @throws[InterruptedException]
  @throws[TimeoutException]
  def get(timeout: Duration): Map[String, Any] = await(timeout)

  private def await(timeout: Duration): Map[String, Any] =
    val row = Await.result(future, timeout)
    ...
  ```
  注意 `@throws[TimeoutException]` 注解从无参 `get` 移除（无超时不抛）、添加到带超时重载（typed contract 与 caller 期望一致）。同步 `ObservationSuite` 加用例：(a) `Await.ready(future, 11.minutes)` 后 `obs.get` 不抛超时（`get` 阻塞至 future 完成）；(b) `obs.get(1.second)` 在未完成路径下抛 `TimeoutException`。
- **预估**: 10-15 min（默认值改 + 重载 + 注解调整 + 两个测试）
- **价值**: MEDIUM。`Observation` 是 Spark dataset 指标观测的核心 API，长时 batch + stage retry 是 production 场景下的典型路径。10 分钟硬超时在错误路径下产出"obs 失败但 future 未完成"的不一致状态，是用户难以诊断的失败模式；与上游对齐改 `Duration.Inf` 是 zero-cost 修复。
- **关联**: 与 R29 `failPendingObservations` 跨查询观测污染同 `Observation` 路径 wall-time / future 终态契约缺陷族系——R29 是 catch 窗口错误广播失败完成不相关 future（语义错误广播），R62 是默认超时过严提前剥夺等待权（语义提前剥夺），两者落点不同但同属 observation 公共 API 的 future 完成语义偏离族系。Round 43 completeness 维度新增。

---

#### R65. `StreamingQuery.status` 返回 raw proto `StreamingQueryCommandResult.StatusResult`（public API 直接泄漏 proto 类型给用户代码，与上游 typed `StreamingQueryStatus` 偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/StreamingQuery.scala:63-65`
- **问题**: SC3 实现：
  ```scala
  // StreamingQuery.scala:63
  def status: StreamingQueryCommandResult.StatusResult =
    val result = executeQueryCmd(_.setStatus(true))
    result.getStatus
  ```
  返回类型 `StreamingQueryCommandResult.StatusResult` 是 `org.apache.spark.connect.proto` 自动生成的 Java proto 类——public API 直接把 proto wire format 泄漏给用户代码。用户调用 `q.status.getMessage` / `q.status.getIsTriggerActive` 等必须 import `org.apache.spark.connect.proto.*`，破坏客户端代码与协议绑定的隔离。
  对照上游 `spark-mine-12/sql/api/.../streaming/StreamingQuery.scala:86`：
  ```scala
  def status: StreamingQueryStatus
  ```
  上游 `StreamingQueryStatus` 是 typed Scala class（`spark-mine-12/sql/api/.../streaming/StreamingQueryStatus.scala`），含 `message: String` / `isDataAvailable: Boolean` / `isTriggerActive: Boolean` typed 字段，与 proto 解耦。SC3 用户：
  - **跨版本兼容性破坏**：proto 字段名 / 顺序在协议升级时可能变化，typed Scala class 提供稳定 API surface。
  - **import 污染**：用户业务代码必须显式 `import org.apache.spark.connect.proto.*`，client 应内化 proto 而非透传。
  - **migration 阻塞**：从上游迁移 `q.status.message` 在 SC3 编译失败（proto getter 是 `getMessage()`），用户必须改写所有访问点。
- **修复**: vendor 上游 `StreamingQueryStatus` 类，改 `status` 返回 typed：
  ```scala
  // src/main/scala/org/apache/spark/sql/streaming/StreamingQueryStatus.scala
  class StreamingQueryStatus(
      val message: String,
      val isDataAvailable: Boolean,
      val isTriggerActive: Boolean)

  // StreamingQuery.scala
  def status: StreamingQueryStatus =
    val result = executeQueryCmd(_.setStatus(true))
    val s = result.getStatus
    StreamingQueryStatus(s.getStatusMessage, s.getIsDataAvailable, s.getIsTriggerActive)
  ```
  注意 proto 字段名 `getStatusMessage` 与上游 typed 字段 `message` 偏离——这是协议层与 API 层正常的命名重映射。同步审计 `isActive` 的实现（`StreamingQuery.scala:20-22` 也 `setStatus(true)`）确保两者使用同一 proto 调用 path 不重复 RPC。`StreamingQuerySuite` 加 typed 断言：`q.status.isInstanceOf[StreamingQueryStatus]` + 字段值校验。
- **预估**: 30-45 min（typed class vendor + 实现转换 + 单测断言更新；source-incompatible breaking change 需 0.4.0 release notes）
- **价值**: MEDIUM。public API 长期泄漏 proto 类型维护成本高，typed `StreamingQueryStatus` 是上游 4.x 稳定契约。修复后用户无需 `import proto.*`、跨版本协议升级时 client 内化变更不影响用户代码。
- **关联**: 与 R57 `StreamingQuery.recentProgress` / `lastProgress` / `exception` 返回 raw `String` / `Option[String]`（同 `StreamingQuery.scala` 文件同 streaming 状态查询路径 typed 返回偏离）同直接族系兄弟——R57 是 progress / exception 返回类型偏离、R65 是 status 返回类型偏离，两者构成 `StreamingQuery` 全部状态查询入口（`isActive` / `recentProgress` / `lastProgress` / `status` / `exception`）typed 对齐的批量修复闭环；与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 同属 "streaming 路径 typed contract 偏离" 大族系。0.4.0 一并修复 R55 + R57 + R65 可形成 streaming 公共 API typed 对齐的完整批量 release。Round 43 completeness 维度新增。Round 46 consistency 维度补齐反向引用：Round 45 新增 R74 `DataStreamWriter.outputMode(streaming.OutputMode)` typed 重载缺 `.toLowerCase(Locale.ROOT)` 致 server 端启动失败同属 streaming 子系统公共 API 偏离族系——R65 是 StreamingQuery 命令查询返回 raw proto（typed-contract 退化静默用户须自解 proto）、R74 是 writer 配置大小写归一化偏离（loud 启动失败），两者覆盖 streaming query 命令查询 / writer 配置 双个 typed-contract / wire-protocol 对齐子系。Round 48 consistency 维度补齐反向引用：Round 47 新增 R77 `SparkSession.range(...)` 返回 `DataFrame` 而非 typed `Dataset[java.lang.Long]` 且缺 `range(start, end)` 双参重载，与本项同 typed-return 契约偏离族系，落点在 SparkSession 数据生成入口。

---

#### R69. `options(java.util.Map[String, String])` Java-interop 重载在 5 个 reader/writer 入口完全缺失（与上游 sql/api 提供的 Java overload 偏离致 Java/PySpark 互操作场景下用户必须 `import scala.jdk.CollectionConverters.*` + `.asScala.toMap` 手动桥接）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameReader.scala:34`、`DataFrameWriter.scala:49`、`DataStreamReader.scala:31`、`DataStreamWriter.scala:66`、`DataFrameWriterV2.scala:35`
- **问题**: SC3 5 个 reader/writer 类的 `options` 重载仅提供 Scala `Map[String, String]` 入口：
  ```scala
  // DataFrameReader.scala:34（其余 4 个文件同型）
  def options(m: Map[String, String]): DataFrameReader =
    m.foreach((k, v) => option(k, v)); this
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala:129,142`：
  ```scala
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.extraOptions ++= options
    this
  }
  def options(options: java.util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }
  ```
  其中 Java overload 在 `DataFrameReader` / `DataFrameWriter` / `DataStreamReader` / `DataStreamWriter` / `DataFrameWriterV2` 5 个站点对称提供。Java/PySpark 互操作场景下用户从上游迁移：
  ```java
  // Java caller
  Map<String, String> opts = Map.of("path", "/data", "header", "true");
  spark.read().options(opts).csv("/data");  // 上游 OK
  ```
  在 SC3 编译失败（`java.util.Map` 与 Scala `Map[String, String]` 类型不匹配），用户必须显式 `import scala.jdk.CollectionConverters.*` + `opts.asScala.toMap` 手动桥接——破坏 Java client 直接消费 SC3 sql API 的零摩擦预期。
- **修复**: 5 个文件统一加 Java overload：
  ```scala
  // DataFrameReader.scala（其余 4 文件同型）
  def options(m: java.util.Map[String, String]): DataFrameReader =
    import scala.jdk.CollectionConverters.*
    options(m.asScala.toMap)
  ```
  对应 `DataFrameWriter.scala` / `DataStreamReader.scala` / `DataStreamWriter.scala` / `DataFrameWriterV2.scala` 同步加 Java overload。同步在各对应 IntegrationSuite 加 `java.util.Map` 入口断言。
- **预估**: 15-20 min（5 文件 × 1 overload + 5 个测试断言；纯 additive 增加，无 breaking change）
- **价值**: MEDIUM。Java/PySpark 互操作是 Spark Connect 主要使用场景之一，public API 缺 Java overload 致 Java caller 必须改写客户端代码；纯 additive 修复，无回归风险。
- **关联**: 与 R56 `UDFRegistration.registerJava` 完全缺失同属 "Java/PySpark 互操作 public API 缺失" 族系——R56 是 Java UDF 注册路径完全缺失，R69 是 5 个 reader/writer 配置入口的 Java overload 缺失；两者覆盖 Java client 直接消费 SC3 sql API 的不同子系（UDF 注册 vs. 数据源选项）。与 R52/R55/R57/R59/R60/R67/R68 同 "public API 与上游契约偏离/缺失" 大 family——R52/R59 是参数顺序静默反转、R55/R67/R68 是返回类型 typed 退化、R57/R60/R65 是 typed contract 偏离、R56/R69 是 API 完全缺失子分支；R69 + R56 闭合 SC3 Java/PySpark 互操作 typed-API 缺失子族系。Round 44 completeness 维度新增。Round 49 关联：R80 `DataFrameNaFunctions.fill[T]` Java `ju.Map[String, T]` Java-friendly 重载完全缺失——与本项 reader/writer `options(ju.Map)` 5 site 缺失同 "Java-interop `ju.Map` 入口缺失" 子分支（R69 是 reader/writer.options、R80 是 na.fill），R56/R69/R71/R78/R79/R80 6-site Java-interop 公共 API 完全缺失批量补齐子族系（reader/writer.options + UDFRegistration.registerJava + DataFrame.join + na.fill + freqItems + sampleBy）。R81 `DataFrameWriterV2.replace()` / `createOrReplace()` 无参方法完全缺失——与本项 `DataFrameWriterV2.options(ju.Map)` 同 `DataFrameWriterV2.scala` 文件 V2 writer 公共 API 缺失子族（R69 是 V2 writer Java overload、R81 是 V2 writer 无参 sink），同文件 batch additive 0.4.0 修复。

---

#### R70. `GroupedDataFrame.mean`/`avg`/`max`/`min`/`sum` 在空 `colNames*` 入口下静默返回原始 df 而非自动选择全部数值列（与上游 `RelationalGroupedDataset.aggregateNumericColumns` 通过 `selectNumericColumns(colNames)` 在 colNames 为空时 fallback 到 schema 内全部数值列偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/GroupedDataFrame.scala:77-93,131-139`
- **问题**: SC3 `GroupedDataFrame` 5 个聚合便捷方法在空 varargs 入口下退化为 no-op：
  ```scala
  // GroupedDataFrame.scala:77-93
  def mean(colNames: String*): DataFrame =
    val cols = resolveColNames(colNames, "avg")
    if cols.isEmpty then df else agg(cols.head, cols.tail*)
  // avg/max/min/sum 同型
  
  // resolveColNames:131-139 仅做名字 → Column 直白映射，无自动选数值列逻辑
  private def resolveColNames(colNames: Seq[String], funcName: String): Seq[Column] =
    colNames.map { name => ... Column(name) ... .as(s"$funcName($name)") }
  ```
  对照上游 Connect 客户端路径 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/RelationalGroupedDataset.scala:77-84`：
  ```scala
  // 上游 Connect path：selectNumericColumns 直接转发 colNames 到 df.col
  private def selectNumericColumns(colNames: Seq[String]): Seq[Column] = {
    // This behaves different than the classic implementation.
    colNames.map(df.col)
  }
  private def aggregateNumericColumns(colNames: Seq[String], function: Column => Column): DataFrame = {
    toDF(selectNumericColumns(colNames).map(function))
  }
  def mean(colNames: String*): DataFrame = aggregateNumericColumns(colNames, functions.avg)
  ```
  注意 Connect 客户端 path 与 classic / API path 语义有意分流：上游 Connect path 在 `selectNumericColumns(colNames)` 仅做 `colNames.map(df.col)` 直接映射（不做空 fallback、不做 numeric 类型校验），auto-numeric fallback 语义保留在 classic path `spark-mine-12/sql/core/src/main/scala/org/apache/spark/sql/classic/RelationalGroupedDataset.scala:111-126` 与 server 侧。即使在上游 Connect 客户端 `df.groupBy("dept").mean()` 也只生成 `aggregateNumericColumns(Seq.empty, avg) = toDF(Seq.empty.map(avg))`，但上游 `toDF(Seq.empty)` 生成的 proto 经 server-side resolution 时被 server 解析为 "all numeric columns"——SC3 端 `if cols.isEmpty then df` 直接客户端 short-circuit，proto 根本不发送，server 无机会回填 fallback。失败模式：用户从上游迁移 `df.groupBy("dept").mean()`（无参 → "对所有数值列求平均"）在 SC3 上静默返回原始 grouped df，无任何错误信号——失败模式不可观测；同时 `df.groupBy("dept").max("non_numeric_str")` 在上游会校验失败抛错，SC3 直接构造 `max(string_col)` proto 让 server 端报错——错误信息层位错位（client-side 应 fail-fast）。
- **修复**: 重写 `resolveColNames` + 5 个聚合便捷方法支持空 colNames 自动选数值列：
  ```scala
  private def selectNumericColumns(colNames: Seq[String]): Seq[Column] =
    if colNames.nonEmpty then
      // user-specified: validate types via schema lookup if available
      colNames.map(Column(_))
    else
      // empty: select all numeric columns from df schema
      df.schema.fields.collect {
        case f if f.dataType.isInstanceOf[NumericType] => Column(f.name)
      }.toSeq
  
  def mean(colNames: String*): DataFrame =
    val cols = selectNumericColumns(colNames).map(c => functions.avg(c).as(s"avg(${c.toString})"))
    if cols.isEmpty then df else agg(cols.head, cols.tail*)
  // avg/max/min/sum 同型重写
  ```
  同步在 `GroupedDataFrameIntegrationSuite` 加无参 `mean()` / `max()` 等断言，验证 schema 内全部数值列被聚合（非数值列正确跳过）。
- **预估**: 20-30 min（1 helper 方法重写 + 5 入口逻辑修订 + 测试断言）
- **价值**: MEDIUM。`df.groupBy(...).mean()`（无参）是 EDA / quick-look 高频惯用法，SC3 静默 no-op 致用户结果与预期完全偏离且无错误信号。
- **关联**: 与 R41 `GroupedDataFrame.agg(aggExpr: (String, String), aggExprs: (String, String)*)` 重复 column 静默 `.toMap` 合并（同 `GroupedDataFrame.scala` 文件同 GroupedDataFrame 公共 API 静默语义偏离族系——R41 是重复列静默丢失、R70 是空入口静默 no-op，两者落点正交但同 GroupedDataFrame public API 行为分歧）直接族系兄弟。与 R29 `SparkSession.failPendingObservations` 跨查询观测污染 / R47 `Catalog.getCreateTableString(asSerde=true)` 文法位置错放 / R48 `DataFrame.union` 默认 `isAll=false` / R50 `DataFrameWriter.jdbc` 缺 partitionBy 校验 同属 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R29 在 SparkSession observation、R41/R70 在 GroupedDataFrame、R47 在 Catalog SQL 字符串、R48 在 DataFrame setOp、R50 在 DataFrameWriter sink format，多个 SC3 公共 API 入口均存在客户端层静默语义偏离。Round 44 completeness 维度新增。

---

#### R71. `DataFrame.join(right, usingColumn: String, joinType: String)` 三参单列重载完全缺失（5/9 join overload 已存在但缺单列 + joinType 重载致用户从上游迁移 `df.join(other, "id", "left_outer")` 编译失败必须改写为 `Seq("id")`）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrame.scala:110-149`
- **问题**: SC3 `DataFrame` 提供 5 个 join overload：
  ```scala
  // DataFrame.scala:110-149
  def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame
  def join(right: DataFrame): DataFrame
  def join(right: DataFrame, usingColumn: String): DataFrame
  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame
  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala:613,639,655,683,713,734,764,778,808` 提供 9 个 overload，其中 `:713`：
  ```scala
  def join(right: Dataset[_], usingColumn: String, joinType: String): DataFrame =
    join(right, Seq(usingColumn), joinType)
  ```
  SC3 缺此 single-column + joinType 重载。用户从上游迁移：
  ```scala
  // 上游惯用
  df.join(other, "id", "left_outer")  // 单列 join + joinType
  ```
  在 SC3 编译失败（无匹配重载），必须改写为 `df.join(other, Seq("id"), "left_outer")`。这是 Spark 用户日常 SQL 风格 join 的最常见路径之一。
- **修复**: 在 `DataFrame.scala:140` 后加单列 + joinType 重载：
  ```scala
  def join(right: DataFrame, usingColumn: String, joinType: String): DataFrame =
    join(right, Seq(usingColumn), joinType)
  ```
  同步在 `DataFrameIntegrationSuite` 加单列 + joinType 路径断言。
- **预估**: 5-10 min（1 line overload + 1 测试断言；纯 additive，无 breaking change）
- **价值**: MEDIUM。`df.join(other, "id", "left_outer")` 是 Spark SQL 风格 join 最常见模式之一，public API 缺重载致用户必须改写编译——`Seq("id")` 包装在视觉上与上游不一致；纯 additive 修复，无回归风险。
- **关联**: 与 R56 `UDFRegistration.registerJava` 完全缺失 / R61 `StorageLevel` 缺 3 常量 / R69 `options(java.util.Map)` Java overload 缺失 同属 "public API 完全缺失" 子族系——R56 在 UDF 注册、R61 在持久化级别枚举、R69 在 Java overload、R71 在 join 重载；四者均为纯 additive 缺失，可独立追加。与 R52/R55/R57/R59/R60/R67/R68 同 "public API 与上游契约偏离/缺失" 大 family。与 R69 同 Round 44 typed-API 增量批次但根因正交（R69 是 Java overload、R71 是 Scala overload），可一并落地。Round 44 completeness 维度新增。Round 49 关联：R80 `DataFrameNaFunctions.fill[T]` Java `ju.Map[String, T]` Java-friendly 重载完全缺失——与本项 join 三参单列重载缺失同 "Scala 3 API surface 完全缺失" 子分支（R71 是 join 重载、R80 是 fill 重载），R56/R69/R71/R78/R79/R80 6-site Java/Scala public API 完全缺失批量补齐子族系（DataFrame.join + na.fill + DataFrameStatFunctions.freqItems/sampleBy + reader/writer.options + UDFRegistration.registerJava），0.4.0 集中纯 additive 修复零回归。R81 `DataFrameWriterV2.replace()` / `createOrReplace()` 无参方法完全缺失——同 "DataFrameWriter 系列方法缺失" 子族（R71 在 Reader join、R81 在 WriterV2 sink），与 R71 / R69 / R56 共同形成 0.4.0 reader/writer/join API 缺失批量修复闭环。

---

#### R76. `Catalog.createTable(tableName, path)` 2 参重载硬编码 `source="parquet"` 而上游 Connect path 不调 `setSource(...)` 让 server fall back 到 `spark.sql.sources.default`（cluster default 非 parquet 时静默建出 parquet 表 / 数据格式与用户预期不一致）

- **位置**: `src/main/scala/org/apache/spark/sql/Catalog.scala:229-230`
- **问题**: SC3 `createTable(tableName, path)` 2 参重载分流到内部 5 参实现并硬编码 `source="parquet"`：
  ```scala
  def createTable(tableName: String, path: String): DataFrame =
    createTable(tableName, "parquet", "", StructType(Seq.empty), Map("path" -> path))
  ```
  下游 5 参实现总是调 `setSource(source)`（line ~259）。对照上游 `spark-mine-12/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/Catalog.scala:390-398`：
  ```scala
  override def createTable(tableName: String, path: String): DataFrame = {
    sparkSession.newDataFrame { builder =>
      builder.getCatalogBuilder.getCreateTableBuilder
        .setTableName(tableName)
        .setSchema(...new StructType)
        .setDescription("")
        .putOptions("path", path)
    }
  }
  ```
  上游对应 2 参重载完全不调 `setSource(...)`——server 端 source 字段未设时 fall back 到 `spark.sql.sources.default`（用户可配 orc / json 等）；SC3 客户端硬把它锁成 parquet，用户 cluster-level default 设为非 parquet 时 `spark.catalog.createTable("t", "s3://x/data.json")` 实际建出 parquet 表 / 报 schema mismatch。
- **修复**: 2 参重载分流到独立路径，不调 `setSource(...)`：
  ```scala
  def createTable(tableName: String, path: String): DataFrame =
    val builder = CreateTable.newBuilder()
      .setTableName(tableName)
      .setSchema(DataTypeProtoConverter.toProto(StructType.empty))
      .setDescription("")
      .putOptions("path", path)
    catalogDf(_.setCreateTable(builder.build()))
  ```
  同步在 `CatalogIntegrationSuite` 加：cluster default 配 orc 时 `spark.catalog.createTable("t", "s3://x/data.json")` 实际建表 source 为 cluster default（orc）而非硬编码 parquet。
- **预估**: 10-15 min（删除 2 参 path overload 的 hardcoded `source="parquet"` 分流 + 单独构造 builder 不调 setSource + IntegrationSuite 默认 source fallback 回归）
- **价值**: MEDIUM。cluster default 非 parquet 集群下建表数据格式与用户预期不一致致下游读取报 schema mismatch / 误读为 parquet 错误数据；parquet-default 集群下无可见症状但用户从上游迁移代码行为偏离（typed signature 用户 IDE 看不到任何 source 参数但实际硬编码）。修复成本极小（独立路径不调 setSource）。
- **关联**: 与 R72 `Catalog.createTable` 5 参重载 `description ↔ schema` 形参顺序反转 / R52 `Catalog.listColumns` / `tableExists` / `functionExists` 形参顺序反转 / R59 `Catalog.getTable` / `Catalog.getFunction` 形参顺序反转 同属 "Catalog 公共 API 静默语义偏离" 大族——R52 / R59 / R72 是 arg-order swap，R76 是默认 source fallback 偏离；落点正交可独立修复。R76 与 R72 共同闭合 Catalog `createTable` 全部入口与上游契约对齐（`createTable(name, path)` 2 参 default-source 路径 + `createTable(name, source, description, schema, options)` 5 参形参顺序路径）。Round 46 completeness 维度新增。

---

#### R81. `TableValuedFunction` 缺 `range(start, end, step, numPartitions)` 4 参重载 + 缺 `python_worker_logs()` 方法（`spark.tvf.*` API surface 与 Spark 4.1.0 parity 失齐，PySpark worker debug 入口对外不可达）
- **位置**: `src/main/scala/org/apache/spark/sql/TableValuedFunction.scala:24-32`、`src/main/scala/org/apache/spark/sql/TableValuedFunction.scala:33-67`
- **问题**: SC3 `TableValuedFunction` 缺两个 upstream 公共方法：
  1. `range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame` 4 参重载完全缺失（upstream `sql/api/.../TableValuedFunction.scala:60` 定义为 abstract，Connect 实现在 server 端）；SC3 仅有 1/2/3 参 range，用户从上游迁移 `spark.tvf.range(0, 100, 2, 8)` 编译失败。该缺位与 R77 `SparkSession.range` 4 参是**同根但独立的两个站点**——R77 修 `SparkSession`，但 `spark.tvf.range(...)` 路径仍然返回 3-arg fallback，需单独补齐。
  2. `python_worker_logs(): DataFrame` 完全缺失（upstream Spark 4.1 新增 since 4.1.0，见 `sql/api/.../TableValuedFunction.scala:174`）；Spark 4.1+ debug PySpark workers 的标准入口，SC3 用户无法通过 typed DSL 调用，只能 `spark.sql("SELECT * FROM python_worker_logs()")` 字符串拼接。

  额外：1/2/3 参 `range` 全部委托 `sparkSession.range(...)` 返回 `DataFrame`，与 R77 typed-return 退化族系同源——但本项关注点是**方法 surface 缺失**而非返回类型偏离，因此与 R77 互补（R77 修返回类型，R81 补缺位 surface）。
- **修复**: 两处增量：
  1. 新增 `def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame = sparkSession.range(start, end, step, numPartitions)`（SparkSession 该 4 参 overload 已存在）；
  2. 新增 `def python_worker_logs(): DataFrame = fn("python_worker_logs", Seq.empty)` 走已有 `fn` helper 即可，无需新 proto 字段。
  补 stub-server unit test：mock proto 验证两个新方法生成的 `Relation` 含正确 `tvf.functionName` 与 args。
- **预估**: 10 min（2 个方法 + 2 个 stub-server unit test 走 `fn` 路径）
- **价值**: MEDIUM。闭合 `spark.tvf.*` API surface 与 4.1.0 parity；PySpark worker debug 入口对外可达；用户从上游迁移代码不再需要绕道 `spark.sql(...)` 字符串拼接。
- **关联**: R77 `SparkSession.range` 4 参 + typed-return 的姐妹站点；range 重载 family 由 `SparkSession.range` + `tvf.range` 两个代码路径组成，R77 闭合 SparkSession 侧（含 typed-return），R81 闭合 tvf 侧（含 4 参 surface）。与 R69 `options(java.util.Map)` Java-interop 重载 5 site 缺失 / R71 `DataFrame.join(right, usingColumn, joinType)` 三参单列重载缺失 同 "上游已稳定的便利重载在 SC3 完全缺失" 子族系。Round 48 completeness 维度新增。

---

#### R83. `Dataset[T].localCheckpoint(eager: Boolean, storageLevel: StorageLevel)` 双参重载完全缺失（用户必须使用 `df.localCheckpoint()` 默认 eager + 默认 `MEMORY_AND_DISK` 或 `df.localCheckpoint(eager=false)` 单参——上游稳定 API 在 SC3 完全缺失致用户无法选定 storage level）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrame.scala:400-419`、`spark-upstream/sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala:351-393`
- **问题**: SC3 `DataFrame.localCheckpoint` 仅提供单个 `(eager: Boolean = true)` 重载，硬编码 server-default 的 `MEMORY_AND_DISK` 不暴露给客户端：
  ```scala
  // DataFrame.scala:400, 403
  def checkpoint(eager: Boolean = true): DataFrame = checkpointInternal(eager, local = false)
  def localCheckpoint(eager: Boolean = true): DataFrame = checkpointInternal(eager, local = true)
  // DataFrame.scala:405-419
  private def checkpointInternal(eager: Boolean, local: Boolean): DataFrame =
    val cmd = Command.newBuilder()
      .setCheckpointCommand(CheckpointCommand.newBuilder()
        .setRelation(relation).setLocal(local).setEager(eager).build())
      .build()
    val responses = client.executeCommandWithResponses(cmd)
    ...
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala:351-393` 提供 3 个重载：`localCheckpoint()` / `localCheckpoint(eager: Boolean)` / `localCheckpoint(eager: Boolean, storageLevel: StorageLevel)`——上游 path 在 3 参重载内把 user-selected `StorageLevel` 写入 proto `CheckpointCommand.storageLevel`。SC3 缺失致用户无法定制 checkpoint 存储级别（如 `DISK_ONLY` / `MEMORY_ONLY_2` 等）。
- **修复**: 加 3 参 `localCheckpoint(eager, storageLevel)` 重载并扩展 `checkpointInternal` 接受可选 `StorageLevel` 参数；`CheckpointCommand` proto 已含 `storage_level` 字段（参见 `spark-upstream/sql/connect/common/src/main/protobuf/spark/connect/commands.proto`）：
  ```scala
  // DataFrame.scala
  def localCheckpoint(eager: Boolean, storageLevel: StorageLevel): DataFrame =
    checkpointInternal(eager, local = true, storageLevel = Some(storageLevel))

  private def checkpointInternal(
      eager: Boolean,
      local: Boolean,
      storageLevel: Option[StorageLevel] = None
  ): DataFrame =
    val ckBuilder = CheckpointCommand.newBuilder()
      .setRelation(relation).setLocal(local).setEager(eager)
    storageLevel.foreach(sl => ckBuilder.setStorageLevel(StorageLevelProtoConverter.toProto(sl)))
    val cmd = Command.newBuilder().setCheckpointCommand(ckBuilder.build()).build()
    ...
  ```
  补对应 `IntegrationSuite` 断言：`df.localCheckpoint(eager = true, StorageLevel.DISK_ONLY)` 生成 proto 含 `storage_level.useDisk = true`。
- **预估**: 30 min（1 个 overload + checkpointInternal 扩展 + StorageLevel proto 转换器 + 1 个集成测试）
- **价值**: MEDIUM。`localCheckpoint` 是迭代算法 / 长 lineage DAG 截断典型入口，上游稳定 3 参重载在 SC3 完全缺失致 storage-level-sensitive 工作流（disk-only / replicated-memory）退化为 server-default `MEMORY_AND_DISK`。纯 additive 修复无 breaking change。
- **关联**: 与 R67 `Dataset[T].mergeInto` 返回 untyped `MergeIntoWriter` / R77 `SparkSession.range(...)` 缺 `range(start, end)` 双参重载 / R71 `DataFrame.join(right, usingColumn, joinType)` 三参单列重载缺失 / R81 `tvf.range` 缺 4 参重载 同 "上游已稳定的便利重载在 SC3 完全缺失" 子族系——R67 在 MERGE DSL、R71 在 join、R77 在 SparkSession data gen、R81 在 tvf、R83 在 checkpoint，五者覆盖 SparkSession + DataFrame + Dataset + DataFrameWriter 全公共 API 重载缺失子分支。Round 49 completeness 维度新增。

---

#### R84. `ArrowSerializer.sparkTypeToArrow` 把 `VariantType` 平铺为 `Binary` 而非 Arrow 19.0 `Variant` extension type / 双 buffer struct（与上游 `ArrowWriter` 不一致致 server 端反序列化 fail）

- **位置**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:98, 160-200`
- **问题**: SC3 `ArrowSerializer.sparkTypeToArrow` 对 `VariantType` 平铺为 `Binary`：
  ```scala
  // ArrowSerializer.scala:98
  case types.VariantType => ArrowType.Binary.INSTANCE
  ```
  且 `setArrowValue` 只按 vector 类型派发，无 `(DataType, FieldVector)` 组合分支——当用户 `Row` 字段为 `VariantVal`（持有 `value: Array[Byte]` + `metadata: Array[Byte]` 双 buffer）写入到 `ArrowType.Binary` 配套的 `VarBinaryVector` 时走通用分支：
  ```scala
  // ArrowSerializer.scala:190-192
  case v: VarBinaryVector =>
    val bytes = value.asInstanceOf[Array[Byte]]  // ClassCastException: VariantVal cannot be cast to byte[]
    v.setSafe(idx, bytes, 0, bytes.length)
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/util/ArrowUtils.scala` 与 `ArrowWriter.VariantWriter`——上游把 `VariantType` 映射为 `ArrowType.Struct.INSTANCE` 双子字段 struct（`{value: BINARY, metadata: BINARY}`），与 server 端 IPC reader 协议一致。SC3 客户端写单 buffer Binary（且写 path 直接 ClassCast 失败）致 server 端 Arrow batch 解析失败：`Unable to parse Variant data: expected struct<value:binary,metadata:binary> but found binary`。失败模式：用户 `df.select(parse_json("...").as("v")).collect()` 之后再 `createDataFrame(rows, schema)` 反向回 server，要么客户端 ClassCastException、要么 server 端 schema 不一致 reject。注意 `ArrowDeserializer.scala:202-210` 已正确实现 Variant struct 模式识别（`isVariantStructSchema` 子字段 `value`/`metadata` + Arrow field metadata `variant=true`），但 `ArrowSerializer` 未对应——读写不对称。
- **修复**: 走双 buffer struct path（参考上游 `ArrowWriter.VariantWriter` 与 `ArrowDeserializer.isVariantStructSchema`）：
  ```scala
  // ArrowSerializer.scala:98
  case types.VariantType => ArrowType.Struct.INSTANCE
  // sparkTypeToArrowField 加 VariantType 分支：构造含 value/metadata 两个 BINARY 子字段的 Struct，
  // 并在 metadata 子字段附 Arrow field metadata "variant" -> "true"
  // setArrowValue 加 StructVector + VariantType 联合分支：
  case v: StructVector if dt == types.VariantType =>
    val variant = value.asInstanceOf[VariantVal]
    val valueChild = v.getChild("value", classOf[VarBinaryVector])
    val metadataChild = v.getChild("metadata", classOf[VarBinaryVector])
    valueChild.setSafe(idx, variant.value, 0, variant.value.length)
    metadataChild.setSafe(idx, variant.metadata, 0, variant.metadata.length)
    v.setIndexDefined(idx)
  ```
  补 `ArrowSerializerSuite` 断言：`Row(VariantVal(bytes1, bytes2))` against `StructType(StructField("v", VariantType))` 编码后 Arrow schema 含 `Struct{value:Binary, metadata:Binary}` 且 metadata 子字段附 `variant=true`；编解码 round-trip 经 `ArrowDeserializer` 还原为等价 `VariantVal`。
- **预估**: 3-4 小时（含 `sparkTypeToArrowField` 加 Variant 分支 + `setArrowValue` 加 StructVector+VariantType 分支 + `VariantVal` 类型 vendor + 集成测试 round-trip 校验）
- **价值**: MEDIUM。Variant 是 Spark 4.x 新增数据类型，目前 SC3 用户 typed `VariantVal` round-trip 完全失败——4.1+ 工作流（`parse_json` / `variant_get`）阻塞，但 ROI 中等（仅影响 Variant-using 路径）。
- **关联**: 与 R23 `ArrowSerializer.DecimalVector` 不归一化 `BigDecimal` scale / R24 `ArrowSerializer.setArrowValue` 缺失 `DurationVector` / `IntervalYearVector` / `TimeMicroVector` 写分支 / R25 `ArrowSerializer.MapVector` 不校验 null key 同 "ArrowSerializer 编码与上游 ArrowWriter 偏离" 大族系——R23 是 Decimal scale 归一化偏离、R24 是 Interval/Time 写分支缺失、R25 是 Map null key 校验缺失、R84 是 Variant 类型平铺；四者均为 ArrowSerializer 与上游 ArrowWriter 的类型映射偏离子分支。0.4.0 集中修复可闭合 ArrowSerializer 与 server 端 Arrow batch 协议子族系一致性。Round 49 completeness 维度新增。

---

#### R85. `KeyValueGroupedDataset.cogroupSorted` fallback path 静默丢弃 sortExprs（fallback 至 `cogroup` 时 sortExprs 不传入 proto 致 server 端按 unsorted 顺序计算 group merge——与用户预期排序顺序偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala:310-353, 881-896`
- **问题**: SC3 `KeyValueGroupedDataset.cogroupSorted` 主路径正确把 `thisSortExprs`/`otherSortExprs` 通过 `addInputSortingExpressions`/`addOtherSortingExpressions` 写入 proto（line 345-346），但当 encoder 任一为 null（`keyAg == null || valueAg == null || otherValueAg == null || outAg == null`，line 321）时直接 fallback 到 `cogroupLocal` 私有方法：
  ```scala
  // KeyValueGroupedDataset.scala:321
  if keyAg == null || valueAg == null || otherValueAg == null || outAg == null then
    return cogroupLocal(other, func, outEnc)

  // KeyValueGroupedDataset.scala:881-896 (cogroupLocal)
  private def cogroupLocal[U: ClassTag, R: ClassTag](
      other: KeyValueGroupedDataset[K, U],
      func: (K, Iterator[V], Iterator[U]) => IterableOnce[R],
      outEnc: Encoder[R]
  ): Dataset[R] =
    val leftData = ds.collect().groupBy(groupingFunc)
    val rightData = other.ds.collect().groupBy(other.groupingFunc)
    val allKeys = leftData.keySet ++ rightData.keySet
    val results = allKeys.iterator.flatMap { key =>
      val leftIter = leftData.getOrElse(key, Array.empty[V]).iterator
      val rightIter = rightData.getOrElse(key, Array.empty[U]).iterator
      func(key, leftIter, rightIter)
    }.toSeq
    ...
  ```
  `cogroupLocal` 通过 `.collect().groupBy(groupingFunc)` 在客户端拉取并按 group key 分桶后直接调用 `func`，**完全不应用 `thisSortExprs` / `otherSortExprs`** ——sort 语义在 fallback path 静默丢失。失败模式：用户期待 `cogroupSorted` 按 sortExprs 排序后再 merge group，但 encoder-null 路径接收到的两个 iterator 按 `Array.iterator` 默认顺序（即 collect 顺序，依赖 server 端 partition 排列）merge——结果集语义偏离用户预期，无任何错误信号。这是典型的 silent-fallback 反模式。
- **修复**: 在 `cogroupSorted` 中检测到将走 `cogroupLocal` 时 fail-fast，避免 silent semantic drift：
  ```scala
  def cogroupSorted[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U]
  )(thisSortExprs: Column*)(otherSortExprs: Column*)(...): Dataset[R] =
    val outEnc = summon[Encoder[R]]
    ...
    if keyAg == null || valueAg == null || otherValueAg == null || outAg == null then
      throw new UnsupportedOperationException(
        "cogroupSorted requires AgnosticEncoder-compatible encoders for key/value/output. " +
        "Falling back to client-side cogroup would silently drop sort expressions. " +
        "Either supply Spark-built-in encoders or use cogroup(other)(f) without sort semantics.")
    ...
  ```
  或者扩展 `cogroupLocal` 接受可选 sortExprs 并在客户端按 sortExprs 排序后再传入 func（成本更高、但保留功能）。补测试断言抛错信息含 "silently drop sort expressions"。
- **预估**: 1 小时（fail-fast 替换 + 测试断言抛错信息 + scaladoc 同步说明 encoder-null 路径限制）
- **价值**: MEDIUM。silent-fallback 致用户语义偏离不可观测——fail-fast 让用户感知 server 版本不兼容并显式选择降级（`cogroup` 不带 sort）或升级 server。纯防御性修复，无回归。
- **关联**: 与 R49 `KeyValueGroupedDataset.cogroup` 系列方法 server-dependent + 测试 cancel-only 同 `KeyValueGroupedDataset.scala` 同子系统族系——R49 是 cogroup 类整体 server 依赖性测试覆盖空缺、R85 是 cogroupSorted 特定路径 silent fallback 反模式。与 R29 `SparkSession.failPendingObservations` 跨查询观测污染 / R47 `Catalog.getCreateTableString(asSerde=true)` 文法位置错放 / R48 `DataFrame.union` 默认 `isAll=false` / R50 `DataFrameWriter.jdbc` 缺 partitionBy 校验 / R70 `GroupedDataFrame.{mean,max,min,sum}()` 无参 no-op / R82 `DataFrameReader.table` 静默丢弃 options 同 "客户端 API 路径静默语义偏离 / silent contract violation" 大 family——R29/R47/R48/R50/R70/R82/R85 共同覆盖 SC3 客户端层 silent-violation 反模式跨子系统的 7 个 site，0.4.0 fail-fast 批量修复可闭合 silent-violation family 完整子族系。Round 49 completeness 维度新增。

---

#### R89. `DataFrameReader` 缺 `json(Dataset[String])` / `csv(Dataset[String])` / `xml(Dataset[String])` parse-from-Dataset 重载（用户从上游迁移 `spark.read.json(rddOfStrings.toDS)` 模式 100% 编译失败，且 SC3 无替代 in-memory string parsing 入口）

- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameReader.scala:69`、`:78`、`:86`
- **问题**: SC3 `DataFrameReader` 当前 `json` / `csv` / `xml` 三入口均仅提供 paths 重载：
  ```scala
  // DataFrameReader.scala:69
  def json(paths: String*): DataFrame = format("json").load(paths)
  // DataFrameReader.scala:78
  def csv(paths: String*): DataFrame = format("csv").load(paths)
  // DataFrameReader.scala:86
  def xml(paths: String*): DataFrame = format("xml").load(paths)
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/DataFrameReader.scala:310` `def json(jsonDataset: Dataset[String]): DataFrame`、line 376 `def csv(csvDataset: Dataset[String]): DataFrame`、line 434 `def xml(xmlDataset: Dataset[String]): DataFrame`，上游均提供 `Dataset[String]` 重载支持从已读 in-memory string Dataset 解析。后果：用户已有 `Dataset[String]`（如经 `text()` 读入 + 用户预处理后字符串集合）需走 JSON/CSV/XML schema 推导路径时，SC3 无入口可用，必须 round-trip 写回临时文件再 `spark.read.json(path)`，性能与 ergonomics 双重损失；上游迁移代码 `spark.read.json(stringDataset)` 直接编译失败。
- **建议**: 补 3 个 `Dataset[String]` 重载，内部生成 `Parse` proto relation 接 dataset 作为 input：
  ```scala
  def json(jsonDataset: Dataset[String]): DataFrame =
    parse(jsonDataset, ParseFormat.PARSE_FORMAT_JSON)
  def csv(csvDataset: Dataset[String]): DataFrame =
    parse(csvDataset, ParseFormat.PARSE_FORMAT_CSV)
  def xml(xmlDataset: Dataset[String]): DataFrame =
    parse(xmlDataset, ParseFormat.PARSE_FORMAT_XML)
  private def parse(input: Dataset[String], format: ParseFormat): DataFrame =
    sparkSession.newDataFrame { builder =>
      val parseBuilder = builder.getParseBuilder
        .setInput(input.plan.getRoot)
        .setFormat(format)
        .putAllOptions(extraOptions.asJava)
      userSpecifiedSchema.foreach(s => parseBuilder.setSchema(DataTypeProtoConverter.toConnectProtoType(s)))
    }
  ```
  补 `DataFrameReaderIntegrationSuite` 测试三入口，对照 paths 路径输出 schema 与数据等价。
- **预估**: 2 小时（3 重载 + Parse proto relation 适配 + 集成测试）
- **价值**: MEDIUM。`Dataset[String]` parse 路径用户量中等（多见于流式或预处理场景），但既是上游公开 API 又无替代——堵死从上游迁移路径，需补齐保持 surface 完整。
- **关联**: 与 R55 `DataStreamReader.textFile` typed-return / R68 `DataFrameReader.textFile` typed-return 同 `DataFrameReader` / `DataStreamReader` reader-path 公共 API 入口缺失 / typed-return 偏离族系——R55/R68 是 typed-return 偏离（reader 入口返回类型与上游不符），R89 是 reader 入口重载完全缺失（同一方法名缺 Dataset[String] 重载），三者覆盖 reader 子系统 API 表面偏离的不同维度。Round 50 completeness 维度新增。

---

#### R90. `SparkSession.createDataFrame` 缺 `[A <: Product](data: Seq[A])` typed-Seq 重载与 `(util.List[_], Class[_])` Java-bean-list 重载（仅提供 `Row`-Seq / `util.List[Row]` 两种入口，用户传入 case class Seq / Java POJO List 模式 100% 编译失败，与上游主流构造路径偏离）

- **位置**: `src/main/scala/org/apache/spark/sql/SparkSession.scala:170`、`:184`
- **问题**: SC3 `SparkSession.createDataFrame` 当前仅 2 个公开入口：
  ```scala
  // SparkSession.scala:170
  def createDataFrame(rows: Seq[Row], schema: types.StructType): DataFrame
  // SparkSession.scala:184
  def createDataFrame(rows: java.util.List[Row], schema: types.StructType): DataFrame
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/SparkSession.scala:221` `def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame`（基于 case class / Tuple 自动 schema 推导）与 line 242 `def createDataFrame(data: util.List[_], beanClass: Class[_]): DataFrame`（Java POJO 反射 schema 推导），上游两个主流便捷入口 SC3 全部缺失。后果：（1）用户 `spark.createDataFrame(Seq(Person("a", 1), Person("b", 2)))` 直接编译失败，必须手工构造 `Row` + 手写 `StructType`；（2）跨语言互操作 `spark.createDataFrame(javaPersonList, classOf[Person])` 无入口；（3）上游 SQL programming guide / Spark Examples 多处使用 typed-Seq 路径展示 case class API，SC3 无法承载迁移。
- **建议**: 补 2 个重载，分别经 `ScalaReflection.encoderFor[A]` / `JavaTypeInference.encoderFor(beanClass)` 推导 schema：
  ```scala
  def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame =
    val encoder = ScalaReflection.encoderFor[A]
    val schema = encoder.schema
    val rows = data.map(p => encoder.toRow(p))
    createDataFrame(rows, schema)
  def createDataFrame(data: util.List[_], beanClass: Class[_]): DataFrame =
    val encoder = JavaTypeInference.encoderFor(beanClass)
    val schema = encoder.schema
    import scala.jdk.CollectionConverters.*
    val rows = data.asScala.map(d => encoder.toRow(d.asInstanceOf[Any])).toSeq
    createDataFrame(rows, schema)
  ```
  补 `SparkSessionSuite` / `SparkSessionIntegrationSuite` 测试 case class Seq + Java POJO List → DataFrame schema 与 collect 结果等价。
- **预估**: 3-4 小时（2 重载 + ScalaReflection / JavaTypeInference 端 encoderFor 可用性确认 + Row 构造 + 单元 / 集成测试）
- **价值**: MEDIUM。typed-Seq createDataFrame 是上游用户最常用的 DataFrame 构造入口（教学示例 / 单元测试 fixture / interactive session 高频使用），SC3 强制用户走 manual `Row` + 手写 `StructType` 路径——降低 ergonomics 与从上游 / 教学材料迁移可行性，需补齐保持 surface 完整。
- **关联**: 与 R56 `UDFRegistration.registerJava` Java-interop 缺失 / R69 `DataFrameReader.options(ju.Map[String,String])` Java-interop 缺失 / R71 `DataFrameWriter.partitionBy` 单元素重载缺失 / R78 `DataFrameWriter.options(ju.Map[String,String])` Java-interop 缺失 / R79 `Dataset.{drop,withColumns}(ju.Map[String,Column])` Java-interop 缺失 / R80 `Catalog.{tableExists,functionExists,databaseExists}(database, name)` 双参重载缺失 / R87 `Encoders.{bean,kryo,javaSerialization,row(StructType)}` 工厂缺失 同 "Java-interop / 公共 API 入口缺失" 大 family——R56/R69/R71/R78/R79/R80/R87/R90 共同覆盖 SC3 客户端层 Java-interop 与便捷重载缺失反模式跨子系统的 8 个 site，0.4.0 集中补齐可闭合 missing-API family 完整子族系。Round 50 completeness 维度新增。

---

### 🟢 LOW

#### R10. URL 参数解析异常消息回显原始片段（含可能的 token）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/SparkConnectClient.scala:516,523,531`
- **问题**: 三处 `IllegalArgumentException` 消息插值原始参数串 `$p`（形如 `key=value`）。若用户连接串为 `sc://host:port;token=secret123` 且某段格式异常，错误消息会回显含 `token=secret123` 的片段，进入日志收集系统后构成泄漏。注：`parseUrl` 的 `host`/`port` 错误路径不回显完整 URL，仅这三处 param 解析点有风险。
- **建议**: 异常消息中对值做 redact（保留 key，遮蔽 value），或仅展示参数序号。
- **预估**: 5 min
- **价值**: 防御性。当前默认 logger 不会主动打印 URL，但用户自定义 logger / 异常上报链路可能落盘。

#### R11. `GrpcRetryHandler` 算术：deadline + backoff 双重溢出/负值
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/GrpcRetryHandler.scala:26,36-39`
- **问题（deadline，行 26）**: `val deadline = System.nanoTime() + policy.maxTotalDurationMs * 1_000_000L`：
  - 负值或 0：`maxTotalDurationMs <= 0` 时 `deadline <= nanoTime()`，第一次比较即触发 deadline 路径，重试整体失效。最易触发。
  - 溢出：`maxTotalDurationMs > Long.MaxValue / 1e6 ≈ 9.2e12 ms ≈ 292 年` 时乘法溢出，`deadline` 可能变负，同样立即触发。理论上限。
- **问题（backoff，行 36-39）**: `policy.initialBackoffMs * math.pow(policy.backoffMultiplier, attempt.toDouble).toLong`：当 `backoffMultiplier ≥ 10` 且 `attempt` 较大时，`pow(...).toLong` 接近 `Long.MaxValue`；再乘以 `initialBackoffMs > 1` 整数溢出为负值。`math.min(negative, maxBackoffMs)` 取小者仍为负值，传入 `Thread.sleep(negative)` 抛 `IllegalArgumentException`，被行 45 catch-all 重抛，retry 提前失败。
- **建议**: 入参校验（要求 `maxTotalDurationMs > 0` 与 `backoffMultiplier ≥ 1`），`Math.multiplyExact` 防 deadline 溢出，backoff 用 `math.min(maxBackoffMs, math.max(0L, ...))` 钳制。
- **预估**: 10 min
- **价值**: 防御性。负 `maxTotalDurationMs` 是真实可触发路径；backoff 溢出在长 retry/激进倍率下可触发。

#### R12. `ArrowDeserializer` catch-all scaladoc 过于笼统
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala:155-160`
- **问题**: scaladoc 只说"upstream Spark 行为一致"，未指出 upstream 文件/行号。
- **建议**: 添加 `see o.a.s.sql.connect.client.arrow.ArrowDeserializingIterator#cleanup`。
- **预估**: 1 min
- **价值**: 可调试性。

#### R13. `ArrowSerializer` 内部命名 `rows.size` / `rows.length` / `numRows` 三种混用
- **文件**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:52-69`
- **问题**: 同一函数内出现三种写法：`rows.size`（行 52、69）、`rows.length`（行 55，与 `numRows` 同行赋值）、`numRows` 局部变量（声明于行 55，使用于行 57 的 while 条件）。
- **建议**: 全部统一使用 `numRows` 局部变量。
- **预估**: 2 min
- **价值**: 可读性。

#### R14. `setPlanCompressionOptions` 已无调用方（dead code 嫌疑）
- **文件**: `src/main/scala/org/apache/spark/sql/connect/client/SparkConnectClient.scala:140-141`
- **问题**: CompressionState enum 重构后该 setter 无内部调用。
- **建议**: 先 grep 确认无外部测试调用，再删除或加 `@deprecated`。
- **预估**: 5 min（含外部 grep）
- **价值**: 消除冗余 API 表面。

#### R16. `Row.toString` / `Row.mkString` 对 `Array[Byte]` 与嵌套结构渲染失真
- **文件**: `src/main/scala/org/apache/spark/sql/Row.scala:176-180`
- **问题**: 两处都对值走裸 `v.toString`。`Array[Byte]` 渲染为 `[B@1f3e2c`、嵌套 `Row` / `Seq` / `Map` 走 Scala 集合 toString，对调试与日志几乎无用。与 R9 同源（`Row.scala:128-130, 148-150`）但作用于非 JSON 路径，且无契约约束输出格式，因此降级为 LOW。
- **建议**: 二选一：
  - A. 复用 R9 的类型感知 helper：`Array[Byte]` → 短哈希或 base64、嵌套 `Row` → 递归 `toString`、`Seq`/`Map` → 递归
  - B. scaladoc 显式说明 "binary and nested values render via JVM `toString`; use `json` for stable serialization"
- **预估**: 10 min（A）/ 2 min（B）
- **价值**: 提高诊断输出可读性；与 R9 修复合并实现可分摊成本（同 helper 函数）。

#### R17. `ArrowDeserializerSuite` 编码 helper 中 `ArrowStreamWriter` 未走 try/finally
- **文件**: `src/test/scala/org/apache/spark/sql/connect/client/ArrowDeserializerSuite.scala:22-36`（以及类似的 inline 编码块 ~232-237 / ~289-294）
- **问题**: `encodeArrowBatch` helper 在外层 `try` 内创建 `ArrowStreamWriter`，依次执行 `writer.start() / writer.writeBatch() / writer.end() / writer.close()`（行 28-32），其中任何一步抛异常都会跳过 `writer.close()`。外层 `finally` 仅关闭 `root` 与 `allocator`（行 34-36），writer 自身持有的内部缓冲（DictionaryProvider / channel）未释放。当前测试都 happy-path，故未触发，但属于 R6（资源泄漏未被 suite 显式覆盖）家族扩展。
- **建议**: 将 writer 包入嵌套 `try/finally`（或 `Using.resource`），确保异常路径下也调用 `writer.close()`。
- **预估**: 5 min
- **价值**: 防御性。与 R6 合并实施时，统一资源管理风格。

#### R22. `SparkSession.close()` 不清理 `StreamingQueryListenerBus` + `append` 持锁阻塞 30s gRPC
- **文件**:
  - `src/main/scala/org/apache/spark/sql/SparkSession.scala:336-339`
  - `src/main/scala/org/apache/spark/sql/streaming/StreamingQueryListenerBus.scala:34-50,52-60`
- **问题（关闭路径，行 336-339）**: `SparkSession.close()` 仅调用 `client.close()` 与清理 active/default session 引用，**未调用** `streams.streamingQueryListenerBus.close()`。当用户已通过 `streams.addListener(...)` 注册过监听器时：
  - bus 的 daemon 线程（`StreamingQueryListenerBus.scala:46` 设 `setDaemon(true)`）继续在已关闭的 gRPC channel 上阻塞 / poll，直到自然抛错被 `queryEventHandler` 内的 catch-all 捕获并写入 `[WARN]` 到 stderr（行 120）。daemon 性质保证不阻塞 JVM 退出，但对长生命周期进程（共享 JVM、嵌入式 REPL）会留下一段噪声日志窗口。
  - server 端的 listener 注册不会被显式 unregister；server 在 stream 终止后才感知 client 离开，期间无法回收对应资源。
- **问题（注册路径，行 34-50）**: `append` 使用 `lock.synchronized { ... registerServerSideListener() ... }` 包住 30s 超时（`RegisterListenerTimeoutNanos = 30L * 1e9`）的 gRPC 注册调用。若注册期间另一线程调用 `remove`、`close`、或并发 `append`（均也走 `lock.synchronized`），最长会阻塞 30s。生产中通常单线程使用，但多线程 streaming 集成场景下属真实可观测的串行化点。
- **建议**:
  - A. `SparkSession.close()` 中在 `client.close()` 之前调用 `if streams != null then streams.streamingQueryListenerBus.close()`（注意 `streams` 是惰性字段，仅在已实例化时关闭以避免触发懒初始化）。
  - B. `append` 重构为 "先 register（无锁），成功后再在锁内提交 listener+thread"——失败时无需触碰共享状态：
    ```scala
    def append(listener: StreamingQueryListener): Unit =
      val needRegister = lock.synchronized { listeners.isEmpty }
      val iter = if needRegister then registerServerSideListener() else null
      lock.synchronized {
        listeners.add(listener)
        if needRegister then
          val thread = Thread(() => queryEventHandler(iter))
          thread.setDaemon(true); thread.setName(...); thread.start()
          executionThread = Some(thread)
      }
    ```
    （边界情况：两个线程同时观察到 `isEmpty` 时会重复 register；可用 `AtomicBoolean registering` 做 CAS 守卫，或退而求其次接受偶发重复 register——server 端幂等。）
- **预估**: 15-25 min（含两点修复 + 单元测试用 mock client 验证关闭路径触达 bus.close）
- **价值**: 防御性 + 并发正确性。当前 daemon 线程行为对 JVM 退出无害，但对嵌入式 / REPL / 长 driver 场景留下生命周期噪声；锁内 30s gRPC 是真实串行化点。两项均与 streaming 路径耦合，coverage 已排除（`build.sbt:142`），属"server-dependent" 类目，因此降级为 LOW。

#### R40. `StructType.treeString(maxLevel: Int)` 缺失 `maxLevel <= 0` 兜底（与上游"非正即无限"惯例偏离，`treeString(0)` 输出空骨架）
- **文件**: `src/main/scala/org/apache/spark/sql/types/DataType.scala:186-206`
- **问题**: SC3 实现：
  ```scala
  // DataType.scala:186-206
  def treeString(maxLevel: Int): String =
    val sb = StringBuilder()
    sb.append("root\n")
    def buildTree(fields: Seq[StructField], indent: Int): Unit =
      if indent <= maxLevel then        // ← maxLevel=0 时 1<=0 false，整段短路
        fields.foreach { f => ... }
    def recurseType(dt: DataType, indent: Int): Unit =
      if indent < maxLevel then
        ...
    buildTree(fields, 1)
    sb.toString
  ```
  对照上游 `spark-mine-12/sql/api/src/main/scala/org/apache/spark/sql/types/StructType.scala:382-389`：
  ```scala
  def treeString(maxDepth: Int): String = {
    val stringConcat = new StringConcat()
    stringConcat.append("root\n")
    val prefix = " |"
    val depth = if (maxDepth > 0) maxDepth else Int.MaxValue   // ← 非正参数 reroute 到全展开
    fields.foreach(field => field.buildFormattedString(prefix, stringConcat, depth))
    stringConcat.toString()
  }
  ```
  上游惯例：`treeString(0)` / `treeString(-1)` 等价于 `treeString(Int.MaxValue)`（即 no-arg `treeString`）；SC3 的实现把 0 / 负值都当作 "limit at level 0"，输出仅 `"root\n"`——空骨架。用户从上游迁移到本客户端时若手写 `schema.treeString(0)` 期望"全部展开"会得到空输出，悄无声息丢字段。
- **建议**: 在 `treeString(maxLevel)` 入口对参数做 `<= 0 → Int.MaxValue` 的兜底，与上游对齐：
  ```scala
  def treeString(maxLevel: Int): String =
    val effectiveMax = if maxLevel > 0 then maxLevel else Int.MaxValue
    val sb = StringBuilder()
    sb.append("root\n")
    def buildTree(fields: Seq[StructField], indent: Int): Unit =
      if indent <= effectiveMax then ...
    def recurseType(dt: DataType, indent: Int): Unit =
      if indent < effectiveMax then ...
    buildTree(fields, 1)
    sb.toString
  ```
  或：`def treeString(maxLevel: Int): String = treeString(if maxLevel > 0 then maxLevel else Int.MaxValue, internal = true)` 走辅助 overload。补单测覆盖 `treeString(0)` / `treeString(-1)` 与 `treeString` 同输出。
- **预估**: 5 min（一行兜底 + 1-2 个单测）
- **价值**: API 行为与上游一致，避免迁移用户因边界值不同得到非预期空输出。display-only 影响，无功能性 / 数据正确性后果，因此 LOW。

---

#### R63. `Observation.get` 在 `row.schema = None` 路径下静默返回 `Map.empty`（成功 metric 已写入但 schema 丢失致用户误判为"无指标"，与 R30 / R43 同根 schema 丢失族系 observe 入口下的 cosmetic 表象）

- **位置**: `src/main/scala/org/apache/spark/sql/Observation.scala:49-53`
- **问题**: SC3 实现：
  ```scala
  // Observation.scala:49
  if row == null then Map.empty
  else
    row.schema match
      case Some(s) => row.getValuesMap[Any](s.fields.map(_.name).toSeq)
      case None    => Map.empty
  ```
  `row.schema = None` 路径走兜底返回 `Map.empty`——但 `row` 本身非 null，意味着 server 已成功发送 metric Row，仅是 client 端 LiteralValueProtoConverter（R43 路径）或 ArrowDeserializer（R30 嵌套 struct 路径）未挂上 schema。用户拿到 `Map.empty` 误判为"无指标"，但实际指标值已在 `row` 内只是无字段名访问通道，是与 R30 / R43 同根 schema 丢失族系在 `Observation.get` 入口下的 user-visible 表象——R30 / R43 修复后该路径自然不触发，但若未修则用户在 observe 路径下 silent 失去全部指标 visibility。
- **修复**: schema=None 兜底改为构造合成索引名 + warning 日志：
  ```scala
  case None =>
    // schema-less metric Row should not occur once R30/R43 are fixed; fall back to indexed column
    // names so users at least see metric values exist (rather than silent Map.empty).
    val log = java.util.logging.Logger.getLogger(getClass.getName)
    log.warning(s"Observation '$name' received metric Row without schema; using synthetic c0/c1/... names")
    (0 until row.size).map(i => s"c$i" -> row.get(i)).toMap
  ```
  或更激进——抛 `IllegalStateException("Observation metric Row missing schema; this indicates a client deserialization bug, please file an issue")`，强制 R30 / R43 路径修复（loud failure 比 silent map 用户更易诊断）。两种修复均需 ObservationSuite 加单测：构造 schema-less metric Row 路径下 `obs.get` 不返回 `Map.empty`（要么 synthetic 名字要么 IllegalStateException）。
- **预估**: 5-10 min（兜底分支改造 + 单测）
- **价值**: LOW（cosmetic 边界处理，root cause 在 R30 / R43，本项是表象修复增加 visibility）。优先级低于 R30 / R43 本身的修复——如先修 R30 / R43 则本项分支自然不触发；但保留 visibility 兜底避免未来其他 schema 丢失路径再次产生 silent failure。
- **关联**: 与 R30 `ArrowDeserializer` 嵌套 struct 行丢失 schema / R43 `LiteralValueProtoConverter.toScalaValue` STRUCT 分支构造 schema-less Row 同 schema 丢失族系——R30 在 Arrow 路径、R43 在 LiteralValueProtoConverter observe 路径、R63 在 `Observation.get` 出口路径，三者构成 observe 流程从底层解码到上层 user API 的 schema 完整性 chain，root cause 在 R30 / R43、R63 是表象兜底；与 R29 / R62 同 `Observation` 路径但不同维度（R29 跨查询污染 future 完成、R62 默认超时过严、R63 schema 兜底 visibility 缺口）。Round 43 completeness 维度新增。

---

#### R64. `ArrowSerializer` 用字符串 `"UTF-8"` 而非 `StandardCharsets.UTF_8` 调 `String.getBytes`（隐式声明 checked `UnsupportedEncodingException` 且 idiom 与 Java 11+ 推荐相反）

- **位置**: `src/main/scala/org/apache/spark/sql/ArrowSerializer.scala:172,234`
- **问题**: SC3 两处用字符串字面量 `"UTF-8"` 调 `getBytes`：
  ```scala
  // ArrowSerializer.scala:172
  case v: VarCharVector  => v.setSafe(idx, value.toString.getBytes("UTF-8"))
  // :234
  val bytes = s.getBytes("UTF-8")
  ```
  `String.getBytes(String charsetName: String)` 是 checked-throw 重载，签名 `throws UnsupportedEncodingException`——虽然 `"UTF-8"` 在 JVM 上永远不会触发该异常（UTF-8 是 JVM 必备 charset），但编译器/IDE 仍把该方法标记为可能抛 checked 异常，调用代码 idiom 逊色。Java 7+ 推荐 `String.getBytes(Charset charset)` 重载，传 `java.nio.charset.StandardCharsets.UTF_8` 常量——非 checked-throw、编译时类型保证、避免运行时 charset name lookup。
  对照本仓其余正确 idiom（`grep` 验证）：`SparkConnectClient.scala:31-35` `fullUrl` decoded path 使用 `URLDecoder.decode(s, StandardCharsets.UTF_8)`、`PlanCompressionOptions` zstd helper 使用 `s.getBytes(StandardCharsets.UTF_8)`——同一仓内 idiom 不一致（仅 `ArrowSerializer.scala` 两站点用 string-name 重载）。
- **修复**: 替换为 `StandardCharsets.UTF_8` 常量：
  ```scala
  import java.nio.charset.StandardCharsets

  // :172
  case v: VarCharVector  => v.setSafe(idx, value.toString.getBytes(StandardCharsets.UTF_8))
  // :234
  val bytes = s.getBytes(StandardCharsets.UTF_8)
  ```
  同步全仓 `grep -rn '"UTF-8"' src/main` 确认仅 `ArrowSerializer.scala` 两处剩余，无其他遗漏。
- **预估**: 2 min（两处替换 + import 添加）
- **价值**: LOW（idiom + checked-throw 消除）。无功能性 / 性能影响，纯代码风格 / API 现代化。可作为 ArrowSerializer 后续编辑时顺手修复。
- **关联**: 与 R10 URL 参数解析异常消息回显原始片段（`SparkConnectClient.scala:31-35` 同文件 fullUrl 路径）同 charset / URL idiom 微观一致性族系——R10 是异常消息泄漏 token 风险、R64 是 charset string-name idiom 落后，两者均为本仓代码风格 / 安全 idiom 微观可观察分歧但落点不同。Round 43 completeness 维度新增。

---

#### R66. `getPlanCompressionOptions` 把 `getConfig("...threshold").toInt` 的 `NumberFormatException` 留给 `NonFatal` 兜底致每次调用重复 RPC 重试不缓存（坏 config 永久成本）

- **位置**: `src/main/scala/org/apache/spark/sql/connect/client/SparkConnectClient.scala:113-137`
- **问题**: SC3 实现：
  ```scala
  // SparkConnectClient.scala:113-137
  private def getPlanCompressionOptions: CompressionState =
    _planCompressionOptions match
      case CompressionState.Uninitialized =>
        synchronized {
          _planCompressionOptions match
            case CompressionState.Uninitialized =>
              try
                val state = CompressionState.Enabled(PlanCompressionOptions(
                  thresholdBytes =
                    getConfig("spark.connect.session.planCompression.threshold").toInt,
                  algorithm =
                    getConfig("spark.connect.session.planCompression.defaultAlgorithm")
                ))
                _planCompressionOptions = state
                state
              catch
                case _: ClassNotFoundException | _: UnsupportedOperationException =>
                  _planCompressionOptions = CompressionState.Disabled
                  CompressionState.Disabled
                case e if NonFatal(e) =>
                  // Transient error — do NOT cache; let caller retry later
                  CompressionState.Uninitialized
            case cached => cached
        }
      case cached => cached
  ```
  `getConfig("spark.connect.session.planCompression.threshold").toInt` 在 server 配置错误返回非数字（如 `"large"` / 空字符串）路径下抛 `NumberFormatException`——属 `NonFatal` 但不属 `ClassNotFoundException`/`UnsupportedOperationException`，落入 transient catch 分支不缓存。每次后续 `tryCompressPlan` 调用重新进 synchronized lock + 发起新 `getConfig` RPC + 同样抛 `NumberFormatException` + 同样不缓存——形成永久性 per-plan-submission RPC 重试循环，server 配置错误下 plan submission 路径成本被无限放大。
  正确语义判别：`NumberFormatException` 是 **永久性配置错误**（server 配置不变情况下重试结果不变），应缓存为 `Disabled` 而非 `Uninitialized`；只有真正的 transient 错误（network timeout / `StatusRuntimeException` 等）才应保持 `Uninitialized` 让 caller retry。
- **修复**: 把 `NumberFormatException` 与 `ClassNotFoundException`/`UnsupportedOperationException` 一并归入永久错误分支：
  ```scala
  catch
    case _: ClassNotFoundException | _: UnsupportedOperationException | _: NumberFormatException =>
      _planCompressionOptions = CompressionState.Disabled
      CompressionState.Disabled
    case e if NonFatal(e) =>
      // Transient error (network etc) — do NOT cache; let caller retry later
      CompressionState.Uninitialized
  ```
  补 `SparkConnectClientSuite` 单测：mock `getConfig` 返回 `"not-a-number"`，验证 `tryCompressPlan` 第一次后状态变为 `Disabled`、后续调用不再触发 `getConfig` RPC（spy assertion）。
- **预估**: 3-5 min（catch 子句扩展 + 单测）
- **价值**: LOW（性能 + 正确语义微调，server 配置错误是 corner case）。修复后避免 server 配置错误路径下 client 持续重发 RPC，改善 plan-submission cold-path 成本，但 server 配置正常路径下零影响。
- **关联**: 与 R14 `setPlanCompressionOptions` 已无调用方 dead code 嫌疑同 PlanCompressionOptions 子系统 cosmetic 缺陷族系——R14 是 dead code 清理、R66 是错误分类语义微调，两者均为该子系统的次要质量改进；与 R8 `GrpcRetryHandler` `RetryException` 分支无 deadline 检查 + `lastException` 可能丢失同 RPC 错误分类语义族系——R8 是 retry exception 错误分类不准、R66 是 plan compression 配置错误分类不准，两者均为客户端错误分类语义偏离致 retry / cache 逻辑次优。Round 43 completeness 维度新增。

---

#### R78. `DataFrameStatFunctions.freqItems(cols: Seq[String])` 内联构造 proto 不调 `setSupport` 默认 0 + 缺 `Array[String]` Java-friendly 重载
- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala:87-90`
- **问题**: SC3 现状（`DataFrameStatFunctions.scala:87-90`）：
  ```scala
  def freqItems(cols: Seq[String]): DataFrame =
    val fiBuilder = StatFreqItems.newBuilder().setInput(df.relation)
    cols.foreach(fiBuilder.addCols)
    df.withRelation(_.setFreqItems(fiBuilder.build()))
  ```
  内联构造 proto 不调 `setSupport(0.01)`——`StatFreqItems.support` 字段未显式赋值，依赖 proto3 默认值 `0.0`；server 端 `FreqItems` aggregator `support=0.0` 行为未定义（divide-by-zero / 全部 item 均"frequent"）。上游 `sql/api/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala:326`：
  ```scala
  def freqItems(cols: Seq[String]): DataFrame = freqItems(cols, 0.01)
  ```
  委托双参版本显式传 `0.01`——upstream 默认 1% support，与 scaladoc 描述"default support of 1%"一致。同时上游 line 248/265 提供 `Array[String]` Java-friendly 重载（双参 + 单参），SC3 完全缺失致 Java 调用方需手工 `Arrays.asList()` 包装。
- **修复**: 单参版本改为委托：
  ```scala
  def freqItems(cols: Seq[String]): DataFrame = freqItems(cols, 0.01)
  ```
  补两个 Array 重载：
  ```scala
  def freqItems(cols: Array[String], support: Double): DataFrame = freqItems(cols.toSeq, support)
  def freqItems(cols: Array[String]): DataFrame = freqItems(cols.toSeq, 0.01)
  ```
- **预估**: 10-15 min（含 scaladoc 同步 + 单测验证 setSupport=0.01 出现在 proto 中）
- **价值**: LOW（修复 default support=0 致 server 行为偏离 scaladoc + Java 友好性补齐；用户多用双参版本，单参 default 路径触达率较低）
- **关联**: 与 R56 `DataFrameNaFunctions` Java-friendly `Array[String]` 重载缺失同 Java-friendly 重载缺失族系（R56/R78/R79 闭合 stat/na 子系统 Java 友好性）；与 R52/R59/R72 `Catalog` arg-order swap + R76 `createTable(name, path)` 默认 `source` fallback 偏离同 "Catalog 公共 API 与上游契约偏离" 大族（共同覆盖 "上游已稳定的便利重载/默认值在 SC3 完全缺失" 子线索）。Round 47 completeness 维度新增。Round 49 关联：R80 `DataFrameNaFunctions.fill[T]` Java `ju.Map[String, T]` Java-friendly 重载完全缺失——与本项 `freqItems` `Array[String]` 重载缺失同 "stat/na 子系统 Java-friendly 入口缺失" 子分支（R78 在 stat freqItems、R80 在 na fill），R56/R69/R71/R78/R79/R80 6-site Java-interop 公共 API 完全缺失批量补齐子族系（DataFrameStatFunctions.freqItems + DataFrameStatFunctions.sampleBy + DataFrameNaFunctions.fill + UDFRegistration.registerJava + reader/writer.options + DataFrame.join），0.4.0 集中纯 additive 修复 stat + na + UDF + reader/writer + join 全公共 API Java-interop 入口零阻塞。

---

#### R79. `DataFrameStatFunctions.sampleBy[T]` 缺 `ju.Map[T, jl.Double]` Java-friendly 重载
- **位置**: `src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala:93-113`
- **问题**: SC3 现状仅 2 个 Scala-Map 重载：
  ```scala
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame
  ```
  上游 `sql/api/src/main/scala/org/apache/spark/sql/DataFrameStatFunctions.scala:358/378/415/434` 提供 4 个重载（Scala-Map + `ju.Map` 双轨）：
  ```scala
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame
  def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame
  def sampleBy[T](col: Column, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame
  ```
  Java 调用方需手工 `JavaConverters.mapAsScalaMap()` + `Double` 装箱 unwrap，且每次升级 SC3 版本需重新审视 binary 兼容性。
- **修复**: 补两个 ju.Map 重载委托现有 Scala-Map 实现：
  ```scala
  def sampleBy[T](col: String, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame =
    sampleBy(Column(col), fractions, seed)
  def sampleBy[T](col: Column, fractions: ju.Map[T, jl.Double], seed: Long): DataFrame =
    import scala.jdk.CollectionConverters.*
    sampleBy(col, fractions.asScala.toMap.view.mapValues(_.doubleValue()).toMap, seed)
  ```
- **预估**: 10 min（含 import + 单测验证 ju.HashMap 输入产生与 Scala-Map 一致 proto）
- **价值**: LOW（Java 友好性补齐；纯 Scala 用户零影响）
- **关联**: 与 R56 `DataFrameNaFunctions` ju.Map Java-friendly 重载缺失 + R78 `freqItems` Array[String] 重载缺失同 Java-friendly 重载缺失子族系（R56/R78/R79 闭合 stat/na 子系统 Java 友好性 ju.Map 和 Array 双轨补齐）。Round 47 completeness 维度新增。Round 49 关联：R80 `DataFrameNaFunctions.fill[T]` Java `ju.Map[String, T]` Java-friendly 重载完全缺失——与本项 `sampleBy[T]` `ju.Map[T, jl.Double]` 重载缺失同 "ju.Map[K, V] Java-friendly 入口缺失" 直接子分支（R79 在 stat sampleBy、R80 在 na fill），均使用 `import scala.jdk.CollectionConverters.*` + `.asScala.toMap` 桥接到现有 Scala 实现，纯 additive 0.4.0 修复零回归。R56/R69/R71/R78/R79/R80 6-site Java-interop 公共 API 完全缺失批量补齐子族系闭合。

---

## ⚪ 不修建议

### encode/decode 命名不对称（保留）
- **文件**: `ArrowSerializer.scala` / `ArrowDeserializer.scala`
- **现状**: serializer 用 `encodeRows`，deserializer 用 `fromArrowBatchWithSchema`。
- **决策**: 保留——与 upstream Spark Connect 命名对齐。可在 scaladoc 中互相 `@see` 引用以便发现。

### `Column.lit` 对非原语类型走 `toString` 兜底（保留）
- **文件**: `src/main/scala/org/apache/spark/sql/Column.scala:494-511`
- **现状**: `lit` 仅 type-aware 处理 null / Boolean / Byte / Short / Int / Long / Float / Double / String / Column；其余落入行 510 的 `case v => setString(v.toString)`。这意味着 `df.filter($"d" === java.sql.Date.valueOf(...))` 实际把日期字面量编码为字符串，server 端比较语义与 Date 列不匹配；`Array[Byte]` 编码为 JVM 标识串 `[B@deadbeef`；`BigDecimal` / `Row` / `Seq` / `Map` 同理。
- **决策**: 保留——`code-review-todo.md` 行 96 已显式将此项列入"不修复（设计选择 / upstream 对齐，已文档化）"。如需提升保真度，应通过新 issue 走 typed-literal 路径（`setDate` / `setTimestamp` / `setDecimal` / `setBinary`）一次性引入，而非作为 round 2 的修复项。

---

## ✅ 已验证清洁项（无需处理）

### 安全
- `fullUrl` 不出现在 `toString` 输出中（`SparkConnectClient.toString` 使用 `connectionUrl`，已 redact）
- `getPlanCompressionOptions` 的 DCL 初始化（`@volatile` + 双重检查）正确；状态机为单向 Uninitialized → Enabled/Disabled，Disabled 一经写入即粘滞，因此 `tryCompressPlan` catch 路径中的无锁写 Disabled 不引入语义竞态（见 R14：`setPlanCompressionOptions` 是 R14 标记的待清理 dead code 嫌疑入口，移除后该路径将彻底单调）
- Retry 路径无副作用泄露
- Arrow allocator 256GB cap 有效阻断恶意 schema 触发的 OOM
- 无硬编码 secrets / API key / token

### 一致性
- `-Werror` + `-Wunused:all` 在 Compile 启用，Test 关闭：合理
- CI 矩阵 JDK 17 + 21：覆盖前向兼容
- coverage 仅在 JDK 17 跑：避免重复，决策合理

### 测试
- 27 个 property test 与 `ArrowSerializerSuite` example test 互补，不冗余
- `deepEqual` 辅助函数在实现层面支持 `Array[Byte]` / nested Row / `Seq` / `Map`，但当前 generator 仅覆盖 primitives + `ArrayType[Int]/[String]`；`MapType` 与嵌套 `StructType` 路径未被 property 测试触达（example 测试已覆盖）；`ArrayType` 非原语元素类型亦未触达——见 R1
- `assertRoundTrip` 对空行 `Seq` 短路返回，不调用 decoder——空 byte array 解码路径由 `ArrowDeserializerSuite` 显式覆盖，不在 property suite 范围内（设计决策）
- generator 对齐 decoder 规范形式（UTC midnight Date / microsecond Timestamp）：消除 TZ 漂移

---

## 建议下一步

| 方案 | 内容 | 预估 |
|---|---|---|
| **A0** | 仅修 R48（CRITICAL，`DataFrame.union`/`unionByName`/`unionAll` + typed `Dataset.union`/`unionAll`/`unionByName` 默认 `isAll=false` 致服务端套 `Distinct(union)`，数据正确性退化为 UNION 而非 UNION ALL；最低成本闭合 CRITICAL） | 10-15 min |
| **A** | 仅修 R1（HIGH，数据丢失） | 30-45 min |
| **B** | R48 + R1 + R2 + R15 + R20 + R28 + R29 + R31 + R34 + R41 + R45 + R46 + R49 + R52 + R55 + R58 + R59 + R60 + R67 + R68 + R72 + R73 + R74 + R75（一项 CRITICAL + 二十三项 HIGH——前述项不变；R75 `DataStreamWriter.foreach` 总是用 `UnboundRowEncoder` 序列化 `ForeachWriterPacket` 致 typed `ForeachWriter[T]` 在 server 端按 `Row` 解码 → `ClassCastException: Row cannot be cast to T`，根因是 `DataStreamWriter` 类缺 `[T]` 类型参数无从拿 dataset encoder，与 R67 / R55 / R68 / R74 同 streaming/typed-DSL 公共 API typed 契约偏离族但本项最严重（运行时 wire-protocol 失败而非编译错），60-90 min） | 505-730 min |
| **C** | B + R3 + R5(A) + R8 | 522-747 min |
| **D** | B + R3 + R5(A) + R6 + R7(B) + R8 | 542-767 min（取 R7 选项 A 时 557-782） |
| **E** | B + R3 + R5(A) + R6 + R7(B) + R8 + R9 | 572-812 min |
| **F** | E + R16(B 文档化) | 574-814 min |
| **G** | 全部关闭，本轮 review 结束 | 0 min |

R4（测试 flake 修复）、R17（writer 资源管理收敛）、R18（中断标志恢复，5 min）、R19（`Row.fromSeqWithSchema` 不校验长度，JSON 静默丢字段 / IOOB，MEDIUM，10 min）、R22（streaming bus 关闭 + append 锁内 30s gRPC，跨防御 ∩ 并发桶）、R21/R23/R24/R25/R26/R27/R30/R32/R42/R43/R44（Arrow 编解码补丁组——可一并修以闭合 Arrow 路径；R30 `ArrowDeserializer` 嵌套 struct 行丢失 schema 致下游 typed accessor 失败，与 R43 同根因不同落点（Arrow 路径 vs. LiteralValueProtoConverter 路径），可一并落地，10-15 min；R32 `arrowTypeToSparkType` Array/Map child nullable 传播失真同族系；R42 `IntervalYearVector` 解码返回 raw `Int` 与 R26/R38 同属 vector-specific 解码 schema/值类型不对称族系，5-10 min；R43 `LiteralValueProtoConverter.toScalaValue` STRUCT 分支 schema-less Row 与 R30 同根因不同落点（observe / literal 路径 vs. ArrowDeserializer Arrow 路径），可与 R30 一并落地，10-15 min；R44 `LiteralValueProtoConverter.toScalaValue` 三 INTERVAL 分支返回原始 tuple/Int/Long 与 schema 宣称的 `CalendarIntervalType`/`YearMonthIntervalType`/`DayTimeIntervalType` 异步，与 R38/R43 同方法同根因，可一并落地，15-20 min）、R33（`ExecutePlanResponseReattachableIterator.next()` 不检查 `resultComplete`，迭代器状态机不对称）、R35（`StructType.sql` 缺 override，含嵌套 struct 列 DDL 字段丢失，DataType DDL 保真）、R36（`functions.rand`/`randn` 无参重载 seed=0L 致重复序列，与 R34 同属 "functions.scala 客户端构造表达式缺失上游状态注入" 族系但独立追加，10-15 min）、R37（`DataFrame.sample`/`randomSplit` + `Dataset.sample`/`randomSplit` 五个 API 入口默认 `seed=0L` 致确定性子集，与 R36 同根因不同入口、可一并落地，20-30 min）、R38（`LiteralValueProtoConverter.toScalaValue` TIME 分支返回 raw `Long` 致 `df.observe("m", first(timeCol))` 路径 typed accessor 失败，MEDIUM，与 R20/R26 同属 vector-specific TIME / TZ-bearing 类型解码族系但落点在 LiteralValueProtoConverter 而非 ArrowDeserializer，10-15 min）、R39（`StreamingQueryListenerBus.registerServerSideListener` `hasNext=false` 落空兜底 silent registration failure，与 R22 同属 streaming bus 生命周期 / 错误路径族系，MEDIUM，10-15 min）、R40（`StructType.treeString(0)` 缺失 `<= 0 → Int.MaxValue` 兜底致空骨架输出，与上游 `StructType.scala:382-389` 偏离，display-only LOW，5 min）、R47（`Catalog.getCreateTableString(asSerde=true)` 拼接 `SHOW CREATE TABLE AS SERDE \`name\`` 与 `SqlBaseParser.g4:334` 文法 `SHOW CREATE TABLE identifierReference (AS SERDE)?` 偏离致 server 抛 `ParseException`，MEDIUM，5 min）、R50（`DataFrameWriter.jdbc` 缺 `assertNotPartitioned`/`assertNotBucketed`/`assertNotClustered` + `validatePartitioning()` 四重客户端校验致 partitionBy/bucketBy/clusterBy + jdbc 组合 silent contract violation，与 R47 同 silent contract violation 模式但落点在 sink format 而非 SQL 字面量，MEDIUM，20-30 min）、R51（`ArtifactManager.addArtifactsImpl` 用 inline `StreamObserver[AddArtifactsResponse]` 直接调 `asyncStub.addArtifacts` 注册流响应——既不走 `responseValidator.wrapIterator` 包裹也不在 `onNext` 中校验 `v.getSessionId`，server 端 session swap 静默接受，与 R45 同 client session 防护族系但 R45 落点为 `ResponseValidator.wrapIterator` catch-all 缺 session-id mismatch 单独路径而 R51 落点为 ArtifactManager async streaming 路径完全绕过 ResponseValidator 保护层，MEDIUM，5-10 min）、R52（`Catalog.listColumns`/`tableExists`/`functionExists` 形参顺序与上游相反致 silent argument-swap，与 R47 同 Catalog 公共 API 静默契约违反族系——R47 是 SHOW CREATE TABLE AS SERDE 文法偏离致 server 端 ParseException，R52 是参数顺序静默互换致用户传入合法库/表名后查询错库或抛 NoSuchTableException 错误根因不可定位，落点均在 Catalog API 公共契约层，HIGH，15-20 min）、R53（`DataFrameNaFunctions.replace[T]` 缺 `"*"` 通配 + 数值类型归一化为 `Double`，与上游 `buildReplacement.convertToDouble` 偏离致 `df.na.replace("*", Map(1 -> 2))` 类语句静默无效或在混合 Int/Long replacement map 上抛 schema mismatch，与 R52/R55 同公共 API 与上游契约偏离族系——R52 是参数顺序、R55 是返回类型/前置校验、R53 是 replacement 语义；三者落点正交，可独立修复，MEDIUM，25-35 min）、R54（`StreamingQueryManager.get` 不做 `response.hasQuery` 检查致服务端返回 `RUNNING_NOT_FOUND` 也构造伪造 `StreamingQuery(id="", runId="", name=None)` 暴露给用户而非返回 `null`/`Option.empty`，与 R39 `StreamingQueryListenerBus.registerServerSideListener` `hasNext=false` 落空兜底同 streaming silent fallthrough 族系——两者均在 streaming 路径下静默吞掉服务端"无对象/未找到"信号，落点正交（R39 在 listener 注册、R54 在 query 查询），可独立修复，MEDIUM，10-15 min）、R55（`DataStreamReader.textFile` 返回 `DataFrame` 而非上游约定的 `Dataset[String]` 且缺 `assertNoSpecifiedSchema("textFile")` 前置校验致 typed-pipeline 用户 `.textFile(path).map(_.length)` 编译失败或运行时类型错配——与 R52 同公共 API typed signature 偏离族系——R52 是 Catalog 参数顺序、R55 是 streaming reader 返回类型，两者正交可独立修复，HIGH，15-25 min）、R56（`UDFRegistration` 缺 `registerJava(name, className, returnDataType)` 方法致 Java/PySpark 互操作场景下用户无法注册编译好的 Java UDF 类——上游通过 `JavaUdfBuilder` 直接注册 className 到 server，client-side 用户唯一选择是反射加载并包装 Scala lambda 但该路径绕过 server 端 className 注册无法跨 session 复用，与 R52/R53/R55/R57 同 "公共 API 缺失/偏离致用户无法获得上游等价能力" 族系但落点更窄（仅 UDF 注册路径），可独立追加，MEDIUM，30-45 min）、R57（`StreamingQuery.recentProgress`/`lastProgress`/`exception` 返回 `Seq[String]` / `Option[String]` / `Option[String]` 而非上游 typed `Array[StreamingQueryProgress]` / `StreamingQueryProgress` / `Option[StreamingQueryException]` 致用户必须自己 `JsonUtils.fromJson` 反序列化丢失 IDE 完成与编译期类型检查——与 R55 `DataStreamReader.textFile` 返回 `DataFrame` 同属 "streaming 路径 typed contract 偏离" 族系——R55 是 reader 入口、R57 是 query 状态查询；R55 + R57 可作为 streaming 公共 API typed 对齐的批量修复入口，与 R39 streaming 静默兜底族系正交可独立修复，MEDIUM，60-90 min）、R58（`RuntimeConfig.get(key)` 在 key 不存在且无默认值时不抛 `NoSuchElementException` 静默返回 `""` 与上游 `@throws[NoSuchElementException]` 契约偏离，且 `get` 与 `getOption` 在 "key 不存在" 路径上观察等价致 typed 路径区分破坏——与 R52 / R55 / R56 / R57 同 "public API 与上游契约偏离/缺失" family，落点在 SparkSession `RuntimeConfig` 子层 `@throws` 契约 + typed 路径区分；与 R59 同 round 新增 Catalog 形参顺序反转共同形成 "Round 42 typed contract 偏离" 增量批次，HIGH，15-20 min）、R59（`Catalog.getTable(tableName, dbName)` / `getFunction(functionName, dbName)` 形参顺序与上游 `(dbName, tableName)` / `(dbName, functionName)` 反向致 silent argument-swap——与 R52 同族系直接扩展使 Catalog 公共 API 形参顺序静默反转族系在 5 个 site `Catalog.scala:78,141,151,180,190` 上完整闭环：`listColumns/getTable/getFunction/tableExists/functionExists`；与 R47 同属 Catalog 公共 API 静默契约违反大类，但 R47 是 SQL 语法字符串拼装位置错误、R52/R59 是 Scala API 形参顺序反转——不同失败模式同 Catalog 表面，HIGH，10-15 min）、R61（`StorageLevel.scala:24-34` 公共常量集合仅暴露 10 个（`NONE`/`DISK_ONLY`/`DISK_ONLY_2`/`MEMORY_ONLY`/`MEMORY_ONLY_2`/`MEMORY_ONLY_SER`/`MEMORY_AND_DISK`/`MEMORY_AND_DISK_2`/`MEMORY_AND_DISK_SER`/`OFF_HEAP`），上游 `common/utils/.../storage/StorageLevel.scala:149-161` 暴露 13 个，本端缺失 `DISK_ONLY_3` / `MEMORY_ONLY_SER_2` / `MEMORY_AND_DISK_SER_2` 三常量致用户 `df.persist(StorageLevel.DISK_ONLY_3)` 编译失败——与 R56 `UDFRegistration.registerJava` 缺失方法同属 "公共 API 缺失" 族系，落点不同（R56 在 UDF 注册、R61 在持久化级别枚举），可独立追加，MEDIUM，30 min）、R62（`Observation.scala:43,85` 内部 `awaitTimeout` 默认 `Duration(10, "minutes")` 而上游 `sql/api/.../Observation.scala` 使用 `Duration.Inf`，致用户跑 >10 分钟查询时 `Observation.get` 抛 timeout 异常而非阻塞至完成——与上游契约偏离族系，与 R58 `RuntimeConfig.get` 静默返回 `""` 同 "默认值/兜底偏离致用户行为不可预期" 族系但落点在 Observation 模块 timeout 默认值，MEDIUM，10-15 min）、R63（`Observation.scala:49-53` `metricsRow.schema` 为 `None` 时静默 fallback 到 `Map.empty`，丢失 schema 信息致用户 `Observation.get` 返回空 map 与上游 `metricsRow.toMap` 直接 NPE 行为偏离——与 R30/R43 同 schema-loss 族系（R44 INTERVAL 类型语义偏离虽同方法但根因不同，故不同族系）但落点在 Observation 客户端构造层而非 ArrowDeserializer/LiteralValueProtoConverter；与 R62 同 Observation 模块但根因正交（R62 是 timeout 默认值、R63 是 schema-None 兜底），LOW，5-10 min）、R64（`ArrowSerializer.scala:172,234` 使用 string `"UTF-8"` 编码而非 `StandardCharsets.UTF_8` 常量，与 Java idiom 偏离致 unsafe charset lookup——LOW idiom 修正，与 R61 同 "上游/标准库 idiom 对齐" 族系但落点在 Arrow 编解码 charset 引用，5 min）、R65（`StreamingQuery.scala:63-65` `status` 方法返回 raw proto `StreamingQueryCommandResult.StatusResult` 而非 typed `StreamingQueryStatus` case class——与 R57 `recentProgress`/`lastProgress`/`exception` 返回 JSON `Seq[String]`/`Option[String]`（需用户自行 `JsonUtils.fromJson` 反序列化）同属 "streaming 路径 typed contract 退化致用户必须自行解码丢失编译期类型检查" 族系（R57 落点为 JSON 字符串泄漏、R65 落点为 raw proto 泄漏，泄漏介质不同但 typed-contract 退化同型）——R57 闭合 progress / exception 三入口，R65 直接扩展 status 入口闭合 StreamingQuery 公开 API typed-contract 退化在 4 个 method 上完整闭环，MEDIUM，15-25 min）、R66（`SparkConnectClient.scala:113-137` `parsePlanCompressionOptions` `try / catch case e if NonFatal(e)` 吞掉 `NumberFormatException` 后回退到默认配置——`NumberFormatException` 属 permanent 配置错误但被当作 transient 异常处理，与 R45 / R46 同 "异常分类错误致 silent fallthrough" 族系但落点在 client 配置解析层而非流式响应路径；与 R63 / R64 同 Round 43 LOW 批次但根因正交，LOW，10-15 min）、R69（`DataFrameReader.scala:34` / `DataFrameWriter.scala:49` / `DataStreamReader.scala:31` / `DataStreamWriter.scala:66` / `DataFrameWriterV2.scala:35` 五个 reader/writer 入口缺 `options(java.util.Map[String, String])` Java-interop 重载致用户 `reader.options(javaProps)` 编译失败——与 R56 `UDFRegistration.registerJava` 同 "Java-interop 公共 API 缺失" 族系但落点不同：R56 在 UDF 注册路径、R69 在 reader/writer options 入口共 5 site，可独立追加，MEDIUM，15-20 min）、R70（`GroupedDataFrame.scala:77-93` `mean`/`avg`/`max`/`min`/`sum` 在空 `colNames*` 入口下用 `if cols.isEmpty then df` 兜底静默返回原始 df 而非自动选择全部数值列，与上游 `RelationalGroupedDataset.aggregateNumericColumns` 通过 `selectNumericColumns(colNames)` 在空列名时 fallback 到 schema 内全部数值列偏离——与 R41 `GroupedDataFrame.agg .toMap` 重复 key 静默丢失同 GroupedDataFrame 公共 API 行为偏离族系，落点正交可独立修复，MEDIUM，10-15 min）、R71（`DataFrame.scala:110-149` 五个 join 重载缺 `join(right: Dataset[_], usingColumn: String, joinType: String)` 三参单列重载致用户从上游迁移 `df1.join(df2, "id", "left_outer")` 编译失败必须改写为 `df1.join(df2, Seq("id"), "left_outer")`——与 R56/R61/R69 同 "公共 API 完全缺失" 族系，落点正交可独立追加，MEDIUM，10-15 min）、R76（`Catalog.scala:229-230` 2-arg `createTable(tableName, path)` 路径硬编码 `setSource("parquet")` 而上游 `spark-mine-12/sql/api/.../catalog/Catalog.scala` 同 2-arg 重载不调 `setSource` 让 server 端按 `spark.sql.sources.default` 配置决策——用户 `spark.sql.sources.default=orc` 环境下调 `catalog.createTable("tbl", "/path")` 在 SC3 上得 parquet 表而非 orc 表，与上游默认源策略偏离致 silent contract violation；与 R72/R52/R59 同 Catalog 公共 API 与上游契约偏离族系，落点为 createTable 默认源 fallback 而非形参顺序，MEDIUM，10-15 min）可独立追加，与上面任一选项不互斥。

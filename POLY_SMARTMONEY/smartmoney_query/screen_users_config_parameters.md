# screen_users_config.json 参数说明

本文档解释 `smartmoney_query/screen_users_config.json` 中每个参数的作用。

> 说明：配置中所有以 `_comment_` 开头的字段为说明性注释，运行时不参与逻辑，仅用于说明字段含义。

## 路径与文件名

- `data_dir`: 数据输入目录（`users_summary.csv` 所在目录）。
- `users_dir`: 用户明细目录（每个地址一个子目录，包含 `positions/closed_positions`）。
- `output_dir`: 输出目录（`features/candidates/final` 等文件写入位置）。

- `features_filename`: 全量用户特征表文件名。
- `candidates_filename`: 通过筛选的候选名单文件名。
- `final_filename`: 最终输出表文件名。
- `metadata_filename`: 筛选元信息（参数与统计）文件名。

## 统计口径与基础阈值

- `window_days_default`: 若 summary 缺失时间区间，默认使用的统计窗口天数。
- `min_cost_for_roi`: 计算 ROI 的最小成本阈值（低于该成本不计 ROI）。
- `flat_pnl_epsilon`: 判定盈亏为 flat 的阈值。
- `near_expiry_days`: 距到期多少天内视为“临近到期”。
- `min_action_timing_count`: 使用成交动作时间统计所需的最小数量。
- `require_action_timestamps`: 是否强制使用成交动作时间序列，不足则直接淘汰。

## 胜率平滑（Bayes）

- `bayes_alpha`: Bayes 胜率平滑参数 alpha。
- `bayes_beta`: Bayes 胜率平滑参数 beta。

## 价格分布分段（price_bands）

- `price_bands.tail_high`: 高价尾单阈值（>=）。
- `price_bands.tail_low`: 低价长 shot 阈值（<=）。
- `price_bands.mid_low`: 中间价带下限（含）。
- `price_bands.mid_high`: 中间价带上限（含）。

## 筛选条件（filters）

- `filters.min_closed_count`: 最低平仓样本数。
- `filters.min_account_age_days`: 账号注册至今最小天数。
- `filters.min_lifetime_realized_pnl`: 账号注册至今历史总收益下限（严格大于）。

- `filters.max_trades_per_day`: 日均平仓数上限。
- `filters.max_daily_trades`: 单日平仓数上限。

- `filters.max_p90_cost`: 单笔成本 P90 上限（`null` 为不限制）。
- `filters.max_cost`: 单笔成本最大值上限（`null` 为不限制）。
- `filters.max_open_exposure`: 当前持仓敞口上限（`null` 为不限制）。
- `filters.max_loss`: 最差单笔盈亏下限（越大越严格，`null` 为不限制）。

- `filters.min_bayes_win_rate`: Bayes 胜率下限。
- `filters.min_median_roi`: 中位 ROI 下限。

- `filters.max_tail_high_ratio`: 高价尾单占比上限。
- `filters.max_tail_low_ratio`: 低价长 shot 占比上限。
- `filters.min_mid_ratio`: 中间价带占比下限。

- `filters.max_minute_burst_ratio`: 分钟爆发占比上限。
- `filters.min_interval_median_minutes`: 平仓间隔中位数下限（分钟）。

## 评分权重（score_weights）

- `score_weights.bayes_win_rate`: 评分中 Bayes 胜率权重。
- `score_weights.median_roi`: 评分中中位 ROI 权重。
- `score_weights.profit_factor`: 评分中盈利因子权重。

- `score_weights.mid_ratio`: 评分中中间价带占比权重。
- `score_weights.interval_median_minutes`: 评分中平仓间隔中位数权重。

- `score_weights.minute_burst_ratio`: 评分中分钟爆发占比权重（负向）。
- `score_weights.tail_high_ratio`: 评分中高价尾单占比权重（负向）。
- `score_weights.tail_low_ratio`: 评分中低价长 shot 占比权重（负向）。

- `score_weights.trades_per_day`: 评分中日均平仓数权重（负向）。
- `score_weights.burstiness`: 评分中日内爆发度权重（负向）。
- `score_weights.near_expiry_ratio`: 评分中近到期占比权重（负向）。

- `score_weights.p90_cost`: 评分中成本 P90 权重（0 表示不计）。
- `score_weights.open_exposure`: 评分中持仓敞口权重（0 表示不计）。

## 评分归一化上限（score_clamps）

- `score_clamps.bayes_win_rate`: 评分归一化上限：Bayes 胜率。
- `score_clamps.median_roi`: 评分归一化上限：中位 ROI。
- `score_clamps.profit_factor`: 评分归一化上限：盈利因子。

- `score_clamps.mid_ratio`: 评分归一化上限：中间价带占比。
- `score_clamps.tail_high_ratio`: 评分归一化上限：高价尾单占比。
- `score_clamps.tail_low_ratio`: 评分归一化上限：低价长 shot 占比。

- `score_clamps.minute_burst_ratio`: 评分归一化上限：分钟爆发占比。
- `score_clamps.interval_median_minutes`: 评分归一化上限：平仓间隔中位数（分钟）。

- `score_clamps.trades_per_day`: 评分归一化上限：日均平仓数。
- `score_clamps.burstiness`: 评分归一化上限：日内爆发度。
- `score_clamps.near_expiry_ratio`: 评分归一化上限：近到期占比。

- `score_clamps.p90_cost`: 评分归一化上限：成本 P90。
- `score_clamps.open_exposure`: 评分归一化上限：持仓敞口。

## 标签规则（label_rules）

### `label_rules.price_style`

- `label_rules.price_style.tail_high_ratio_tail`: 高价尾单占比达到该值判为“尾单偏多”。
- `label_rules.price_style.tail_low_ratio_longshot`: 低价长 shot 占比达到该值判为“长shot偏多”。
- `label_rules.price_style.mid_ratio_balanced`: 中间价带占比达到该值判为“均衡”。

### `label_rules.copy_style`

- `label_rules.copy_style.minute_burst_ratio_high`: 分钟爆发占比达到该值判为“结算爆发”。
- `label_rules.copy_style.interval_median_minutes_fast`: 平仓间隔中位数达到该值判为“时效强”。
- `label_rules.copy_style.trades_per_day_high`: 日均平仓数达到该值判为“时效强”。
- `label_rules.copy_style.burstiness_high`: 日内爆发度达到该值判为“时效强”。
- `label_rules.copy_style.near_expiry_ratio_high`: 近到期占比达到该值判为“临近到期”。

## 最终输出（final_output）

- `final_output.sort_by`: 最终表排序字段（使用 rename 前的字段名）。
- `final_output.descending`: 排序方向（`true` 为降序）。

- `final_output.columns`: 最终输出列（按顺序输出）。
- `final_output.rename`: 最终输出列名映射（键为字段名，值为输出显示名）。

### `final_output.rename` 字段说明

- `final_output.rename.user`: 地址列名。
- `final_output.rename.copy_score`: 评分列名。
- `final_output.rename.price_style`: 价格风格列名。
- `final_output.rename.copy_style`: 可复制性列名。
- `final_output.rename.notes`: 备注列名。

- `final_output.rename.bayes_win_rate`: Bayes 胜率列名。
- `final_output.rename.win_rate_no_flat`: 剔除 flat 的胜率列名。
- `final_output.rename.closed_count`: 平仓样本数列名。
- `final_output.rename.median_roi`: 中位 ROI 列名。
- `final_output.rename.profit_factor`: 盈利因子列名。
- `final_output.rename.account_age_days`: 账号年龄列名。
- `final_output.rename.leaderboard_month_pnl`: 月榜 PnL 列名。
- `final_output.rename.suspected_hft`: 疑似高频账号列名。
- `final_output.rename.hft_reason`: 高频原因列名。
- `final_output.rename.trade_actions_records`: 成交记录数列名。
- `final_output.rename.trade_actions_actions`: 成交动作数列名。
- `final_output.rename.lifetime_realized_pnl_sum`: 历史总收益列名。

- `final_output.rename.tail_high_ratio`: 高价尾单占比列名。
- `final_output.rename.tail_low_ratio`: 低价长 shot 占比列名。
- `final_output.rename.mid_ratio`: 中间价带占比列名。
- `final_output.rename.price_median`: 价格中位数列名。

- `final_output.rename.trades_per_day`: 日均平仓数列名。
- `final_output.rename.interval_median_minutes`: 平仓间隔中位列名。
- `final_output.rename.minute_burst_ratio`: 分钟爆发占比列名。
- `final_output.rename.burstiness`: 日爆发度列名。

- `final_output.rename.max_loss`: 最差单笔盈亏列名。
- `final_output.rename.p95_loss`: 亏损单 P95 列名。
- `final_output.rename.near_expiry_ratio`: 近到期占比列名。
- `final_output.rename.profile_url`: Polymarket 网页端个人主页链接列名。
- `final_output.rename.passed_filter`: 是否通过筛选列名。
- `final_output.rename.filter_failures`: 筛选失败原因列名。
- `final_output.rename.filter_warnings`: 筛选警告原因列名。

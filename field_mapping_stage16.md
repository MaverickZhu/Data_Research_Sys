# 第16阶段字段映射说明

## 数据结构变更

### 原始结构（第15阶段之前）
- source_*: 消防监督检查系统
- target_*: 安全隐患排查系统

### 新结构（第16阶段）
- primary_*: 安全隐患排查系统（基准数据）
- matched_*: 消防监督检查系统（匹配数据）

## 字段对应关系

| 显示用途 | 新字段名 | 数据来源 | 说明 |
|---------|---------|---------|------|
| 基准单位名称 | primary_unit_name | 安全隐患排查系统.UNIT_NAME | 始终有值 |
| 匹配单位名称 | matched_unit_name | 消防监督检查系统.dwmc | 仅匹配成功时有值 |
| 基准单位地址 | primary_unit_address | 安全隐患排查系统.UNIT_ADDRESS | - |
| 匹配单位地址 | matched_unit_address | 消防监督检查系统.dwdz | - |
| 基准法定代表人 | primary_legal_person | 安全隐患排查系统.LEGAL_PEOPLE | - |
| 匹配法定代表人 | matched_legal_person | 消防监督检查系统.fddbr | - |
| 基准联系电话 | primary_contact_phone | 安全隐患排查系统.CONTACT_PHONE | - |
| 匹配联系电话 | matched_contact_phone | 消防监督检查系统.lxdh | - |
| 基准信用代码 | primary_credit_code | 安全隐患排查系统.CREDIT_CODE | - |
| 匹配信用代码 | matched_credit_code | 消防监督检查系统.tyshxydm | - |

## Web页面更新

### 表格标题
- 第一列：安全隐患排查系统单位
- 第二列：消防监督检查系统单位

### JavaScript字段访问
```javascript
// 基准单位名称（始终显示）
result.primary_unit_name

// 匹配单位名称（匹配成功时显示，否则显示"未匹配"）
result.matched_unit_name || "未匹配"

// 匹配度显示
result.similarity_score ? (result.similarity_score * 100).toFixed(1) + '%' : '0.0%'
```

## 数据保证

1. **完整性**: 所有安全隐患排查系统的记录都会被处理和存储
2. **一致性**: primary_* 字段始终有值，matched_* 字段仅在匹配成功时有值
3. **可追溯性**: 每条记录都有明确的来源系统标识

## 用户体验改进

1. **明确标识**: 用户能清楚知道哪些是基准数据，哪些是匹配数据
2. **完整覆盖**: 不会遗漏任何安全隐患排查系统的记录
3. **状态清晰**: 匹配状态一目了然（matched/unmatched）

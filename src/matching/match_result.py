"""
匹配结果数据类模块
定义各种匹配结果的数据结构
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class MatchResult:
    """匹配结果数据类（单一匹配，保持向后兼容）"""
    matched: bool
    match_type: str  # 'credit_code', 'unit_name', 'none'
    confidence: float
    source_record: Dict
    target_record: Optional[Dict] = None
    match_details: Optional[Dict] = None


@dataclass
class MultiMatchResult:
    """一对多匹配结果数据类"""
    matched: bool
    source_record: Dict
    matched_records: List[Dict]  # 所有匹配的记录
    match_summary: Dict  # 匹配统计信息
    primary_match: Optional[Dict] = None  # 主要匹配记录（最佳匹配）
    
    @property
    def total_matches(self) -> int:
        """返回匹配记录总数"""
        return len(self.matched_records)
    
    @property
    def has_multiple_matches(self) -> bool:
        """是否有多个匹配记录"""
        return len(self.matched_records) > 1
    
    @property
    def is_exact_match(self) -> bool:
        """是否包含精确匹配"""
        return self.match_summary.get('exact_matches', 0) > 0
    
    @property
    def is_fuzzy_match(self) -> bool:
        """是否包含模糊匹配"""
        return self.match_summary.get('fuzzy_matches', 0) > 0
    
    @property
    def primary_match_type(self) -> str:
        """主要匹配类型"""
        if self.is_exact_match:
            return 'exact'
        elif self.is_fuzzy_match:
            return 'fuzzy'
        else:
            return 'none'
    
    def get_match_records_by_type(self, match_type: str) -> List[Dict]:
        """根据匹配类型获取记录"""
        return [
            record for record in self.matched_records
            if record.get('match_info', {}).get('match_type', '').startswith(match_type)
        ]
    
    def get_inspection_history_summary(self) -> Dict:
        """获取检查历史摘要"""
        if not self.matched_records:
            return {}
        
        inspection_dates = []
        for record in self.matched_records:
            date = record.get('match_info', {}).get('inspection_date')
            if date:
                inspection_dates.append(date)
        
        inspection_dates.sort()
        
        return {
            'total_inspections': len(self.matched_records),
            'earliest_inspection': inspection_dates[0] if inspection_dates else None,
            'latest_inspection': inspection_dates[-1] if inspection_dates else None,
            'inspection_span_days': None,  # 可以后续计算
            'inspection_frequency': len(inspection_dates) / max(1, len(set(inspection_dates)))
        } 
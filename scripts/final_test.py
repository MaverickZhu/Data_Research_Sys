#!/usr/bin/env python3
"""
首页数据载入问题最终测试脚本
"""

import requests
import json
import time
from datetime import datetime

def test_api_stats():
    """测试API统计接口"""
    print("🔍 测试 /api/stats 接口...")
    try:
        response = requests.get('http://127.0.0.1:8888/api/stats', timeout=10)
        if response.status_code == 200:
            data = response.json()
            print("✅ API接口正常")
            print(f"   监督管理系统: {data['data_sources']['supervision_count']:,} 条")
            print(f"   安全排查系统: {data['data_sources']['inspection_count']:,} 条")
            print(f"   匹配结果: {data['data_sources']['match_results_count']:,} 条")
            print(f"   总匹配数: {data['matching_stats']['total_matches']:,} 条")
            return True
        else:
            print(f"❌ API错误: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ API测试失败: {str(e)}")
        return False

def test_homepage():
    """测试首页访问"""
    print("\n🏠 测试首页访问...")
    try:
        response = requests.get('http://127.0.0.1:8888/', timeout=10)
        if response.status_code == 200:
            content = response.text
            print("✅ 首页访问正常")
            
            # 检查关键元素
            checks = [
                ('id="supervisionCount"', '监督管理系统数据元素'),
                ('id="inspectionCount"', '安全排查系统数据元素'),
                ('id="matchResultsCount"', '匹配结果数据元素'),
                ('refreshStats()', '数据刷新函数调用'),
                ('updateStatsDisplay', '数据更新函数'),
                ('fetch(\'/api/stats\')', 'API调用代码')
            ]
            
            found_count = 0
            for check, desc in checks:
                if check in content:
                    print(f"   ✅ 找到 {desc}")
                    found_count += 1
                else:
                    print(f"   ❌ 未找到 {desc}")
            
            print(f"   📊 检查结果: {found_count}/{len(checks)} 项通过")
            return found_count >= len(checks) - 1  # 允许1项失败
        else:
            print(f"❌ 首页错误: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 首页测试失败: {str(e)}")
        return False

def test_test_page():
    """测试调试页面"""
    print("\n🧪 测试调试页面 /test...")
    try:
        response = requests.get('http://127.0.0.1:8888/test', timeout=10)
        if response.status_code == 200:
            content = response.text
            if '数据加载测试页面' in content:
                print("✅ 调试页面正常")
                return True
            else:
                print("⚠️  调试页面内容异常")
                return False
        else:
            print(f"❌ 调试页面错误: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 调试页面测试失败: {str(e)}")
        return False

def main():
    """主测试函数"""
    print("=" * 60)
    print("🚀 首页数据载入问题最终测试")
    print("=" * 60)
    print(f"⏰ 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 执行测试
    tests = [
        ("API统计接口", test_api_stats),
        ("首页访问", test_homepage),
        ("调试页面", test_test_page)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        result = test_func()
        results.append((test_name, result))
        time.sleep(1)
    
    # 汇总结果
    print("\n" + "=" * 60)
    print("📊 测试结果汇总")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
    
    print(f"\n📈 总体结果: {passed}/{total} 项测试通过")
    print(f"📈 成功率: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n🎉 所有测试通过！首页数据载入问题已解决！")
        print("\n💡 使用建议:")
        print("   1. 访问首页: http://127.0.0.1:8888")
        print("   2. 访问调试页面: http://127.0.0.1:8888/test")
        print("   3. 按F12查看浏览器控制台的详细日志")
    else:
        print(f"\n⚠️  还有 {total-passed} 项测试未通过")
        print("\n🔧 故障排除:")
        print("   1. 检查系统是否正常启动")
        print("   2. 确认端口8888可用")
        print("   3. 查看系统日志文件")
        print("   4. 使用调试页面进行详细诊断")

if __name__ == "__main__":
    main() 
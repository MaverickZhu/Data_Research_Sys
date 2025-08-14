#!/usr/bin/env python3
"""
监控知识图谱构建进度
"""

import requests
import time
import json
from datetime import datetime

def monitor_kg_build():
    """监控知识图谱构建进度"""
    base_url = "http://127.0.0.1:18888"
    
    print("🔍 知识图谱构建监控")
    print("=" * 60)
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    start_time = time.time()
    check_count = 0
    
    while True:
        check_count += 1
        current_time = datetime.now().strftime('%H:%M:%S')
        elapsed = int(time.time() - start_time)
        
        print(f"\n[{current_time}] 第 {check_count} 次检查 (已运行 {elapsed//60}分{elapsed%60}秒)")
        
        try:
            # 检查构建状态API
            response = requests.get(f"{base_url}/api/kg/status", timeout=5)
            if response.status_code == 200:
                status_data = response.json()
                print(f"📊 构建状态: {status_data.get('status', '未知')}")
                if 'progress' in status_data:
                    print(f"📈 进度: {status_data['progress']}%")
                if 'message' in status_data:
                    print(f"💬 消息: {status_data['message']}")
            else:
                print(f"❌ 状态API响应异常: {response.status_code}")
        
        except requests.exceptions.Timeout:
            print("⏰ 状态API超时 - 可能正在进行大量计算")
        except requests.exceptions.ConnectionError:
            print("🔌 连接错误 - 服务器可能重启或崩溃")
            break
        except Exception as e:
            print(f"💥 检查状态时出错: {e}")
        
        # 检查简化实体API（轻量级检查）
        try:
            response = requests.get(f"{base_url}/api/kg/entities_simple", timeout=3)
            if response.status_code == 200:
                print("✅ 服务器响应正常")
            else:
                print(f"⚠️  服务器响应异常: {response.status_code}")
        except:
            print("❌ 服务器无响应")
        
        # 每30秒检查一次
        print("⏳ 等待30秒后继续检查...")
        time.sleep(30)
        
        # 如果运行超过30分钟，询问是否继续
        if elapsed > 1800:  # 30分钟
            print(f"\n⚠️  已运行超过30分钟，可能需要优化或重启")
            break

def check_kg_data():
    """检查已构建的知识图谱数据"""
    base_url = "http://127.0.0.1:18888"
    
    print("\n🔍 检查已构建的知识图谱数据")
    print("-" * 40)
    
    try:
        # 检查实体数据
        response = requests.get(f"{base_url}/api/kg/entities_simple", timeout=10)
        if response.status_code == 200:
            entities_data = response.json()
            if 'total_entities' in entities_data:
                print(f"📊 实体总数: {entities_data['total_entities']}")
        
        # 检查关系数据
        response = requests.get(f"{base_url}/api/kg/relations_simple", timeout=10)
        if response.status_code == 200:
            relations_data = response.json()
            if 'total_triples' in relations_data:
                print(f"🔗 关系总数: {relations_data['total_triples']}")
        
        print("✅ 数据检查完成")
    
    except Exception as e:
        print(f"❌ 数据检查失败: {e}")

def main():
    """主函数"""
    print("🚀 知识图谱构建监控工具")
    print("=" * 60)
    
    # 首先检查当前数据状态
    check_kg_data()
    
    # 开始监控
    try:
        monitor_kg_build()
    except KeyboardInterrupt:
        print("\n\n⏹️  监控已停止")
    
    print("\n📊 最终状态检查")
    check_kg_data()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
实时监控Flask请求的工具
"""
import requests
import time
import json

def test_current_api():
    print("=== 实时API测试 ===")
    
    # 创建测试文件
    test_content = """公司名称,统一社会信用代码,法定代表人
北京科技有限公司,91110108MA001A2B3C,张三
上海智能制造有限公司,91310115MA004D5E6F,李四"""
    
    with open('test_realtime.csv', 'w', encoding='utf-8') as f:
        f.write(test_content)
    
    try:
        print(f"1. 当前时间: {time.strftime('%H:%M:%S')}")
        
        # 发送请求到API
        url = f"http://localhost:5000/api/upload_csv?_t={int(time.time() * 1000)}"
        
        with open('test_realtime.csv', 'rb') as f:
            files = {'file': ('test_realtime.csv', f, 'text/csv')}
            
            print(f"2. 发送请求到: {url}")
            response = requests.post(url, files=files, timeout=10)
            
            print(f"3. 状态码: {response.status_code}")
            print(f"4. 响应头: {dict(response.headers)}")
            
            if response.status_code == 400:
                print("5. 400错误详情:")
                try:
                    error_data = response.json()
                    print(f"   JSON错误: {json.dumps(error_data, ensure_ascii=False, indent=2)}")
                except:
                    print(f"   文本错误: {response.text}")
            elif response.status_code == 200:
                print("5. 成功响应:")
                result = response.json()
                print(f"   成功: {result.get('success')}")
                print(f"   消息: {result.get('message')}")
            else:
                print(f"5. 其他错误: {response.text[:200]}")
                
    except Exception as e:
        print(f"请求异常: {str(e)}")
    finally:
        # 清理
        import os
        if os.path.exists('test_realtime.csv'):
            os.unlink('test_realtime.csv')

if __name__ == "__main__":
    test_current_api()

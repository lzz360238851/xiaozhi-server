#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
电台播放功能测试脚本
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from plugins_func.register import all_function_registry
from config.logger import setup_logging

logger = setup_logging()

def test_radio_function():
    """测试电台播放功能是否已注册"""
    print("=== 电台播放功能测试 ===")
    
    # 检查play_radio函数是否已注册
    if "play_radio" in all_function_registry:
        print("✅ play_radio函数已成功注册")
        func_item = all_function_registry["play_radio"]
        print(f"   函数名称: {func_item.name}")
        print(f"   函数类型: {func_item.type}")
        print(f"   函数描述: {func_item.description}")
    else:
        print("❌ play_radio函数未注册")
        print("   已注册的函数列表:")
        for func_name in all_function_registry.keys():
            print(f"   - {func_name}")
    
    # 检查play_music函数是否已注册（作为对比）
    if "play_music" in all_function_registry:
        print("✅ play_music函数已成功注册")
    else:
        print("❌ play_music函数未注册")
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    test_radio_function() 
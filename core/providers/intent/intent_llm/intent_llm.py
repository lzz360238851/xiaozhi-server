from typing import List, Dict
from ..base import IntentProviderBase
from plugins_func.functions.play_music import initialize_music_handler
from config.logger import setup_logging
import re
import json
import hashlib
import time

TAG = __name__
logger = setup_logging()


class IntentProvider(IntentProviderBase):
    def __init__(self, config):
        super().__init__(config)
        self.llm = None
        self.promot = ""
        # 添加缓存管理
        self.intent_cache = {}  # 缓存意图识别结果
        self.cache_expiry = 600  # 缓存有效期10分钟
        self.cache_max_size = 100  # 最多缓存100个意图
        self.history_count = 4  # 默认使用最近4条对话记录

    def get_intent_system_prompt(self, functions_list: str) -> str:
        """
        根据配置的意图选项和可用函数动态生成系统提示词
        Args:
            functions: 可用的函数列表，JSON格式字符串
        Returns:
            格式化后的系统提示词
        """

        # 构建函数说明部分
        functions_desc = "可用的函数列表：\n"
        for func in functions_list:
            func_info = func.get("function", {})
            name = func_info.get("name", "")
            desc = func_info.get("description", "")
            params = func_info.get("parameters", {})

            functions_desc += f"\n函数名: {name}\n"
            functions_desc += f"描述: {desc}\n"

            if params:
                functions_desc += "参数:\n"
                for param_name, param_info in params.get("properties", {}).items():
                    param_desc = param_info.get("description", "")
                    param_type = param_info.get("type", "")
                    functions_desc += f"- {param_name} ({param_type}): {param_desc}\n"

            functions_desc += "---\n"

        prompt = (
            "你是一个意图识别助手。请分析用户的最后一句话，判断用户意图并调用相应的函数。\n\n"
            "- 如果用户使用疑问词（如'怎么'、'为什么'、'如何'）询问退出相关的问题（例如'怎么退出了？'），注意这不是让你退出，请返回 {'function_call': {'name': 'continue_chat'}\n"
            "- 仅当用户明确使用'退出系统'、'结束对话'、'我不想和你说话了'等指令时，才触发 handle_exit_intent\n\n"
            f"{functions_desc}\n"
            "处理步骤:\n"
            "1. 分析用户输入，确定用户意图\n"
            "2. 从可用函数列表中选择最匹配的函数\n"
            "3. 如果找到匹配的函数，生成对应的function_call 格式\n"
            '4. 如果没有找到匹配的函数，返回{"function_call": {"name": "continue_chat"}}\n\n'
            "返回格式要求：\n"
            "1. 必须返回纯JSON格式\n"
            "2. 必须包含function_call字段\n"
            "3. function_call必须包含name字段\n"
            "4. 如果函数需要参数，必须包含arguments字段\n\n"
            "示例：\n"
            "```\n"
            "用户: 现在几点了？\n"
            '返回: {"function_call": {"name": "get_time"}}\n'
            "```\n"
            "```\n"
            "用户: 播放素颜\n"
            '返回: {"function_call": {"name": "play_music"}}\n'
            "```\n"
            "```\n"
            "用户: 当前电池电量是多少？\n"
            '返回: {"function_call": {"name": "get_battery_level", "arguments": {"response_success": "当前电池电量为{value}%", "response_failure": "无法获取Battery的当前电量百分比"}}}\n'
            "```\n"
            "```\n"
            "用户: 当前屏幕亮度是多少？\n"
            '返回: {"function_call": {"name": "self_screen_get_brightness"}}\n'
            "```\n"
            "```\n"
            "用户: 设置屏幕亮度为50%\n"
            '返回: {"function_call": {"name": "self_screen_set_brightness", "arguments": {"brightness": 50}}}\n'
            "```\n"
            "```\n"
            "用户: 我想结束对话\n"
            '返回: {"function_call": {"name": "handle_exit_intent", "arguments": {"say_goodbye": "goodbye"}}}\n'
            "```\n"
            "```\n"
            "用户: 你好啊\n"
            '返回: {"function_call": {"name": "continue_chat"}}\n'
            "```\n\n"
            "注意：\n"
            "1. 只返回JSON格式，不要包含任何其他文字\n"
            '2. 如果没有找到匹配的函数，返回{"function_call": {"name": "continue_chat"}}\n'
            "3. 确保返回的JSON格式正确，包含所有必要的字段\n"
            "特殊说明：\n"
            "- 当用户单次输入包含多个指令时（如'打开灯并且调高音量'）\n"
            "- 请返回多个function_call组成的JSON数组\n"
            "- 示例：{'function_calls': [{name:'light_on'}, {name:'volume_up'}]}"
        )
        return prompt

    def _parse_intent_response(self, raw: str) -> dict:
        """健壮解析：
        - 支持多段JSON对象和杂糅的文本，例如：
          {"song_name": "盗将行"}play_music  \n  {"song_name": "悬溺"}play_music  \n  {"song_name": "姑娘我怎能忘"}
        - 优先规则：
          1) 如果存在标准的 {"function_call": {...}}，选择最后一个
          2) 否则从多个裸参数JSON + 尾随函数名模式中提取，选择最后一条
          3) 若仍失败，返回 {"function_call": {"name": "continue_chat"}}
        """
        # 预处理：去除 Markdown 代码块包裹，例如 ```json ... ``` 或 ``` ... ```，仅保留内部内容
        try:
            raw = re.sub(r"```(?:json)?\s*([\s\S]*?)```", lambda m: m.group(1).strip(), raw, flags=re.IGNORECASE)
        except Exception:
            pass

        # 先尝试直接解析完整JSON
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and "function_calls" in data and isinstance(data["function_calls"], list) and len(data["function_calls"]) > 0:
                return {"function_call": data["function_calls"][-1]}
            return data
        except Exception:
            pass

        # 新增：使用括号计数提取多段 JSON（忽略字符串内的括号）
        def extract_json_objects(text: str):
            blocks = []
            depth = 0
            start = None
            in_str = False
            escape = False
            for i, ch in enumerate(text):
                if in_str:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_str = False
                    # 字符串内的所有字符（包括花括号）都忽略
                    continue
                else:
                    if ch == '"':
                        in_str = True
                    elif ch == '{':
                        if depth == 0:
                            start = i
                        depth += 1
                    elif ch == '}':
                        if depth > 0:
                            depth -= 1
                            if depth == 0 and start is not None:
                                blocks.append((start, i + 1))
                                start = None
            return blocks

        json_blocks_pos = extract_json_objects(raw)
        fc_candidates = []
        pair_candidates = []
        song_candidates = []

        for (s, e) in json_blocks_pos:
            block = raw[s:e]
            try:
                obj = json.loads(block)
            except Exception:
                continue

            # 标准 function_call 结构
            if isinstance(obj, dict) and "function_call" in obj and isinstance(obj["function_call"], dict):
                fc_candidates.append(obj["function_call"])  # 只存内部，便于统一返回
                continue

            # function_calls 数组
            if isinstance(obj, dict) and "function_calls" in obj and isinstance(obj["function_calls"], list) and obj["function_calls"]:
                fc_candidates.append(obj["function_calls"][-1])
                continue

            # 裸参数 JSON，尝试判断是否歌曲参数
            if isinstance(obj, dict) and ("song_name" in obj or "music" in obj or "title" in obj):
                song_candidates.append(obj)
                # 继续看尾随函数名
            
            # 提取尾随的函数名: 紧跟在该 JSON 之后的标识符
            tail = raw[e:]
            m = re.match(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)", tail)
            if m:
                fname = m.group(1)
                if fname not in ["json", "text", "string", "data", "result", "output", "response"]:
                    if isinstance(obj, dict):
                        pair_candidates.append({"name": fname, "arguments": obj})

        # 选择策略：优先最后一个标准 function_call
        if fc_candidates:
            return {"function_call": fc_candidates[-1]}

        # === 新增：支持 function_name{json} 模式 ===
        # 例如：get_news_from_chinanews {"command": "play", "radio_name": "中国之声"}
        func_match = re.match(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*", raw)
        if func_match:
            func_name = func_match.group(1)
            if func_name not in ["json", "text", "string", "data", "result", "output", "response"]:
                after_func = raw[func_match.end():].lstrip()
                json_blocks_pos_after = extract_json_objects(after_func)
                if json_blocks_pos_after:
                    # 选择最后一个 JSON 块，遵循“就近、后者覆盖前者”的策略
                    last_json_pos = json_blocks_pos_after[-1]
                    json_str = after_func[last_json_pos[0]:last_json_pos[1]]
                    try:
                        args = json.loads(json_str)
                        if isinstance(args, dict):
                            return {"function_call": {"name": func_name, "arguments": args}}
                    except Exception:
                        pass

        # 其次：若存在 {json}func 对，始终选择最后一个（不做任何歌曲覆盖）
        if pair_candidates:
            last_pair = pair_candidates[-1]
            return {"function_call": last_pair}

        # 再次：通用兜底策略——取最后一个 JSON，结合它后面最近的函数名
        if json_blocks_pos:
            last_s, last_e = json_blocks_pos[-1]
            last_obj = None
            try:
                last_obj = json.loads(raw[last_s:last_e])
            except Exception:
                last_obj = None
            # 向右寻找最近的函数名
            tail = raw[last_e:]
            m = re.match(r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)", tail)
            if m:
                fname = m.group(1)
                if fname not in ["json", "text", "string", "data", "result", "output", "response"]:
                    if isinstance(last_obj, dict):
                        return {"function_call": {"name": fname, "arguments": last_obj}}
                    else:
                        return {"function_call": {"name": fname}}
            # 如果没有函数名且是歌曲参数，则默认 play_music
            if isinstance(last_obj, dict) and ("song_name" in last_obj or "music" in last_obj or "title" in last_obj):
                return {"function_call": {"name": "play_music", "arguments": last_obj}}

        # 兼容：旧的正则匹配（提取所有标准的 function_call 片段）
        fc_matches = re.findall(r"\{\s*\"function_call\"\s*:\s*\{.*?\}\s*\}", raw, re.DOTALL)
        for m in reversed(fc_matches):
            try:
                return json.loads(m)
            except Exception:
                continue

        # 兼容：形如 {json}func 的模式（正则兜底）
        pair_iter = list(re.finditer(r"(\{\s*.*?\s*\})(?:\s*\n?\s*)([a-zA-Z_][a-zA-Z0-9_]*)", raw, re.DOTALL))
        if pair_iter:
            last_pair = pair_iter[-1]
            last_pair_json = last_pair.group(1)
            last_pair_func = last_pair.group(2)
            try:
                json_iter_all = list(re.finditer(r"\{\s*.*?\s*\}", raw, re.DOTALL))
                if json_iter_all:
                    last_block_match = json_iter_all[-1]
                    if last_block_match.start() >= last_pair.end():
                        try:
                            trailing_args = json.loads(last_block_match.group(0))
                            if isinstance(trailing_args, dict) and ("song_name" in trailing_args or "music" in trailing_args or "title" in trailing_args):
                                return {"function_call": {"name": "play_music", "arguments": trailing_args}}
                        except Exception:
                            pass
                args = json.loads(last_pair_json)
                if isinstance(args, dict):
                    return {"function_call": {"name": last_pair_func, "arguments": args}}
            except Exception:
                pass

        # 兜底：尝试提取最后一个函数名
        func_tail = re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\s*$", raw)
        if func_tail:
            func_name = func_tail[-1]
            if func_name not in ["json", "text", "string", "data", "result", "output", "response"]:
                return {"function_call": {"name": func_name}}

        return {"function_call": {"name": "continue_chat"}}

    def clean_cache(self):
        """清理过期缓存"""
        now = time.time()
        # 找出过期键
        expired_keys = [
            k
            for k, v in self.intent_cache.items()
            if now - v["timestamp"] > self.cache_expiry
        ]
        for key in expired_keys:
            del self.intent_cache[key]

        # 如果缓存太大，移除最旧的条目
        if len(self.intent_cache) > self.cache_max_size:
            # 按时间戳排序并保留最新的条目
            sorted_items = sorted(
                self.intent_cache.items(), key=lambda x: x[1]["timestamp"]
            )
            for key, _ in sorted_items[: len(sorted_items) - self.cache_max_size]:
                del self.intent_cache[key]

    def replyResult(self, text: str, original_text: str):
        llm_result = self.llm.response_no_stream(
            system_prompt=text,
            user_prompt="请根据以上内容，像人类一样说话的口吻回复用户，要求简洁，请直接返回结果。用户现在说："
            + original_text,
        )
        return llm_result

    async def detect_intent(self, conn, dialogue_history: List[Dict], text: str) -> str:
        if not self.llm:
            raise ValueError("LLM provider not set")
        if conn.func_handler is None:
            return '{"function_call": {"name": "continue_chat"}}'

        # 记录整体开始时间
        total_start_time = time.time()

        # 打印使用的模型信息
        model_info = getattr(self.llm, "model_name", str(self.llm.__class__.__name__))
        logger.bind(tag=TAG).debug(f"使用意图识别模型: {model_info}")

        # 计算缓存键
        cache_key = hashlib.md5(text.encode()).hexdigest()

        # 检查缓存
        if cache_key in self.intent_cache:
            cache_entry = self.intent_cache[cache_key]
            # 检查缓存是否过期
            if time.time() - cache_entry["timestamp"] <= self.cache_expiry:
                cache_time = time.time() - total_start_time
                logger.bind(tag=TAG).debug(
                    f"使用缓存的意图: {cache_key} -> {cache_entry['intent']}, 耗时: {cache_time:.4f}秒"
                )
                return cache_entry["intent"]

        # 清理缓存
        self.clean_cache()

        if self.promot == "":
            functions = conn.func_handler.get_functions()
            if hasattr(conn, "mcp_client"):
                mcp_tools = conn.mcp_client.get_available_tools()
                if mcp_tools is not None and len(mcp_tools) > 0:
                    if functions is None:
                        functions = []
                    functions.extend(mcp_tools)

            self.promot = self.get_intent_system_prompt(functions)

        music_config = initialize_music_handler(conn)
        music_file_names = music_config["music_file_names"]
        prompt_music = f"{self.promot}\n<musicNames>{music_file_names}\n</musicNames>"

        home_assistant_cfg = conn.config["plugins"].get("home_assistant")
        if home_assistant_cfg:
            devices = home_assistant_cfg.get("devices", [])
        else:
            devices = []
        if len(devices) > 0:
            hass_prompt = "\n下面是我家智能设备列表（位置，设备名，entity_id），可以通过homeassistant控制\n"
            for device in devices:
                hass_prompt += device + "\n"
            prompt_music += hass_prompt

        logger.bind(tag=TAG).debug(f"User prompt: {prompt_music}")

        # 构建用户对话历史的提示
        msgStr = ""

        # 获取最近的对话历史
        start_idx = max(0, len(dialogue_history) - self.history_count)
        for i in range(start_idx, len(dialogue_history)):
            msgStr += f"{dialogue_history[i].role}: {dialogue_history[i].content}\n"

        msgStr += f"User: {text}\n"
        user_prompt = f"current dialogue:\n{msgStr}"

        # 记录预处理完成时间
        preprocess_time = time.time() - total_start_time
        logger.bind(tag=TAG).debug(f"意图识别预处理耗时: {preprocess_time:.4f}秒")

        # 使用LLM进行意图识别
        llm_start_time = time.time()
        logger.bind(tag=TAG).debug(f"开始LLM意图识别调用, 模型: {model_info}")

        intent = self.llm.response_no_stream(
            system_prompt=prompt_music, user_prompt=user_prompt
        )

        # 记录LLM调用完成时间
        llm_time = time.time() - llm_start_time
        logger.bind(tag=TAG).debug(
            f"LLM意图识别完成, 模型: {model_info}, 调用耗时: {llm_time:.4f}秒"
        )

        # 记录后处理开始时间
        postprocess_start_time = time.time()

        # 清理和解析响应
        intent = intent.strip()
        
        # 记录总处理时间
        total_time = time.time() - total_start_time
        logger.bind(tag=TAG).debug(
            f"【意图识别性能】模型: {model_info}, 总耗时: {total_time:.4f}秒, LLM调用: {llm_time:.4f}秒, 查询: '{text[:20]}...'"
        )

        # 处理格式化问题和提取有效的意图
        intent_data = self._parse_intent_response(intent)
        
        # 处理解析后的意图数据
        if "function_call" in intent_data:
            function_data = intent_data["function_call"]
            function_name = function_data.get("name")
            function_args = function_data.get("arguments", {})

            # 记录识别到的function call
            logger.bind(tag=TAG).info(
                f"llm 识别到意图: {function_name}, 参数: {function_args}"
            )

            # 如果是继续聊天，清理工具调用相关的历史消息
            if function_name == "continue_chat":
                # 保留非工具相关的消息
                clean_history = [
                    msg
                    for msg in conn.dialogue.dialogue
                    if msg.role not in ["tool", "function"]
                ]
                conn.dialogue.dialogue = clean_history

            # 将解析后的数据序列化为JSON字符串
            intent_json = json.dumps(intent_data, ensure_ascii=False)

            # 添加到缓存
            self.intent_cache[cache_key] = {
                "intent": intent_json,
                "timestamp": time.time(),
            }

            # 后处理时间
            postprocess_time = time.time() - postprocess_start_time
            logger.bind(tag=TAG).debug(f"意图后处理耗时: {postprocess_time:.4f}秒")

            # 返回完全序列化的JSON字符串
            return intent_json
        else:
            # 即使没有function_call字段，也返回标准JSON字符串（用于上层处理或兜底）
            intent_json = json.dumps(intent_data, ensure_ascii=False)

            # 添加到缓存
            self.intent_cache[cache_key] = {
                "intent": intent_json,
                "timestamp": time.time(),
            }

            # 后处理时间
            postprocess_time = time.time() - postprocess_start_time
            logger.bind(tag=TAG).debug(f"意图后处理耗时: {postprocess_time:.4f}秒")

            # 返回普通意图（标准JSON字符串）
            return intent_json

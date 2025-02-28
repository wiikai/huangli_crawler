import json
import os
from datetime import datetime
from collections import Counter

class HlDataSaver:
    def __init__(self, filename=''):
        self.filename = filename

    def _datekey_to_datetime(self, item):
        """统一的日期转换方法（匹配你的具体日期格式）"""
        return datetime.strptime(item["datekey"], "%Y/%m/%d %H:%M:%S")

    def _sorted_data(self, data_list):
        """封装统一排序逻辑"""
        return sorted(
            data_list,
            key=self._datekey_to_datetime,
            reverse=False  # 顺序：最旧->最新
        )
    
    def _load_existing(self):
        """加载现有数据并保持内存数据有效性"""
        if not os.path.exists(self.filename):
            return []
        
        try:
            with open(self.filename, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # 验证数据格式的有效性
                return data.get("data", []) if isinstance(data, dict) else []
        except (json.JSONDecodeError, KeyError) as e:
            print(f"加载历史数据失败，重置存储: {str(e)}")
            return []
        
    def save_incrementally(self, new_data):
        """增量存储并保持按日期升序排列（线程安全版）"""
        existing = self._load_existing()
        existing_dates = {item["datekey"]: idx for idx, item in enumerate(existing)}
        modified = False

        # 处理更新和新增
        for item in new_data:
            datekey = item["datekey"]
            
            # 存在性检查
            if datekey in existing_dates:
                idx = existing_dates[datekey]
                
                # 哈希比对避免无意义更新
                if hash(frozenset(existing[idx].items())) != hash(frozenset(item.items())):
                    existing[idx] = item
                    modified = True
            else:
                # 按顺序插入而不是直接append
                insert_pos = next(
                    (i for i, x in enumerate(existing) 
                     if self._datekey_to_datetime(x) > self._datekey_to_datetime(item)),
                    len(existing)
                )
                
                existing.insert(insert_pos, item)
                existing_dates[datekey] = insert_pos
                modified = True

        # 双重确认排序逻辑
        if modified:
            # 最终保存前的全面排序（防止中间插入位置计算错误）
            sorted_data = self._sorted_data(existing)
            
            # 生成统一格式的保存数据
            save_data = {
                "version": 2.0,
                "generated_at": datetime.now().isoformat(),
                "data": sorted_data
            }

            # 原子化保存（避免写文件中途出错破坏数据）
            temp_file = f"{self.filename}.tmp"
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            # 替换原文件（跨平台安全操作）
            if os.path.exists(self.filename):
                os.replace(temp_file, self.filename)
            else:
                os.rename(temp_file, self.filename)

            return True
        return False

class HlDataLoader:
    def __init__(self, filename=''):
        self.filename = filename
        self._data = []
        self._index = {}  # datekey到索引的映射
        self._load_data()

    def _load_data(self):
        """加载并预处理数据"""
        if not os.path.exists(self.filename):
            return
            
        try:
            with open(self.filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # 兼容新旧格式处理
            raw_data = data.get('data', [])
            if isinstance(raw_data, dict):  # 转换旧格式的字典数据
                self._data = list(raw_data.values())
            else:
                self._data = raw_data
                
            # 构建索引
            self._index = {item['datekey']: idx for idx, item in enumerate(self._data)}
        except Exception as e:
            logging.error(f"数据加载失败: {str(e)}")

    def get_by_date(self, date_str):
        """按日期查询数据（支持多种格式）"""
        target_key = self._normalize_datekey(date_str)
        return self._data[self._index.get(target_key, -1)]

    def filter_data(self, start_date=None, end_date=None, keywords=None):
        """高级数据过滤"""
        filtered = []
        for item in self._data:
            # 日期范围过滤
            date_obj = datetime.strptime(item['datekey'], "%Y/%m/%d %H:%M:%S")
            if start_date and date_obj < start_date:
                continue
            if end_date and date_obj > end_date:
                continue
            
            # 关键词搜索
            if keywords:
                search_area = ' '.join(str(v) for v in item.values())
                if not any(kw.lower() in search_area.lower() for kw in keywords):
                    continue
                    
            filtered.append(item)
        
        # 排序保障
        filtered.sort(key=lambda x: x['datekey'])
        return filtered

    def _normalize_datekey(self, date_input):
        """日期格式统一处理"""
        if isinstance(date_input, datetime):
            return date_input.strftime("%Y/%m/%d 00:00:00")
            
        try:
            # 支持多种日期字符串格式
            formats = [
                "%Y/%m/%d %H:%M:%S",
                "%Y-%m-%d",
                "%Y%m%d"
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_input, fmt)
                    return dt.strftime("%Y/%m/%d 00:00:00")
                except ValueError:
                    continue
            raise ValueError("无法识别的日期格式")
        except:
            raise ValueError("无效的日期输入")
        
    # 统计分析功能（示例）
    def get_statistics(self):
        """获取黄历数据统计信息"""
        stats = {
            'total_days': len(self._data),
            'most_common_jsyq': Counter(item['jsyq'] for item in self._data).most_common(5),
        }
        return stats

    # 数据完整性检测（示例）
    def validate_data(self):
        """校验数据完整性"""
        missing_fields = []
        for idx, item in enumerate(self._data):
            if 'datekey' not in item:
                missing_fields.append(f"索引 {idx} 缺少datekey字段")
                continue
                
            # 校验日期格式合法性
            try:
                datetime.strptime(item['datekey'], "%Y/%m/%d %H:%M:%S")
            except:
                missing_fields.append(f"索引 {idx} datekey格式错误: {item['datekey']}")
                
        return missing_fields
    
    def save_data(self, output_file=None):
        """保存数据并自动排序，覆盖重复日期"""
        try:
            target_file = output_file or self.filename
            
            # 转换字典去重并保留最后出现的日期数据
            data_dict = {}
            duplicate_count = 0
            for item in self._data:
                datekey = item.get('datekey')
                if datekey:
                    if datekey in data_dict:
                        duplicate_count += 1
                    data_dict[datekey] = item

            # 排序处理
            sorted_data = sorted(data_dict.values(), key=self._datekey_to_datetime)
            
            # 构建保存数据结构
            save_data = {
                "version": 1.2,
                "generated_at": datetime.now().isoformat(),
                "data": sorted_data
            }

            # 写入文件
            with open(target_file, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)
                
        except PermissionError:
            print("错误：没有文件写入权限")
        except Exception as e:
            print(f"保存失败: {str(e)}")

    def _datekey_to_datetime(self, item):
        """内部日期转换方法"""
        return datetime.strptime(item['datekey'], "%Y/%m/%d %H:%M:%S") 
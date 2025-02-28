import re
import os
import json
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import concurrent.futures
from bs4 import BeautifulSoup
import requests
import threading
from collections import Counter

# ====================== 配置部分 ======================
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')

# ====================== 网络连接配置 ======================
def create_session(retries=3):
    """创建带重试机制的会话对象"""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504)
    )
    adapter = HTTPAdapter(max_retries=retry,pool_connections=50,pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# ====================== 工具函数 ======================
def format_url_date(date_obj):
    """生成无前导零的日期字符串（跨平台兼容）"""
    return f"{date_obj.year}-{date_obj.month}-{date_obj.day}"

def parse_date_from_url(url):
    """从URL解析日期"""
    path = urlparse(url).path
    date_part = path.split('/')[-1].replace('.html', '')
    return datetime.strptime(date_part, "%Y-%m-%d").strftime("%Y/%m/%d 00:00:00")

# ====================== 数据抓取函数 ======================
def extract_suitable_items(soup):
    """提取宜/忌信息"""
    result = {"yi": "", "ji": ""}
    for suitable_div in soup.find_all('div', class_='suitable'):
        span = suitable_div.find('span')
        if not span: continue
        category = span.text.strip()
        if category not in ["宜", "忌"]: continue
        content_div = suitable_div.find_next_sibling('div', class_='suitable_con')
        if not content_div: continue
        items = [li.span.text.strip() for li in content_div.find_all('li')]
        result["yi" if category == "宜" else "ji"] = ' '.join(items)
    return result

def extract_lunar_info(soup):
    """提取农历/干支/生肖/彭祖百忌"""
    lunar_data = {"lunar": "", "ganzhi": "", "pzbj": ""}
    lunar_div = soup.find('div', class_='lunar')
    if lunar_div: lunar_data["lunar"] = lunar_div.text.replace("农历", "").strip()
    body_div = soup.find('div', class_='body')
    if body_div:
        ganzhi_p = body_div.find('p')
        if ganzhi_p: lunar_data["ganzhi"] = ganzhi_p.text.split('生肖属')[0].strip()     
        pzbj_p = ganzhi_p.find_next_sibling('p') if ganzhi_p else None
        if pzbj_p and "彭祖百忌:" in pzbj_p.text: lunar_data["pzbj"] = pzbj_p.text.replace("彭祖百忌:", "").strip()
    return lunar_data

def get_huangli_data(url, session):
    """复用session的副站数据抓取"""
    try:
        response = session.get(url, timeout=8)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        data = {"cs": "", "constellation": "", "weeks": "", "zs": "", "jianxing": "", "taishen": "", "num_weeks":"", "day": "", "wuxing_day": "","jieqi": "","next_jieqi": "","festival": "","next_festival": "",}
        shengxiao = ""
        
        for div in soup.find_all('div', class_='hang_left'):
            key_element = div.find('p', class_='first_corlor')
            value_element = div.find('p', class_='second_color')
            if key_element and value_element:
                key = key_element.get_text(strip=True)
                value = value_element.get_text(strip=True)
                if key == '生肖': shengxiao = value
                elif key == '冲煞':
                    if match := re.search(r'冲(.+?)煞(.+)', value): data["cs"] = f"{shengxiao}日冲{match.group(1)} 煞{match.group(2)}"
                elif key == '星座': data["constellation"] = value
                elif key == '十二建星': data["jianxing"] = value
                elif key == '值神': data["zs"] = value
                elif key == '第几周': data["num_weeks"] = value
                elif key == '胎神': data["taishen"] = value.replace('、', ' ')
                elif key == '纳音': data["wuxing_day"] = value

        week_div = soup.find('div', class_='zhong_week')
        data["weeks"] = week_div.get_text(strip=True) if week_div else ""
        qijie_div = soup.find('div', class_='qijie')
        if qijie_div: data["day"] = qijie_div.find('a').text.strip() if qijie_div.find('a') else ""
        
        
        sucha_div = soup.find('div', class_='sucha')
        if sucha_div:
            zhoushu_list = sucha_div.find_all('div', class_='zhoushu')
            
            # 自适应内容解析逻辑
            for idx, zhoushu in enumerate(zhoushu_list):
                text = zhoushu.get_text(strip=True)
                
                # 判断是否是节日信息块
                if any(keyword in text for keyword in ["节日", "节气"]):
                    # --------- 处理节气信息 ---------
                    if "节气" in text:
                        spans = zhoushu.find_all('span')
                        if len(spans) >= 3:
                            try:
                                # 添加额外格式过滤("当前节气()"转换为"")
                                data["jieqi"] = re.sub(r'当前节气\(?([^)]*)\)?', r'\1', spans[0].text).strip().strip('（）')
                                next_jq = spans[1].text.strip()
                                days = spans[2].text.strip()
                                data["next_jieqi"] = f"距离下一个节气{next_jq}还有{days}"
                            except IndexError:
                                pass
                    # --------- 处理节日信息 ---------
                    else:
                        try:
                            # 增强型节日解析
                            if "是" in text:
                                current_part = re.split(r'是|\(', text, 1)[1].split("距离")[0]
                                festivals = [f.strip(' 、（）') for f in re.split(r'[、,，]', current_part)]
                                data["festival"] = "、".join(filter(None, festivals))
                            else:
                                data["festival"] = text.split("距离")[0].strip('（')

                            # 下个节日处理逻辑
                            next_match = re.search(
                                r'距离下一个节日[（(]*(.*?)[）)]*还有(\d+)天', 
                                text
                            )
                            if next_match:
                                data["next_festival"] = f"距离下一个节日（{next_match.group(1).strip()}）还有{next_match.group(2).strip()}天"
                        except Exception as e:
                            print(f"节日信息解析错误：{str(e)}")

        # 结果后处理清洗
        data["festival"] = re.sub(r'[（(].*?[）)]', '', data["festival"]).strip('，、')
        return data
    except Exception as e:
        logging.warning(f"副站数据获取失败 {url}: {str(e)}")
        return {}

def get_shengxiao_info(date_obj, session):
    date_str = format_url_date(date_obj)
    url = f"https://m.tthuangli.com/jinrihuangli/xiaoyun_{date_str}.html"
    try:
        response = session.get(url, timeout=6)
        response.encoding = 'utf-8'
        response.raise_for_status()
    except Exception as e:
        logging.warning(f"生肖吉凶接口请求失败: {str(e)}")
        return {"good_sx": "获取失败", "bad_sx": "获取失败"}

    soup = BeautifulSoup(response.text, 'html.parser')
    result = {"good_sx": "", "bad_sx": ""}

    for container in soup.find_all('div', class_='jibie_tre'):
        if container.find('div', class_='teji_desx'):
            good_div = container.find('div', class_='sx_info')
            if good_div and good_div.span:
                result["good_sx"] = good_div.span.get_text().strip()
        elif container.find('div', class_='daoshuaisx'):
            bad_spans = [
                div.span.get_text().strip() 
                for div in container.find_all('div', class_='shuai_sx_info') 
                if div.span
            ]
            result["bad_sx"] = ' '.join(bad_spans)
    return result

def get_yiji_info(date_str, session):
    url = f"https://m.tthuangli.com/jinrihuangli/yiji_{date_str}.html"

    try:
        # 发送HTTPS请求
        response = session.get(url, timeout=10) 
        response.raise_for_status()
        
        # 编码处理
        if response.encoding == 'ISO-8859-1':
            response.encoding = response.apparent_encoding
        
        soup = BeautifulSoup(response.text, 'html.parser')

        def extract_by_css():
            container = soup.select_one('div.three_hang table')
            ji_numbers = container.select_one('td:nth-of-type(1) .second_color_ji span').text.strip()
            auspicious_time = container.select_one('td:nth-of-type(2) .second_color_ji span').text.strip()
            return ji_numbers, auspicious_time

        def extract_conflict_info(soup):
            """ 解析生肖冲害信息 """
            # 精确CSS路径定位
            conflict_div = soup.select_one('div.hljiexi div.sucha div.chong_sx')
            if not conflict_div:
                raise ValueError("未找到生肖冲突信息")
                
            # 获取原始文本并处理
            conflict_text = conflict_div.text.strip()
            
            # 多重清洗保障格式
            conflict_text = conflict_text.replace('\u3000', ' ')  # 替换全角空格
            conflict_text = conflict_text.replace('  ', ' ')     # 合并连续空格
            return conflict_text

        # 数据提取
        try:
            lucky_numbers, hour_range = extract_by_css()
            conflict_text = extract_conflict_info(soup)
        except AttributeError as e:
            print(f"元素定位失败：{str(e)}")
            raise

        # 数据格式化
        formatted_data = {
            "lucky_num": lucky_numbers.replace(' ', '').replace(',', '、'),
            "noble_time": hour_range.replace('、', '-').replace('点', '') + '点',
            "conflict_sx": conflict_text
        }

    except requests.exceptions.RequestException as e:
        print(f"网络请求失败：{str(e)}")
    except Exception as e:
        print(f"程序异常：{str(e)}")

    return formatted_data

def get_color_info(date_str, session):
    url = f"https://m.tthuangli.com/jinrihuangli/wuxingchuanyi_{date_str}.html" 
    try:
        response = session.get(url, timeout=10)  # 复用session
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        result = {
            'good_color': '',
            'bad_color': ''
        }

        # 处理大吉色
        djse = soup.find('div', class_='djse')
        if djse:
            dj_colors = djse.get_text(strip=True).split('：')[1]
            result['good_color'] = dj_colors.replace('、', ' ')  # 直接替换顿号为空格
            
        # 处理不宜色
        byse = soup.find('div', class_='byse')
        if byse:
            by_colors = byse.get_text(strip=True).split('：')[1]
            result['bad_color'] = by_colors.replace('、', ' ')  # 直接替换顿号为空格

    except Exception as e:
        print(f"[出现异常] {str(e)}")

    return result

def get_lucky_time(date_str, session):
    url = f'https://m.tthuangli.com/jinrihuangli/jishi_{date_str}.html'
    
    try:
        response = session.get(url, timeout=10)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        lucky_time_list = []
        
        for block in soup.find_all('div', class_='jiri_ji'):
            title_tag = block.find('div', class_='jiri_ji_tit')
            if not title_tag:
                continue
                
            title_parts = title_tag.text.strip().split()
            time_name = title_parts[-1] if title_parts else ''
            
            time_range_tag = block.find('div', class_='juti_time')
            time_range = time_range_tag.text.strip() if time_range_tag else ''
            
            if time_name and time_range:
                lucky_time_list.append(f"{time_name} {time_range}")

        # 修改关键点：删除末尾的 + ", "
        return {
            "lucky_time": ", ".join(lucky_time_list) if lucky_time_list else ""
        }
    
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

# ====================== 主抓取逻辑 ======================
def scrape_single_date(date_str, session):
    """单个日期的完整抓取流程"""
    main_url = f"https://www.huangli.net.cn/{date_str}.html"
    
    try:
        # ===== 主站数据抓取 =====
        response = session.get(main_url, timeout=8)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 解析基础数据
        base_data = {
            "datekey": parse_date_from_url(main_url),
            **extract_lunar_info(soup),
            **extract_suitable_items(soup),
            **parse_god_positions(soup),  # 单独解析财神位
            **parse_jixiong_items(soup)    # 解析吉神宜趋和凶煞宜忌
        }

        # ===== 并发获取副站数据 =====
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = {
                "sub": executor.submit(get_huangli_data, f"https://m.tthuangli.com/jinrihuangli/{date_str}.html", session),
                "sx": executor.submit(get_shengxiao_info, date_obj, session),
                "color": executor.submit(get_color_info, date_str, session),
                "yiji": executor.submit(get_yiji_info, date_str, session),
                "lucky_time": executor.submit(get_lucky_time, date_str, session)
            }

            # 合并副站数据（自动展开嵌套字段）
            for key, future in futures.items():
                try:
                    result = future.result()
                    # 特殊处理各个副站的数据结构
                    if key == "sub":
                        base_data.update({
                            "cs": result.get("cs", ""),
                            "constellation": result.get("constellation", ""),
                            "weeks": result.get("weeks", ""),
                            "zs": result.get("zs", ""),
                            "jianxing": result.get("jianxing", ""),
                            "taishen": result.get("taishen", ""),
                            "num_weeks": result.get("num_weeks", ""),
                            "day": result.get("day", ""),
                            "wuxing_day": result.get("wuxing_day", ""),
                            "jieqi": result.get("jieqi", ""),
                            "next_jieqi": result.get("next_jieqi", ""),
                            "festival": result.get("festival", ""),
                            "next_festival": result.get("next_festival", "")
                        })
                    elif key == "sx":
                        base_data["good_sx"] = result.get("good_sx", "")
                        base_data["bad_sx"] = result.get("bad_sx", "")
                    elif key == "color":
                        base_data["good_color"] = result.get("good_color", "")
                        base_data["bad_color"] = result.get("bad_color", "")
                    elif key == "yiji":
                        base_data.update({
                            "lucky_num": result.get("lucky_num", ""),
                            "noble_time": result.get("noble_time", ""),
                            "conflict_sx": result.get("conflict_sx", "")
                        })
                    elif key == "lucky_time":
                        base_data.update({
                            "lucky_time": result.get("lucky_time", ""),
                        })
                except Exception as e:
                    logging.warning(f"副站 {key} 数据获取失败: {str(e)}")

        return base_data
    except Exception as e:
        logging.error(f"主流程异常 {date_str}: {str(e)}")
        return None

# 封装公共查找函数
def find_item_by_title(title,soup):
    for div in soup.find_all('div', class_='item'):
        h4 = div.find('h4')
        if h4 and h4.text.strip() == title:
            return div
    return None

# ========= 新增的解析函数 =========
def parse_god_positions(soup):
    """解析财神位信息"""
    god_data = {"godposition": ""}

    try:
        caishen_div = find_item_by_title('财神位',soup)
        if not caishen_div:
            raise ValueError("未找到财神位区块")

        caishen_data = {}
        for li in caishen_div.find('ul').find_all('li'):
            key, val = li.text.strip().split('：', 1)
            caishen_data[key] = val
        
        god_data["godposition"] = f"喜神在{caishen_data['喜神']} 财神在{caishen_data['财神']} 福神在{caishen_data['福神']}"

    except Exception as e:
        logging.warning(f"财神位解析失败: {str(e)}")
    return god_data

def parse_jixiong_items(soup):
    """解析吉神宜趋和凶煞宜忌"""
    jixiong_data = {"jsyq": "", "xsyq": ""}
    try:
        # 吉神宜趋 - 添加class匹配
        jsyq_div = soup.find('h4', string='吉神宜趋').find_next('ul', class_='list-2')
        jixiong_data["jsyq"] = ' '.join([li.text.strip() for li in jsyq_div.find_all('li')]) if jsyq_div else ""
        
        # 凶煞宜忌 - 添加class匹配
        xsyq_div = soup.find('h4', string='凶煞宜忌').find_next('ul', class_='list-2')
        jixiong_data["xsyq"] = ' '.join([li.text.strip() for li in xsyq_div.find_all('li')]) if xsyq_div else ""
    except Exception as e:
        logging.warning(f"吉凶信息解析失败: {str(e)}")
    return jixiong_data 
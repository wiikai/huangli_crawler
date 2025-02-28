from datetime import datetime, timedelta
from scraper import create_session, scrape_single_date, format_url_date
from storage import HlDataSaver, HlDataLoader
import concurrent.futures
import logging

def main(start_date, end_date):
    """主控制函数"""
    # 生成日期列表
    date_objs = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    target_dates = [format_url_date(d) for d in date_objs]

    # 创建共享会话
    session = create_session()

    # 使用线程池并发处理日期
    # 存储优化：逐条处理+实时保存
    saver = HlDataSaver(filename='huangli_data.json')
    loader = HlDataLoader(filename='huangli_data.json')

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(scrape_single_date, date_str, session): date_str 
                  for date_str in target_dates}
        
        # 实时处理完成结果
        for future in concurrent.futures.as_completed(futures):
            date_str = futures[future]
            try:
                if data := future.result():
                    # 实时保存单日数据
                    saver.save_incrementally([data])
                    results.append(data)
                    logging.info(f"√ {date_str} 处理并保存完成")
            except Exception as e:
                logging.error(f"× {date_str} 处理失败: {str(e)}")

    return {
        "status": 0,
        "saved_days": len(results),
        "failed_days": len(target_dates)-len(results)
    }

if __name__ == "__main__":
    # 配置抓取日期范围
    start_date = datetime(2025, 3, 1)
    end_date = datetime(2025, 3, 1)
    
    # 执行抓取
    output = main(start_date, end_date)
    
    # 输出结果
    print(f"操作完成 | 成功: {output['saved_days']} 天 | 失败: {output['failed_days']} 天") 
import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from clickhouse_driver import Client
from tqdm import tqdm
import utils
import time
from express_helper import *
import datetime

pd.set_option("display.max_columns",100)

client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})

if __name__ == '__main__':
    start = time.time()
    date = datetime.datetime.today().strftime('%Y%m%d')
    # date = '20230424'
    # 预测分析数据计算与上传
    print("========= Run：{} =============".format(date))
    prediction_processor = PredictionProcessor(date)
    prediction_processor.run()
    print("完成预测分析，耗时：{}".format(time.time() - start))
    # 日内收益数据计算&上传
    intraday_pnl_processor = IntradayPnlProcessor(date)
    intraday_pnl_processor.run_async()

    print("上传结束，一共耗时：{}".format(time.time() - start))
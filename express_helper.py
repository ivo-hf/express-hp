import pandas as pd
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from clickhouse_driver import Client
from tqdm import tqdm
import utils
import time
import multiprocessing as mp

class IntradayPnlProcessor:

    def __init__(self,date):
        self.date = date
        self.client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num', settings={'use_numpy': True})
        self.prev_date = utils.get_prev_date(date)
        self.account_info = utils.get_portfolio_info(date)
        self.min_data_dict = {}
        self.res_dict = {}
        self.min_map = None

    def run(self):
        '''调试'''
        for account in ["PHB_D2HP_0004","PHB_D2HP_0002"]:
            self.run_account(account)
        self.upload()

        optimize_code = "optimize table hp.ModelDailyInfo final"
        self.client.execute(optimize_code)

        optimize_code = "optimize table hp.FixOffsetPredExp final"
        self.client.execute(optimize_code)
        optimize_code = "optimize table hp.TradeExpByGroup final"

        self.client.execute(optimize_code)
        print("Finish Optimize TradeExpByGroup")

    def run_async(self):
        '''多进程运行, 提前下好数据'''
        self.prepare_data()
        processes = []
        for account in self.account_info["Account"]:
            p = mp.Process(target=self.run_account,args=(account,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

        optimize_code = "optimize table hp.IntradayPnl final"
        self.client.execute(optimize_code)

    def prepare_data(self):
        start = time.time()
        print("Start Read History")
        client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                        settings={'use_numpy': True})
        self.pos_dict = {}
        self.trade_dict = {}
        self.min_data_dict = {}
        code_list = list(client.query_dataframe(
            """SELECT DISTINCT Instrument
                    FROM datahouse.PositionTableDistinctView
                    WHERE Account like 'PHB%HP%DC' and Date = '{date}' 
                    and right(Instrument,2) IN( 'SH','SZ')""".format(date=self.date)
                                           )["Instrument"])
        for account in tqdm(self.account_info["Account"]):
            if account[-3:] == "Pos":
                self.pos_dict[account] = utils.get_pos(client,self.prev_date,account)
            else:
                self.trade_dict[account] = utils.get_trades(client,self.date,account)

        for code in code_list:
            self.min_data_dict[code] = utils.get_min_data(self.date,code)
        print("日内数据准备结束，耗时：{}".format(time.time() - start))

    def get_insert_code(self,res):
        insert = [
            "'{}'".format(res[0]),
            "'{}'".format(res[1]),
            "'{}'".format(res[2]),
            "'{}'".format(res[3]),
            str(int(res[4])),
            str(res[5]),
            str(res[6])
        ]
        return "({})".format(",".join(insert))

    def run_account(self,account):
        client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                        settings={'use_numpy': True})
        print("Calc Account:{}".format(account))
        if account[-3:] == "Pos":
            account_res = self.get_yestoday_pnl(account)
        else:
            account_res = self.get_intraday_pnl(account)

        if len(account_res) == 0:
            print("No Result for Account {}".format(account))
            return

        ## Upload Results
        print("Upload Account:{}".format(account))
        query = "INSERT INTO hp.IntradayPnl values "
        rows = client.insert_dataframe(
            f'INSERT INTO hp.IntradayPnl VALUES',account_res
        )
        # for res in account_res.values:
        #     query += self.get_insert_code(res)
        # client.execute(query)


    def get_yestoday_pnl(self,account):
        df_pos = self.pos_dict[account]
        if len(df_pos) == 0:
            return pd.DataFrame()
        df_pos = df_pos[df_pos["NetPos"] != 0]
        code_list = list(df_pos["Instrument"])
        pos_list = list(df_pos["NetPos"])
        pos_dict = {code_list[i]:pos_list[i] for i in range(len(code_list))}
        close_dict = utils.get_close_price(self.prev_date)
        df_list = []
        for code in tqdm(code_list):
            yestoday_pnl = []
            prev_price = close_dict[code]
            pos = pos_dict[code]
            if pos == 0:
                continue
            if code not in self.min_data_dict:
                self.min_data_dict[code] = utils.get_min_data(self.date,code)
            if self.min_map == None:
                self.min_map = {v:k for k,v in enumerate(self.min_data_dict[code].columns)}
            min_data_arr = self.min_data_dict[code].values
            for i in range(len(min_data_arr)):
                min_data = min_data_arr[i]
                target_price = min_data[self.min_map["Close"]]
                pnl = pos * (target_price - prev_price)
                yestoday_pnl.append(pnl)
            df_cum_pnl = pd.DataFrame()
            df_cum_pnl["CumPnl"] = yestoday_pnl
            df_cum_pnl["IntradayTime"] = min_data_arr[:,self.min_map["IntradayTime"]]
            df_cum_pnl["Rtn"] = min_data_arr[:,self.min_map["Rtn"]]
            df_cum_pnl["Code"] = code
            df_cum_pnl["Date"] = self.date
            df_cum_pnl["Account"] = account
            df_cum_pnl["Timestamp"] = min_data_arr[:, self.min_map["Ntime"]]
            df_list.append(df_cum_pnl)
        df_concat = pd.concat(df_list)[["Account","Date","Code","Timestamp","IntradayTime","CumPnl","Rtn"]]
        return df_concat

    def get_intraday_pnl(self,account):
        df_all_trades = self.trade_dict[account]
        if len(df_all_trades) == 0:
            return pd.DataFrame()
        trade_map = {v:k for k,v in enumerate(df_all_trades.columns)}
        code_list = list(set(df_all_trades["InstrumentId"]))
        df_list = []
        for code in tqdm(code_list):
            intraday_pnl = []
            trades_arr = df_all_trades[df_all_trades["InstrumentId"] == code].values
            if code not in self.min_data_dict:
                self.min_data_dict[code] = utils.get_min_data(self.date,code)
            if self.min_map == None:
                self.min_map = {v:k for k,v in enumerate(self.min_data_dict[code].columns)}
            min_data_arr = self.min_data_dict[code].values
            for i in range(len(min_data_arr)):
                min_data = min_data_arr[i]
                target_time = min_data[self.min_map["Ntime"]]
                target_price = min_data[self.min_map["Close"]]
                trades_cum_arr = trades_arr[trades_arr[:,trade_map["TradedTime"]] < target_time]
                pnl = np.sum((((2*(trades_cum_arr[:,trade_map["Direction"]] == 'BUY')) - 1)
                        * (target_price - trades_cum_arr[:,trade_map["TradedPrice"]])
                        * trades_cum_arr[:,trade_map["TradedNum"]]) - trades_cum_arr[:,trade_map["FeeCost"]])
                intraday_pnl.append(pnl)

            df_cum_pnl = pd.DataFrame()
            df_cum_pnl["CumPnl"] = intraday_pnl
            df_cum_pnl["IntradayTime"] = min_data_arr[:,self.min_map["IntradayTime"]]
            df_cum_pnl["Rtn"] = min_data_arr[:,self.min_map["Rtn"]]
            df_cum_pnl["Code"] = code
            df_cum_pnl["Date"] = self.date
            df_cum_pnl["Account"] = account
            df_cum_pnl["Timestamp"] = min_data_arr[:,self.min_map["Ntime"]]
            df_list.append(df_cum_pnl)
        df_concat = pd.concat(df_list)[["Account","Date","Code","Timestamp","IntradayTime","CumPnl","Rtn"]]
        return df_concat


class PredictionProcessor:

    def __init__(self,date):
        self.date = date
        self.client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num', settings={'use_numpy': True})
        self.markets = ["SZ","SH"]
        self.pools = ["All","DC"]
        self.offset_type = "History" # 可以等于Current
        self.quantile = 1
        self.rolling_window = 5


    def run(self):
        self.update_model_daily_info()
        self.update_fix_offset_pred_exp()
        self.update_real_exp()

    def update_model_daily_info(self):
        query_code = """
            SELECT * FROM hp.ModelRecord WHERE Date <= '{}'
            """.format(self.date)
        model_record = self.client.query_dataframe(query_code)
        if len(model_record) == 0:
            return

        for market in list(set(model_record.Market)):
            model_record_by_market = model_record[model_record.Market == market]
            for mark in list(set(model_record_by_market.Mark)):
                data = list(model_record_by_market[model_record_by_market.Mark == mark].sort_values(by="Date").iloc[-1])
                data[0] = self.date
                data = ["'{}'".format(i) for i in data]
                insert_code = """
                    INSERT INTO hp.ModelDailyInfo values ({})
                    """.format(",".join([str(i) for i in data]))
                self.client.execute(insert_code)



    def update_fix_offset_pred_exp(self):
        for market in self.markets:
            for pool in self.pools:
                query_mark = """
                                    SELECT Distinct Mark FROM hp.ProdPrediction WHERE Date = '{}' and right(Code,2) = '{}'
                                    """.format(self.date, market)
                df_mark = self.client.query_dataframe(query_mark)
                if len(df_mark) == 0:
                    print("No Prediction Data:{},{},{}".format(self.date,pool,market))
                    return
                mark_list = list(df_mark["Mark"])
                for mark in mark_list:
                    utils.insert_pred_exp(
                        date=self.date,
                        market=market,
                        mark=mark,
                        quantile=self.quantile,
                        pool = pool,
                        rolling_window=self.rolling_window
                    )


    def update_real_exp(self):
        date = self.date
        portfolio_info = utils.get_portfolio_info(date)
        for market in self.markets:
            for group in sorted(list(set(portfolio_info["Group"]))):
                group_info = portfolio_info[portfolio_info["Group"] == group]
                mark_list = list(set(group_info["Mark"]))
                df_exp = utils.get_real_exp(market, mark_list, date)
                if len(df_exp) == 0:
                    continue
                res = list(df_exp.loc[0])
                insert_list = ["'{}'".format(res[0]), "'{}'".format(res[1]), "'{}'".format(group)] + res[2:]
                insert_code = """
                     INSERT INTO hp.TradeExpByGroup values ({})
                     """.format(",".join([str(i) for i in insert_list]))
                self.client.execute(insert_code)


import pandas as pd
import numpy as np
from clickhouse_driver import Client

def get_prev_date(date):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    query = """
    SELECT Distinct TRADE_DT
    FROM winddb.ashareeodprices
    WHERE TRADE_DT > '20220101' AND TRADE_DT < '{date}' AND S_INFO_WINDCODE = '000001.SZ'
    ORDER BY TRADE_DT
    """.format(date=date)
    prev_date = client.query_dataframe(query)["TRADE_DT"].iloc[-1]
    return prev_date

def get_min_data(date,code):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    df_min_data = client.query_dataframe("""
        SELECT InstrumentID,Ntime,Ttime,Close
        FROM kmdhistory.kdatas 
        WHERE Date = '{date}' and InstrumentID = '{code}'
        ORDER BY InstrumentID,Ntime
        """.format(date=date,code=code))
    df_min_data = df_min_data.iloc[1:]
    df_min_data["IntradayTime"] = (df_min_data["Ntime"] - df_min_data["Ntime"].iloc[0]).apply(
        lambda x:x.seconds / 60 if x.seconds < 7200 else (x.seconds - 5400) / 60
    )
    df_min_data["Rtn"] = df_min_data["Close"] / df_min_data["Close"].iloc[0] - 1
    if len(df_min_data[df_min_data["IntradayTime"] < 0]) > 1:
        print("Error")
    return df_min_data

def get_portfolio_info(date):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    query_account = """
    SELECT * FROM (
        SELECT Account,
               Belong,
               Performance,
               sum(Direction) Enable
        from hp.PortfolioInfo
        WHERE StName == 'HermesPro' and Date <= '{}' and Account Like 'PHB%'
        GROUP BY Account, Belong,Performance
        )
        where Enable == 1
    """.format(date)
    account_info = client.query_dataframe(query_account)
    account_info["Group"] = account_info["Belong"] + account_info["Performance"]
    account_info["Group"] = account_info["Group"].apply(lambda x:x.replace(" ",''))
    account_info["Mark"] = account_info["Account"].apply(lambda x:x.split('_')[1][:2])
    return account_info

def get_pos(client,date,account):
    query = """SELECT * 
    FROM datahouse.PositionTableDistinctView  
    WHERE Date = '{date}' and Account = '{account}'
    """.format(date=date,account=account)
    df_pos = client.query_dataframe(query)
    return df_pos

def get_trades(client,date,account):
    query = """
    SELECT * 
    FROM datahouse.TradeTableDistinctView
    WHERE Date = '{date}' and PortfolioName = '{account}' and ExchangeId in ('SZE','SSE')
    """.format(date=date,account=account)
    df_trades = client.query_dataframe(query)
    return df_trades

def get_close_price(date):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    query = """
    SELECT S_INFO_WINDCODE Code,
            S_DQ_CLOSE Close
    FROM winddb.ashareeodprices
    WHERE TRADE_DT = '{date}' 
    """.format(date=date)

    price = client.query_dataframe(query)
    close_dict = {
        price.iloc[i]["Code"]:float(price.iloc[i]["Close"]) for i in range(len(price))
    }
    return close_dict

def insert_pred_exp(date,market,mark,quantile,pool,rolling_window):
    offset_type = 'History'
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    buy_offset,sell_offset = calculate_offset(
        date=date,
        market=market,
        mark=mark,
        quantile=quantile,
        pool=pool,
        rolling_window=rolling_window
    )
    if np.isnan(buy_offset) or np.isnan(sell_offset):
        return
    if pool == "All":
        query_code = """
        SELECT round(avgIf(RealBuy[1],PredictionMid - (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3 > {buy_offset}),4) ExpBuy,
               round(avgIf(RealSell[1],PredictionMid + (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3 < {sell_offset}),4) ExpSell,
               round(avg(PredictionMid),4) PredMean,
               round(stddevPopStable(PredictionMid),4) PredStd
        FROM hp.ProdPrediction
        WHERE right(Code,2) = '{market}' and Mark = '{mark}' and Date = '{date}'
         AND NOT isNaN(RealBuy[1])
         AND NOT isNaN(RealSell[1])
        """.format(
            market = market,
            mark=mark,
            date=date,
            buy_offset=buy_offset,
            sell_offset=sell_offset
        )
    elif pool == 'DC':
        query_code = """
        SELECT round(avgIf(RealBuy[1],PredictionMid - (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3 > {buy_offset}),4) ExpBuy,
               round(avgIf(RealSell[1],PredictionMid + (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3 < {sell_offset}),4) ExpSell,
               round(avg(PredictionMid),4) PredMean,
               round(stddevPopStable(PredictionMid),4) PredStd
        FROM(
            SELECT * 
              FROM hp.ProdPrediction
            WHERE right(Code,2) = '{market}' and Mark = '{mark}' and Date = '{date}'
             AND NOT isNaN(RealBuy[1])
             AND NOT isNaN(RealSell[1])
             AND Code in (
                SELECT Distinct Instrument
                FROM (
                    SELECT Date,
                           Account,
                           Instrument,
                           YstLongPos - ShortPos StaticPositionT0
                    FROM datahouse.PositionTableDistinctView
                    WHERE Date = '{date}' and Account like '%{mark}HP%DC' and Account not like 'Dummy%'
                )
                WHERE StaticPositionT0 > 0
             )
        )
        """.format(
            market = market,
            mark=mark,
            date=date,
            buy_offset=buy_offset,
            sell_offset=sell_offset,
            pool=pool
        )
    res = client.query_dataframe(query_code)
    data = ["'{}'".format(date),
            "'{}'".format(market),
            "'{}'".format(mark),
            "'{}'".format(pool),
            "'{}'".format(offset_type),
            "{}".format(quantile),
            buy_offset,
            sell_offset
            ] + list(res.values[0])
    if np.isnan(res.values[0][0]):
        return
    if np.abs(res.values[0][0]) > 10 or np.abs(res.values[0][1]) > 10:
        return

    insert_code = """
         INSERT INTO hp.FixOffsetPredExp values ({})
         """.format(",".join([str(i) for i in data]))
    client.execute(insert_code)


def calculate_offset(date,market,mark,quantile,pool,rolling_window):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    query_date = "SELECT Distinct Date FROM hp.ProdPrediction ORDER BY Date"
    date_list = [d.strftime("%Y%m%d") for d in client.query_dataframe(query_date)["Date"]]
    start_date = date_list[date_list.index(date)-rolling_window]
    end_date = date_list[date_list.index(date)-1]

    if pool == 'All':
        query_offset = """
        SELECT avgIf(OffsetBuy,not isNaN(OffsetBuy)) OffsetBuy,
               avgIf(OffsetSell,not isNaN(OffsetSell)) OffsetSell
        FROM
        (
            SELECT quantileExact({buy_qtr})(PredictionMid - (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3) OffsetBuy,
                   quantileExact({sell_qtr})(PredictionMid + (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3) OffsetSell
            FROM hp.ProdPrediction 
            WHERE right(Code,2) = '{market}' and Mark = '{mark}' and Date >= '{start_date}' and Date <= '{end_date}'
            GROUP BY Code,Date
        )
        
        """.format(market=market,
                   mark=mark,
                   start_date=start_date,
                   end_date=end_date,
                   buy_qtr=(100 - quantile)/100,
                   sell_qtr=quantile/100)
    elif pool == 'DC':
        query_offset = """
        SELECT avgIf(OffsetBuy,not isNaN(OffsetBuy)) OffsetBuy,
               avgIf(OffsetSell,not isNaN(OffsetSell)) OffsetSell
        FROM
        (
            SELECT quantileExact({buy_qtr})(PredictionMid - (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3) OffsetBuy,
                   quantileExact({sell_qtr})(PredictionMid + (AskPrice1 - BidPrice1) / (AskPrice1 + BidPrice1) * 1e3) OffsetSell
            FROM 
            (
                SELECT * FROM hp.ProdPrediction
                WHERE right(Code,2) = '{market}' and Mark = '{mark}' and Date >= '{start_date}' and Date <= '{end_date}'
                and Code in (
                        SELECT Distinct Instrument
                    FROM (
                        SELECT Date,
                               Account,
                               Instrument,
                               YstLongPos - ShortPos StaticPositionT0
                        FROM datahouse.PositionTableDistinctView
                        WHERE Date >= '{start_date}' and Date <= '{end_date}' and Account like '%{mark}HP%DC' and Account not like 'Dummy%'
                    )
                    WHERE StaticPositionT0 > 0
                )
            ) AS P
            GROUP BY Code,Date
        )
        """.format(market=market,
                   mark=mark,
                   start_date=start_date,
                   end_date=end_date,
                   buy_qtr=(100 - quantile)/100,
                   sell_qtr=quantile/100,
                  pool=pool)
    offsets = client.query_dataframe(query_offset)
    buy_offset,sell_offset = offsets["OffsetBuy"][0],offsets["OffsetSell"][0]
    return buy_offset,sell_offset

def get_real_exp(market,mark_list,date):
    client = Client(host='192.168.47.110', port='9000', user='hfadmin', password='cmz8QVZ_wmw-vzv8num',
                    settings={'use_numpy': True})
    query_code = """
    SELECT Date,  
       right(Code,2) Market,
       round(sumIf(RealBuy[1] * Filled * Price, Direction = 0) / sum(Filled * Price), 3)           BuyTradeExp,
       round(sumIf(RealSell[1] * Filled * Price, Direction = 1) / sum(Filled * Price), 3)           SellTradeExp,
       round(sumIf(RealBuy[1] * Volume * Price, Direction = 0) / sum(Volume * Price), 3)          BuyOrderExp,
       round(sumIf(RealSell[1] * Volume * Price, Direction = 1) / sum(Volume * Price), 3)          SellOrderExp,
       round(sumIf(Volume * Price,Direction=0)) BuyOrderAmount,
       round(sumIf(Volume * Price,Direction=1)) SellOrderAmount,
       round(sumIf(Filled * Price,Direction=0)) BuyTradeAmount,
       round(sumIf(Filled * Price,Direction=1)) SellTradeAmount
    FROM
        (SELECT *
        FROM (
             SELECT *
             FROM hp.ProdPrediction
             WHERE NOT isNaN(RealBuy[4])
               AND NOT isNaN(RealSell[4])
               AND Date = '{date}' and Mark in ({marks})
               AND right(Code,2) == '{market}'
             ) as P
        JOIN (
            SELECT *
            FROM hp.ProdOrder
            WHERE Class = 0 and Rejected = 0 and right(Code,2) == '{market}'
        ) AS O
        ON P.Mark = O.Mark AND P.Code = O.Code AND P.Date = O.Date AND P.TriggerId = O.InsertTriggerId
    )
    GROUP BY Date,Market
    """.format(date=date,marks=",".join(["'{}'".format(i) for i in mark_list]),market=market)
    df = client.query_dataframe(query_code)
    return df
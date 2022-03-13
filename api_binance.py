""" Python v.3.8.7  PostgresSQL v.13 web_api Binance asyncio js18user """

import asyncio
import websockets
import json
import random
import time
import math
import psycopg2
from psycopg2 import Error
from bin_question import *


class PrError(Exception):
    pass


class ClientError(PrError):
    pass


""" It is a information for operate  API """
ticker_structure = {
    "e": "24hrTicker",  # Тип события
    "E": 123456789,  # + 1 Время события
    "s": "symbol",  # + 2 Пара
    "p": "0.0015",  # Изменение цены
    "P": "250.00",  # Изменение цены в процентах
    "w": "0.0018",  # Средневзвешенная цена
    "x": "0.0009",  # Цена закрытия предыдущих суток
    "c": "0.0025",  # Цена закрытия текущих суток
    "Q": "10",  # Объем закрытия
    "b": "0.0024",  # + 9 Цена лучшего Bid (спроса/покупки)
    "B": "10",  # Объем лучшего Bid
    "a": "0.0026",  # + 11 Цена лучшего ask (преложение/продажа)
    "A": "100",  # Объем лучшего ask
    "o": "0.0010",  # Цена открытия
    "h": "0.0025",  # High
    "l": "0.0010",  # Low
    "v": "10000",  # Общий объем торгов в базовой валюте
    "q": "18",  # Общий объем торгов в квотируемой валюте
    "O": 0,  # Время начала сбора статистики
    "C": 86400000,  # Время окончания сбора статистики
    "F": 0,  # ID первой сделки
    "L": 18150,  # Id последней сделки
    "n": 18151  # Общее кол-во сделок
    }
""" Ended information for operate """


async def web_api_binance_analyze(number, interval):
    """  It is a main procedure for operate  """

    print(f'Start of process ( {number} ), interval ( {interval} )')

    try:
        exchange, timestamp, instrument, big, ask = 0, 1, 2, 3, 4

        ticker_number = 0
        skip = '\n'
        list_dicts, dicts = list(), dict()

        list_dicts.clear()

        """ According to the terms of reference, the interval is interval seconds """

        sql_insert_query = 'INSERT INTO tickers (exchange, timestamp, symbol, big, ask) VALUES (%s,%s,%s,%s,%s)'

        connection = psycopg2.connect(
            'dbname=fintech user=postgres password=aa4401 host=localhost port=5432')
        await asyncio.sleep(random.randint(0, 2) * 0.0001)

        async with websockets.connect(
                'wss://stream.binance.com/ws/' + bin_question()) as web_api_binance:
            """ Socket subscription with tickers set to request """

            start_time = time.time()
            """ run until interrupt --> Ctrl C """
            while 1:

                ticker = json.loads(await web_api_binance.recv())
                """ Reading ticker, type(ticket) ---> dictionary  """
                # print(ticker)

                ticker_number += 1

                """ Create dicts ---> operate dictionary  """
                dicts[exchange] = 'binance'
                dicts[timestamp] = int(ticker['C'])
                dicts[instrument] = ticker['s']
                dicts[big] = float(ticker['b'])
                dicts[ask] = float(ticker['a'])

                """ Process control printing """
                # print(dicts)

                """ Creating list of dicts for insert in tickers table(db = fintech, PostgreSQL)"""
                """ New instrument(dicts) is appended to list """
                """ Update dicts information in list of dicts if instrument is repeated """
                if ticker_number == 1:
                    list_dicts.append(dicts.copy())

                else:
                    length_list_dicts = len(list_dicts)

                    if length_list_dicts >= 1:
                        number_on_the_list, update_indicator = 0, 0

                        while (number_on_the_list < length_list_dicts) and (update_indicator == 0):

                            if list_dicts[number_on_the_list][instrument] == dicts[instrument]:

                                list_dicts[number_on_the_list][timestamp] = dicts[timestamp]
                                list_dicts[number_on_the_list][big] = dicts[big]
                                list_dicts[number_on_the_list][ask] = dicts[ask]

                                update_indicator = 1

                            else:
                                pass

                            number_on_the_list += 1

                        else:
                            pass

                        if update_indicator == 0:
                            list_dicts.append(dicts.copy())

                        else:
                            pass

                    else:
                        pass

                await asyncio.sleep(random.randint(0, 2) * 0.00001)

                """ the process continues for a time interval in sec """
                if (time.time() - start_time) >= interval:

                    length_list_dicts = len(list_dicts)

                    """ After the time interval has elapsed, 
                        the list of dictionaries is converted into a list of tuples 
                        and written to the database """

                    # [print(list_dicts[x]) for x in range(length_list_dicts)]
                    """ process control printing """

                    s_time = time.time()

                    """ Writing tickers to the database """
                    with connection.cursor() as cursor:

                        await asyncio.sleep(random.randint(0, 2) * 0.00001)

                        cursor.executemany(sql_insert_query, [tuple(v) for v in map(dict.values, list_dicts)])

                        await asyncio.sleep(random.randint(0, 2) * 0.00001)

                        # connection.commit()

                    list_dicts.clear()

                    """ Process control printing """
                    print(skip,
                          'process number is  ---->', number, skip,
                          'process interval is --->', interval, skip,
                          'time for insert in db ->', math.floor((time.time() - s_time) * 1000), skip,
                          'actual time in m/sec -->',
                          math.floor(((time.time() - start_time - interval) * 1000)), skip,
                          'com tickers by time --->', ticker_number, skip,
                          'com tickers insert  --->', length_list_dicts, skip)
    
                    ticker_number = 0

                    start_time = time.time()
    
                else:
                    pass

            else:
                pass

    except KeyboardInterrupt:
        pass

    except (ClientError, Error, psycopg2.DatabaseError, GeneratorExit) as err:
    
        print('error : ', err)
        pass
    
    finally:
        cursor.close()

        print(f'End of process ( {number} )')

    return()


async def asynchronous(intervals):

    futures = [web_api_binance_analyze(number, intervals[number])
               for number in range(len(intervals))]
    for i, future in enumerate(asyncio.as_completed(futures)):
        await future


def main(intervals):

    try:

        asyncio.get_event_loop().run_until_complete(asynchronous(intervals))

    except KeyboardInterrupt:
        pass

    except (ClientError, GeneratorExit) as err:

        print('error : ', err)
        pass

    finally:
        print('ok')
    return()


if __name__ == "__main__":
    """ run until interrupt --> Ctrl C """

    main([6, 6, 6])

exit()

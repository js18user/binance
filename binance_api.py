""" Python v.3.8.7  PostgresSQL v.13 web_api Binance asyncio/await asyncpg js18user """

import asyncio
import asyncpg
import websockets
import json
import time
import math

from bin_question import *


class PrError(Exception):
    pass


class ClientError(PrError):
    pass


""" It is a information for operate with API """
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
        exchange, timestamp, symbol, big, ask = 'binance', 'C', 's', 'b', 'a'

        ticker_number = 0
        skip = '\n'
        dict_of_tickers = dict()

        """ According to the terms of reference, the interval is interval seconds """

        sql_insert_query = '''INSERT INTO tickers (exchange, timestamp, symbol, big, ask) VALUES ($1,$2,$3,$4,$5)'''

        connection = await asyncpg.connect(database='fintech',
                                           user='postgres',
                                           password='aa4401',
                                           host='localhost',
                                           port=5432)

        await asyncio.sleep(0)

        async with websockets.connect(
                'wss://stream.binance.com/ws/' + bin_question()) as web_api_binance:
            """ Socket subscription with tickers set to request """

            await asyncio.sleep(0)
            start_time = time.time()
            """ run until interrupt --> Ctrl C """
            while 1:

                ticker = json.loads(await web_api_binance.recv())
                """ Reading ticker, type(ticket) is dictionary  """
                # print(ticker)

                ticker_number += 1

                """ Create dict ---> operate dictionary  """

                dict_of_tickers[ticker[symbol]] = (
                    exchange,
                    int(ticker[timestamp]),
                    ticker[symbol],
                    float(ticker[big]),
                    float(ticker[ask])
                )

                await asyncio.sleep(0)

                """ the process continues for a time interval in sec """
                if (time.time() - start_time) >= interval:

                    length_dict = len(dict_of_tickers)

                    """ After the time interval has elapsed, 
                        the dictionary is converted into a list of tuples 
                        and written to the database """

                    # print([(*dict_of_tickers.values())])
                    """ process control printing """

                    s_time = time.time()

                    await connection.executemany(sql_insert_query, [(*dict_of_tickers.values())])
                    """ Writing tickers to the database """
                    await asyncio.sleep(0)

                    """ Process control printing """
                    print(skip,
                          f'process number is  ----> ({number})', skip,
                          f'process interval is ---> ({interval})', skip,
                          'time for insert in db ->', math.floor((time.time() - s_time) * 1000), skip,
                          'actual time in m/sec -->',
                          math.floor(((time.time() - start_time - interval) * 1000)), skip,
                          'com tickers by time --->', ticker_number, skip,
                          'com tickers inserted--->', length_dict, skip)

                    ticker_number = 0
                    dict_of_tickers.clear()
                    start_time = time.time()
                    await asyncio.sleep(0)

                else:
                    pass

            else:
                pass

    except KeyboardInterrupt:

        pass

    except (ClientError, asyncpg.exceptions, GeneratorExit) as error:

        print('error : ', error)
        pass

    finally:
        print(f'End of process ( {number} , interval ( {interval} ))')
        # pass

    return ()


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

    except (ClientError, GeneratorExit) as error:

        print('error : ', error)
        pass

    finally:
        print('The end of processes')
    return ()


if __name__ == "__main__":
    """ run until interrupt --> Ctrl C """

    main([8, 8, 4, ])

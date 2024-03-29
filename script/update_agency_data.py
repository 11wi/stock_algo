from pathlib import Path
from random import uniform
from time import sleep

import pandas as pd
from sqlitedict import SqliteDict
from tqdm import tqdm


def update_stock_code() -> None:
    # 종목 명부 페이지:
    # http://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage#
    # network - form 데이터 참조
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType={}'
    kospi = pd.concat(pd.read_html(url.format('stockMkt'),
                                   header=0, index_col=0, converters={'종목코드': str}))['종목코드']
    kosdaq = pd.concat(pd.read_html(url.format('kosdaqMkt'),
                                    header=0, index_col=0, converters={'종목코드': str}))['종목코드']
    stock_codes = kospi.append(kosdaq)
    stock_codes.to_pickle(Path('..') / 'meta' / 'stock_code.pkl')


def get_code(stock_name) -> str:
    stock_codes = pd.read_pickle(Path('..') / 'meta' / 'stock_code.pkl')
    target_code = stock_codes.loc[stock_name]
    return str(target_code)


def get_all_code():
    stock_codes = pd.read_pickle(Path('..') / 'meta' / 'stock_code.pkl')
    out = stock_codes.astype(str).values
    return out


def get_stock_name(stock_code: str):
    stock_codes = pd.read_pickle(Path('..') / 'meta' / 'stock_code.pkl')
    assert type(stock_code) == str
    target = stock_codes[stock_codes == stock_code].index
    assert target.shape == (1,)
    return target.item()


def crawl_delay():
    lambda_ = uniform(1, 3)
    time_to_sleep = pd.np.random.poisson(lambda_, 1).item()
    sleep(time_to_sleep)


def crawl_agency_volume(stock_code: str):
    def data_proc_detail(agency_volume_detail):
        agency_volume_detail = agency_volume_detail.dropna().rename(columns={'거래량': '기관매도량', '거래량.1': '기관매수량'})
        foreign_agent_indicator = '외국계추정합'
        foreign_agent_index = agency_volume_detail.query("매도상위 == @foreign_agent_indicator").index
        agency_volume_detail.loc[foreign_agent_index, '매수상위'] = foreign_agent_indicator

        def detail_parser(string):
            return ', '.join(agency_volume_detail[string].map(str).values)

        agency_volume_detail_ = pd.DataFrame({
            '매도상위': detail_parser('매도상위'),
            '매수상위': detail_parser('매수상위'),
            '기관매도량': detail_parser('기관매도량'),
            '기관매수량': detail_parser('기관매수량')
        }, index=[today])
        agency_volume_detail_.index.name = '날짜'
        return agency_volume_detail_

    def data_proc_meta(agency_volume_meta):
        try:
            col_name = list()
            for x in agency_volume_meta.columns.to_flat_index():
                if len(x) == 2:
                    if x[0] != x[1]:
                        col_name.append(''.join(x))
                    else:
                        col_name.append(x[0])
        except Exception:
            pass
        else:
            agency_volume_meta.columns = col_name

        agency_volume_meta = (agency_volume_meta
                              .drop(['외국인보유주수', '외국인보유율', '전일비', ], axis=1)
                              .dropna()
                              .applymap(str)
                              .assign(기관순매매량=lambda df: df['기관순매매량'].str.replace('\+|\,', '')
        if df['기관순매매량'].dtype.name != 'float64' else df['기관순매매량'])
                              .assign(외국인순매매량=lambda df: df['외국인순매매량'].str.replace('\+|\,', '')
        if df['외국인순매매량'].dtype.name != 'float64' else df['외국인순매매량'])
                              .assign(등락률=lambda df: df['등락률'].str.replace('\+|\%', '').str.replace('∞', '0')
        if df['외국인순매매량'].dtype.name != 'float64' else df['외국인순매매량'])
                              .astype({'날짜': 'datetime64[ns]',
                                       '종가': float,
                                       '등락률': float,
                                       '거래량': float,
                                       '기관순매매량': float,
                                       '외국인순매매량': float})
                              .set_index('날짜')
                              .sort_index()
                              .dropna())
        agency_volume_meta.index = agency_volume_meta.index.date
        return agency_volume_meta

    crawl_delay()
    naver_stock_info_url = 'https://finance.naver.com/item/frgn.nhn?code={}'.format(stock_code)
    agency_volume_detail, agency_volume_meta = pd.read_html(naver_stock_info_url,
                                                            attrs={'class': 'type2'},
                                                            encoding='cp949')

    return data_proc_meta(agency_volume_meta), data_proc_detail(agency_volume_detail)


def update_diff_only(stored: pd.DataFrame, recent_crawled: pd.DataFrame) -> pd.DataFrame:
    new_records = recent_crawled.index.difference(stored.index)
    updated = stored.append(recent_crawled.loc[new_records], sort=False)
    return updated


def get_businessday() -> pd.Timestamp:
    now = pd.Timestamp.now()
    if now.hour <= 4:
        now = now - pd.Timedelta('1 days')

    if now.day_name() == 'Saturday':
        now -= pd.Timedelta('1 day')
    elif now.day_name() == 'Sunday':
        now -= pd.Timedelta('2 day')

    return now.date()


if __name__ == '__main__':
    update_stock_code()
    stock_codes = get_all_code()
    agency_db = SqliteDict(Path('..') / 'db' / 'agency_db.sqlite', autocommit=True)

    for stock_code in tqdm(stock_codes):
        today = get_businessday()

        if stock_code not in agency_db:
            agency_db.update({stock_code: {'agency_meta': pd.DataFrame(), 'agency_detail': pd.DataFrame()}})

        is_not_duplicated = today not in agency_db[stock_code]['agency_detail'].index
        if is_not_duplicated:
            agency_volume_meta, agency_volume_detail = crawl_agency_volume(stock_code)
            if not agency_volume_meta.empty:
                agency_volume_meta_ = update_diff_only(agency_db[stock_code]['agency_meta'], agency_volume_meta)
                agency_volume_detail_ = update_diff_only(agency_db[stock_code]['agency_detail'], agency_volume_detail)
                agency_db.update({
                    stock_code: {'agency_meta': agency_volume_meta_,
                                 'agency_detail': agency_volume_detail_}
                })

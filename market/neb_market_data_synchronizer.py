from common.autowired import autowired, Service
from common.db import execute
from common.util.time import DateTime
from nr.nr_states import NRState
from .market_data_synchronizer import MarketDataSynchronizer


@Service()
class NebMarketDataSynchronizer(MarketDataSynchronizer):
    _nr_state: NRState = autowired(NRState)

    def get_last_sync_date(self) -> int:
        return self._nr_state.neb_market_last_sync_date

    def set_last_sync_date(self, date: int):
        self._nr_state.neb_market_last_sync_date = date

    def url_with_date(self, date: str) -> str:
        end = DateTime(timezone_hours=0).date.to_str('%Y%m%d')
        return 'https://coinmarketcap.com/zh/currencies/nebulas-token/historical-data/?start=%s&end=%s' % (date, end)

    def save_market_data(self, data: list):
        sql = ''
        for item in data:
            sql = 'INSERT INTO neb_market_value (`date`, `opening`, `closing`, `highest`, `lowest`, `amount`, `total_circulation`, `total`) ' \
                        'VALUES (%s, %s, %s, %s, %s, \'%s\', \'%s\', \'%s\');' % \
                  (str(item['date']), str(item['opening']), str(item['closing']), str(item['highest']), str(item['lowest']), str(item['amount']), str(item['total_circulation']), str(item['total']))
            execute(sql)

    def currency_count(self) -> int:
        return 100000000

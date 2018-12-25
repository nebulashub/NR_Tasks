from common.autowired import Service, autowired
from nr.nr_data_synchronizer import NrDataSynchronizer
from nr.nr_states import NRState


@Service()
class EthNrDataSynchronizer(NrDataSynchronizer):
    _nr_state = autowired(NRState)

    def get_current_sync_date(self) -> int:
        return self._nr_state.eth_current_sync_date

    def get_last_sync_date(self) -> int:
        return self._nr_state.eth_last_sync_date

    def set_last_sync_date(self, date: int):
        self._nr_state.eth_last_sync_date = date

    def is_neb(self) -> bool:
        return False

    def db_context(self):
        return 'eth_nr_by_date', 'eth_nr_total', 'eth_nr_by_addr'

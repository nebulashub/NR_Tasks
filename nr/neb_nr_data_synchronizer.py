from common.autowired import autowired, Service
from nr.nr_data_synchronizer import NrDataSynchronizer
from nr.nr_states import NRState


@Service()
class NebNrDataSynchronizer(NrDataSynchronizer):
    _nr_state = autowired(NRState)

    def get_current_sync_date(self) -> int:
        return self._nr_state.neb_current_sync_date

    def get_last_sync_date(self) -> int:
        return self._nr_state.neb_last_sync_date

    def set_last_sync_date(self, date: int):
        self._nr_state.neb_last_sync_date = date

    def is_neb(self) -> bool:
        return True

    def db_context(self):
        return 'neb_nr_by_date', 'neb_nr_total', 'neb_nr_by_addr'

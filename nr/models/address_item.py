class AddressItem:
    __slots__ = ['dates', 'count', 'total_nr']

    def __init__(self, dates, count, total_nr):
        self.dates = dates
        self.count = count
        self.total_nr = total_nr

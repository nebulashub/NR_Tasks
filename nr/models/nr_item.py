class NrItem:
    __slots__ = ['address', 'in_outs', 'out_val', 'in_val',
                 'degrees', 'out_degree', 'in_degree',
                 'weight', 'median', 'score', 'date', 'order']

    def __init__(self, nr):
        self.address = nr['address']
        self.in_outs = str(nr['in_outs'])
        self.out_val = str(nr['out_val'])
        self.in_val = str(nr['in_val'])
        self.degrees = str(nr['degrees'])
        self.out_degree = str(nr['out_degree'])
        self.in_degree = str(nr['in_degree'])
        self.weight = str(nr['weight'])
        self.median = str(nr['median'])
        self.score = str(nr['score'])
        self.date = str(nr['date'])
        # self.order = nr['order']

class NRLocalMemCache(object):
    _data = dict()

    @classmethod
    def get(cls, key):
        if key in cls._data.keys():
            return cls._data[key]
        else:
            return None

    @classmethod
    def set(cls, key, value):
        if value is None:
            if key in cls._data.keys():
                cls._data.pop(key)
        else:
            cls._data[key] = value

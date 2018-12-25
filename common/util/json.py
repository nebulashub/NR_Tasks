# coding:utf-8
import json


class JsonUtil(object):

    @staticmethod
    def serialize(obj):
        return json.dumps(JsonUtil._to_dict(obj))

    @staticmethod
    def deserialize(json_data):
        return json.loads(json_data)

    @staticmethod
    def _to_dict(data):
        r = data
        if data is None:
            r = None
        elif isinstance(data, int) or isinstance(data, str) or isinstance(data, float) or isinstance(data, bool):
            r = data
        elif isinstance(data, list) or isinstance(data, tuple) or isinstance(data, set):
            r = list()
            for i in data:
                r.append(JsonUtil._to_dict(i))
        elif isinstance(data, dict):
            r = dict()
            for k in data.keys():
                r[k] = JsonUtil._to_dict(data[k])
        else:
            r = JsonUtil._to_dict(data.__dict__)
        return r

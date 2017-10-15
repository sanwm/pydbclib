import json


class ObjEncoder(json.JSONEncoder):
    def default(self, obj):
        return obj.__repr__()
        # if not isinstance(obj, (list)):
        #     return obj.__repr__()
        # return json.JSONEncoder.default(self, obj)


def reduce_num(n, l):
    num = min(n, l)
    if num > 10:
        return num % 10
    else:
        return 1

import re

class Parse(object):

    def __init__(self, sql):
        self.sql = sql
        self.param_names = []
        self.repl = self.replace()
        self.match_param = re.compile(r":\w+")

    def replace(self):
        def wrapper(matched):
            nonlocal i
            i = i + 1
            param_name = "p%s" % i
            self.param_names.append(param_name)
            return ":%s" % param_name
        i = 0
        return wrapper

    def handle(self):
        return self.match_param.sub(self.repl, self.sql)

def handle(sql):
    parser = Parse(sql)
    sql = parser.handle()
    keys = parser.param_names
    return sql, keys


if __name__ == "__main__":
    h = Parse("select :123,:321,:123 from dual where a=:a")
    sql = h.handle()
    print(sql, h.param_names)

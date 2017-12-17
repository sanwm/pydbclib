import re


class Parse(object):

    def __init__(self, sql):
        self.sql = sql
        self.param_names = []
        self.match_param = re.compile(r":(?P<value>\w+)")

    def pattern(self, repl):
        def wrapper1(matched):
            nonlocal i
            i = i + 1
            param_name = "p%s" % i
            self.param_names.append(param_name)
            return ":%s" % param_name
        def wrapper2(matched):
            param_name = matched.group("value")
            self.param_names.append(param_name)
            return repl
        i = 0
        if repl is None:
            return wrapper1
        else:
            return wrapper2

    def handle(self, repl):
        self.repl = self.pattern(repl)
        return self.match_param.sub(self.repl, self.sql)


def handle(sql, repl=None):
    parser = Parse(sql)
    sql = parser.handle(repl)
    keys = parser.param_names
    return sql, keys


if __name__ == "__main__":
    sql, param_names = handle("select :123,:321,:123 from dual where a=:a")
    print(sql, param_names)
    sql, param_names = handle("select :123,:321,:123 from dual where a=:a", '?')
    print(sql, param_names)

import logging
console = logging.StreamHandler()
formatter = logging.Formatter(
    # '%(asctime)s %(levelname)s %(module)s/%(name)s: %(message)s'
    '<%(asctime)s %(levelname)s %(name)s> : %(message)s',
    '%Y-%m-%d %H:%M:%S'
)
console.setFormatter(formatter)

_debug = {
    True: logging.DEBUG,
    False: logging.INFO,
    None: logging.INFO
}


def instance_log(instance, debug=None):
    if debug:
        name = "[debug on] %s.%s" % (instance.__module__,
                                     instance.__class__.__name__)
    else:
        name = "%s.%s" % (instance.__module__,
                          instance.__class__.__name__)
    log = logging.getLogger(name)
    log.addHandler(console)
    log.setLevel(_debug[debug])
    instance.log = log


if __name__ == '__main__':
    print('hello world')
    log = logging.getLogger('test')
    log.addHandler(console)
    log.setLevel(_debug[True])
    log.info('hello world')

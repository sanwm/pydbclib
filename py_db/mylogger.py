import logging
console = logging.StreamHandler()
formatter = logging.Formatter(
    # '%(asctime)s %(levelname)s %(module)s/%(name)s: %(message)s'
    '<%(asctime)s %(levelname)s %(name)s.%(module)s> %(message)s'
)
console.setFormatter(formatter)
log = logging.getLogger("py_db")
log.addHandler(console)
log.setLevel(logging.INFO)


def print(*args, notice=''):
    log.debug('%s\n%s' % (notice, ' '.join(['%s' % i for i in args])))


if __name__ == '__main__':
    log.setLevel(logging.DEBUG)
    print('hello world')
    log.info('hello world')

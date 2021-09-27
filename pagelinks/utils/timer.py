import time


class Timer:
    def __init__(self):
        self.funcs = {}

    def timeit(self, f):
        """Decorator to time a function."""
        def timed(*args, **kw):

            ts = time.time()
            result = f(*args, **kw)
            te = time.time()

            fname = f.__name__
            self.funcs[fname] = self.funcs.setdefault(fname, 0) + te-ts

            print('func:%r args:[%r, %r] took: %2.4f sec' %
                  (f.__name__, args, kw, te-ts))
            return result

        return timed

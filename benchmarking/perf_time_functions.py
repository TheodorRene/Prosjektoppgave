from time import perf_counter

repeats = 10

def time_func_avg(func):
    def wrapFunc(*args, **kwargs):
        sum = 0
        for _ in range(repeats):
            start = perf_counter()
            func(*args, **kwargs)
            stop = perf_counter()
            sum += (stop-start)
        print(f"avg, {func.__name__}, {str(sum/repeats)}, {args}")
    return wrapFunc

def time_func(func):
    def wrapFunc(*args, **kwargs):
        start = perf_counter()
        res = func(*args, **kwargs)
        stop = perf_counter()
        print("one-shot," + func.__name__ + "," + str(stop-start))
        return res
    return wrapFunc


@time_func_avg
def gurba(hei):
    return [hei for _ in range(1000)]

def gurba2(hei):
    return [hei for _ in range(1000)]

#gurba("hei")

#time_func(gurba2)("hei")
#time_func_avg(gurba2)("hei")




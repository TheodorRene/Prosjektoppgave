from revision import WikiPagelinkSet
from utils.timer import timeit

@timeit
def __main__():
    start_page = WikiPagelinkSet("Erna Solberg", 1)
    pages = [start_page]
    pages_seen = []

    while pages:
        next_page = pages.pop()
        pages_seen.append(next_page.topic)

        new_pages = [page for page in next_page.propagate() if page.topic not in pages_seen]
        pages.extend(new_pages)
    

if __name__ == "__main__":
    __main__()
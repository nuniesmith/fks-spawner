import os

# One image, three programs: the search (a job), the forward runner and the
# signal runner (paper bots), so the code that trades is the code that searched.
runner = os.environ.get("EDGE_RUNNER", "").strip().lower()
if runner == "forward":
    from .forward import run
elif runner == "signal":
    from .live import run
else:
    from .core import run

run()

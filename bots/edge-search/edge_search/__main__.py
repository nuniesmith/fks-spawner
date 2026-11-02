import os

# One image, two programs: the search (a job) and the forward runner (a paper
# bot), so the code that trades forward is the code that searched.
if os.environ.get("EDGE_RUNNER", "").strip().lower() == "forward":
    from .forward import run
else:
    from .core import run

run()

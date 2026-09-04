# How the jobs reload stopped blocking the event loop

> Scratch/learning note — untracked, safe to delete. Explains PR #557 and the
> async/await concepts behind it.

---

## 1. The one idea you must hold in your head

**asyncio runs everything on a single thread.** There is no parallelism by
default. Instead there's an **event loop**: a scheduler that runs one piece of
code at a time and switches between tasks *only when a task voluntarily pauses*.

A task pauses by hitting an **`await`** on something that isn't ready yet
(a network read, a timer, a thread finishing). At that instant the task says
"I'm waiting — loop, go run something else." The loop then runs other tasks.
When the awaited thing is ready, the loop resumes the paused task.

This is **cooperative** multitasking. The catch:

> Between two `await`s, your code runs to completion with **zero interruption**.
> If you do 6 seconds of synchronous work between awaits, the loop is frozen for
> 6 seconds. Every other task — the web UI, the scheduler, websockets — is stuck.

That frozen window is what "blocking the event loop" means. The fix is never
"go faster"; it's "insert `await`s so the loop can breathe."

---

## 2. The vocabulary (with the exact functions in our code)

### `async def` → a coroutine
```python
async def load_job(path, job_id):
    ...
```
Calling `load_job(...)` does **not** run it. It returns a *coroutine object* —
a paused recipe. It only makes progress when something `await`s it (or the loop
drives it). This distinction is the whole bug in the old code, as you'll see.

### `await` → "pause me here; loop, run others until this is ready"
```python
content = await file.read()        # pause until the file bytes arrive
job = await asyncio.to_thread(...)  # pause until the worker thread finishes
```
`await` is the *only* place the loop can switch tasks inside a coroutine.

### `asyncio.to_thread(fn)` → run a blocking function on a worker thread
```python
job = await asyncio.to_thread(_parse)
```
`_parse()` is ordinary blocking CPU work (parse YAML). Running it directly would
freeze the loop for the parse duration. `to_thread` hands it to a background
thread and gives you something to `await`. While the thread works, **the loop is
free** to run other tasks. This is how CPU work "yields."

### `list_yaml_files(...)` → a generator (lazy!)
```python
def list_yaml_files(dir_path):
    for dir, dirs, names in os.walk(dir_path):   # <- the slow, NFS part
        ...
        for path in paths:
            yield path, job_id      # <- produces ONE item, then pauses
```
Because it uses `yield`, calling it runs **none** of the body immediately. Each
step of the `os.walk` only happens when you ask for the *next* item. Whether it
runs "all at once" or "a bit at a time" depends entirely on **how you consume
it** — that's the crux.

### `asyncio.sleep(0)` → "yield the loop right now, then continue"
A no-op pause that just lets the loop run other tasks once. (We don't need it in
the final #557 code, because `await load_job(...)` already yields — but it's the
simplest possible "be nice to the loop" tool.)

---

## 3. The bug (what `main` did)

```python
# main
load_coros = [load_job(path, job_id) for path, job_id in list_yaml_files(jobs_path)]
for chunk in itr.chunks(load_coros, 100):
    results = await asyncio.gather(*chunk)
    ...
```

Look at the list comprehension `[... for ... in list_yaml_files(...)]`. To build
that list, Python must pull **every** item out of the generator **right now** —
which runs the **entire `os.walk`** (every directory, over slow NFS) start to
finish, synchronously, **before the first `await` on the next line.**

So the timeline is:

```
   [======= whole os.walk, 6-9s, NO awaits =======]  await gather ...
   ^ loop is frozen this entire time ^
```

The `to_thread` parsing *was* already fine — but it never gets a chance to help,
because the loop is stuck building the list before any awaiting starts.

> Key insight: a lazy generator doesn't save you if you immediately force it into
> a list. The laziness is destroyed by the `[ ... ]`.

---

## 4. The fix (PR #557)

```python
# fix/reload-event-loop
for path, job_id in list_yaml_files(jobs_path):   # consume ONE item at a time
    _, job, exc = await load_job(path, job_id)     # <- await = loop can run others
    if job is not None:
        jobs[job_id] = job
    if exc is not None:
        errors.append(exc)
```

Now the generator is consumed **lazily**: the loop body asks for one `(path,
job_id)`, which advances `os.walk` by just one small step, then immediately hits
`await load_job(...)`. That `await` (the file read + `to_thread` parse) hands
control back to the loop. Repeat per file.

```
walk step ─ await(file+parse) ─ walk step ─ await(file+parse) ─ ...
            ^loop runs others^              ^loop runs others^
```

The 6-9s of total work is now sliced into tiny pieces separated by `await`s, so
between every file the UI/scheduler/websockets get to run. **Nothing got faster
— the loop just stopped being hogged.**

Everything else (`_parse`, `to_thread`, the ruamel loader, error handling) is
byte-for-byte what `main` had. The *only* change is "drain into a list up front"
→ "iterate one at a time."

---

## 5. Side-by-side diagram

```
OLD (main): eager — one giant frozen block, then awaits
────────────────────────────────────────────────────────────────────────
event loop │XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX│   .   .   .   .   .   .
           │      os.walk (6-9s)          │ await await await ...
           └── UI/scheduler frozen here ──┘
                                            ↑ they finally get to run

NEW (#557): lazy — walk sliced between awaits
────────────────────────────────────────────────────────────────────────
event loop │X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│X│.│
            │ │ │ │ │ │ ...
            │ └─ await (file read + threaded parse): loop runs UI/etc.
            └─── one os.walk step (tiny)

X = synchronous work on the loop thread (a single walk step — milliseconds)
. = an await: control returns to the loop; other tasks run
```

The old bar is one long `X`. The new one is many tiny `X`s with `.` gaps — same
total `X`, but now interruptible.

---

## 6. Function-by-function summary

| Function | Kind | Role | Why it matters for the loop |
|---|---|---|---|
| `list_yaml_files(dir)` | generator | walks the dir, `yield`s `(path, job_id)` | Lazy by nature; safe only if consumed lazily |
| `load_job(path, id)` | `async def` (coroutine) | read + parse one job file | Contains the `await`s that yield the loop |
| `_parse()` | plain function | the blocking YAML parse | Must run in `to_thread` so it doesn't freeze the loop |
| `asyncio.to_thread(_parse)` | awaitable | runs `_parse` on a worker thread | Loop is free while the thread parses |
| `load_jobs_dir(path)` | `async def` (coroutine) | driver: loop over files, collect jobs/errors | The fix lives here: lazy `for` instead of eager list |
| `asyncio.sleep(0)` | awaitable | explicit "yield now" | Simplest way to hand the loop a turn (used in the check loop) |

---

## 7. The mental checklist for "will this block the loop?"

1. Find the longest stretch of code between two `await`s.
2. Is that stretch doing heavy CPU work or slow synchronous I/O (like `os.walk`
   over NFS, or a big YAML parse)?
3. If yes → it blocks. Fix by either:
   - inserting `await`s so it runs in slices (what #557 does with lazy iteration), or
   - moving the heavy call into `await asyncio.to_thread(...)`.
4. Beware anything that secretly consumes a generator eagerly: `list(...)`,
   `[... for ...]`, `sorted(...)`, `"".join(...)`, `asyncio.gather(*[...])`.
   Those run the whole generator *now*, on the loop thread.

That step-4 trap is exactly what bit the old code.

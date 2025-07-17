import logging
import time

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------


class Timer:
    def __init__(self, name="timer", print=None):
        self.__name = str(name)
        self.__print = print
        self.__start = None
        self.__elapsed = None

    def __enter__(self):
        self.__start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.__elapsed = time.perf_counter() - self.__start
        if self.__print is not None:
            self.__print(f"{self.__name} elapsed: {self.__elapsed:.6f} s")

    @property
    def elapsed(self):
        return time.perf_counter() - self.__start if self.__elapsed is None else self.__elapsed


class LogSlow(Timer):
    """
    Context manager that logs if the context is slow to run.
    """

    def __init__(self, name, min_elapsed, *, level=logging.WARN):
        """
        :param min_elapsed:
          Log if context takes longer than this.
        """

        def print(msg):
            if self.elapsed > min_elapsed:
                logging.log(level, msg)

        super().__init__(name, print)

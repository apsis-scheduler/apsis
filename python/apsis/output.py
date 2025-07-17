import logging

from .program import Output

log = logging.getLogger(__name__)

# -------------------------------------------------------------------------------

# Implementation notes: We call `write()` only on _running_ runs, as many times
# as necessary; this caches the output to memory only.  We assume the running
# program can retrieve the entire output if necessaary.  Once the run
# terminates, a single call to `write_through()` commits the output to the
# database.


class OutputStore:
    """
    In-memory cache of outputs, backed by persistent output database.
    """

    def __init__(self, output_db):
        self.__outputs = {}
        self.__output_db = output_db

    def write(self, run_id: str, output_id: str, output: Output):
        self.__outputs.setdefault(run_id, {})[output_id] = output

    def write_through(self, run_id: str, output_id: str, output: Output):
        # Remove from the cache.
        try:
            del (outputs := self.__outputs[run_id])[output_id]
        except KeyError:
            pass
        else:
            if len(outputs) == 0:
                del self.__outputs[run_id]

        # Write to the DB.
        self.__output_db.upsert(run_id, output_id, output)

    def get_metadata(self, run_id):
        try:
            # Check cache first.
            outputs = self.__outputs[run_id]
            # Return just metadatas, not full outputs.
            return {i: o.metadata for i, o in outputs.items()}
        except KeyError:
            # Not in cache; go to database.
            return self.__output_db.get_metadata(run_id)

    def get_output(self, run_id, output_id) -> Output:
        try:
            return self.__outputs[run_id][output_id]
        except KeyError:
            return self.__output_db.get_output(run_id, output_id)

    def get_stats(self) -> dict:
        assert all(len(o) > 0 for o in self.__outputs.values())
        num = sum(len(o) for o in self.__outputs.values())
        return {
            "num_cached": num,
        }

"""
Common exception types.
"""

# -------------------------------------------------------------------------------


class SchemaError(ValueError):
    """
    Incorrect, missing, or extraneous values in a JSON or other representation.
    """

    pass


class JobError(Exception):
    def __init__(self, job_id, msg):
        super().__init__(msg)
        self.job_id = job_id


class JobsDirErrors(Exception):
    """
    One or more exceptions when loading jobs from a jobs dir.
    """

    def __init__(self, msg, errors):
        super().__init__(msg)
        self.errors = tuple(errors)

    def format(self):
        for error in self.errors:
            yield f"{error.job_id}: {error}"


class ConditionError(RuntimeError):
    """
    An error in specification of a condition.
    """

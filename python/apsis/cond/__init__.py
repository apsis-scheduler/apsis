from .base import Condition, ConstantCondition
from .dependency import Dependency
from .max_running import MaxRunning
from .skip_duplicate import SkipDuplicate

Condition.TYPE_NAMES.set(ConstantCondition, "const")
Condition.TYPE_NAMES.set(Dependency, "dependency")
Condition.TYPE_NAMES.set(MaxRunning, "max_running")
Condition.TYPE_NAMES.set(SkipDuplicate, "skip_duplicate")

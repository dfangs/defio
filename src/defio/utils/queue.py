from enum import Enum, auto, unique
from typing import Generic, TypeVar, final

from attrs import define, field


@unique
class QueueSignal(Enum):
    """
    Temporary workaround for signaling the queue consumers that
    there are no work items left from the queue producers.

    NOTE: Python has yet to implement `Queue.shutdown()`
    (see https://discuss.python.org/t/queue-termination/18386)
    """

    DONE = auto()


_T = TypeVar("_T")


@final
@define(order=True)
class PrioritizedItem(Generic[_T]):
    """
    Helper class that allows non-comparable items to be used in priority queues.

    Reference: https://docs.python.org/3/library/queue.html#queue.PriorityQueue
    """

    priority: float
    item: _T = field(order=False)

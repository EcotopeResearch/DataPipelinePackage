_NUMPY_MIN = (1, 24, 1)
_NUMPY_BELOW = (1, 25)


def _check_numpy_version():
    """Fail early and legibly when numpy has drifted out of the supported range.

    pandas 1.5.x is compiled against the numpy 1.x ABI. Importing it under
    numpy 2.x raises ``ValueError: numpy.dtype size changed`` from inside
    pandas, which gives no indication of the real problem. numpy on its own
    imports cleanly under any version, so checking it here -- before the
    submodule imports below pull in pandas -- turns that into a message that
    names the fix.
    """
    import numpy

    raw = numpy.__version__
    parts = []
    for piece in raw.split(".")[:3]:
        digits = ""
        for char in piece:
            if not char.isdigit():
                break
            digits += char
        if digits == "":
            return
        parts.append(int(digits))
    version = tuple(parts)

    if _NUMPY_MIN <= version < _NUMPY_BELOW:
        return

    raise ImportError(
        f"ecopipeline requires numpy >=1.24.1,<1.25 but found {raw}. "
        f"This environment has drifted from the pinned dependency set, most "
        f"likely because numpy was upgraded after ecopipeline was installed. "
        f"Reinstall with 'pip install --upgrade --force-reinstall ecopipeline' "
        f"to restore the supported versions, and run 'pip check' to see "
        f"whether anything else in the environment conflicts."
    )


_check_numpy_version()

from .utils.ConfigManager import ConfigManager
from . import extract
from . import transform
from . import event_tracking
from . import load
__all__ = ['extract', 'transform', 'event_tracking', 'load', 'ConfigManager']

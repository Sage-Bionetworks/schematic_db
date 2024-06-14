"""Types used by various modules"""

from typing import Literal


# "display_label": use the display name as a label, if it is valid.
# "class_label": use the class_label as a label
DisplayLabelType = Literal["class_label", "display_label"]

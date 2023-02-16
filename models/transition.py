from typing import Literal, Optional

import pandas as pd

from models.plant import Plant


class TransitionRegistry:
    def __init__(self):
        self.transitions = []

    def add(
        self,
        year: int,
        transition_type: Literal["retrofit", "new_build", "decommission"],
        origin: Optional[Plant] = None,
        destination: Optional[Plant] = None,
    ):
        transition = {
            "year": year,
            "transition_type": transition_type,
            "region": getattr(origin, "region", None) or destination.region,
            "chemical": getattr(origin, "chemical", None) or destination.chemical,
            "technology_origin": getattr(origin, "technology", None),
            "type_of_tech_origin": getattr(origin, "type_of_tech", None),
            "technology_destination": getattr(destination, "technology", None),
            "type_of_tech_destination": getattr(destination, "type_of_tech", None),
        }

        self.transitions.append(transition)

    def to_dataframe(self):
        return pd.DataFrame(self.transitions)

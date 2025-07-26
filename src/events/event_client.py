from datetime import datetime, timedelta
from src.utils.ccrfcd.ccrfcd_client import CCRFCDClient


class EventClient:

    def __init__(self):
        pass

    def fetch_event_level_data(self, start_time: datetime, end_time: datetime, interval: timedelta, timezone="UTC") -> dict:
        """
        **Timezone**: ``UTC``

        Returns
        ---
        ```python
        {
            "surface_dew_point": ...,
            "elevated_dew_point": ...,
            "theta_e": ...,
            "pwat": ...
            "tropical": bool
        }
        ```
        """

        pass
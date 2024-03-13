import requests
import os

class Polygon:
    """Handle requests to Polygon API.
    https://polygon.io/

    * Export POLYGON_KEY=YOUR_KEY as env variable
    """

    def __init__(self, api_calls_per_min: int = 5):
        """Initialize cretencials and settings."""

        api_key = os.getenv("POLYGON_KEY")
        self.api_key_url = f"&apiKey={api_key}"
        self.api_calls_per_min = api_calls_per_min
        self.base_url = "https://api.polygon.io/"
        self.grouped_daily_endpoint = "v2/aggs/grouped/locale/us/market/stocks/"

    def get_grouped_daily(self,date: str, adjusted: bool = True) -> dict:
        """Return the daily open, high, low, and close (OHLC) 
        for the entire stocks/equities markets.

        -- date format yyyy-mm-dd
        """

        adjusted_query = "?adjusted=true" if adjusted else "?adjusted=false"
        url = (
            f"{self.base_url}"
            f"{self.grouped_daily_endpoint}"
            f"{date}"
            f"{adjusted_query}"
            f"{self.api_key_url}"
            )
        
        print(url)
        self.resp = requests.get(url)
        return pl.resp.json()

if __name__ == "__main__":
    pl = Polygon()
    print(pl.base_url,pl.apikey)
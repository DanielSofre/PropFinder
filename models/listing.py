from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Listing:
    """
    Represents a single real estate property listing scraped from a portal.

    Attributes
    ----------
    source        : origin site identifier ('zonaprop' | 'argenprop')
    title         : raw title text from the listing
    price_usd     : asking price in US Dollars
    surface_m2    : total covered surface in square metres
    rooms         : number of rooms (ambientes)
    neighborhood  : neighbourhood name in Buenos Aires
    url           : canonical URL of the listing (used as deduplication key)
    price_m2      : computed price per m² (set automatically in __post_init__)
    first_seen    : timestamp when the listing was first recorded
    last_seen     : timestamp of the most recent scrape that found this listing
    id            : database primary key (None until persisted)
    """

    source: str
    title: str
    price_usd: float
    surface_m2: float
    rooms: int
    neighborhood: str
    url: str
    condition: str = ""

    # Derived / DB fields
    price_m2: float = field(init=False)
    first_seen: datetime = field(default_factory=datetime.now)
    last_seen: datetime = field(default_factory=datetime.now)
    id: Optional[int] = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def __post_init__(self) -> None:
        self.price_m2 = (
            round(self.price_usd / self.surface_m2, 2) if self.surface_m2 > 0 else 0.0
        )

    # ------------------------------------------------------------------ #
    # Validation                                                           #
    # ------------------------------------------------------------------ #

    def is_valid(self) -> bool:
        """Return True only when all required fields contain sensible values."""
        return (
            self.price_usd > 0
            and self.surface_m2 > 0
            and self.rooms > 0
            and bool(self.neighborhood and self.neighborhood.strip())
            and bool(self.url and self.url.strip())
        )

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict:
        """Return a plain dictionary representation."""
        return {
            "id": self.id,
            "source": self.source,
            "title": self.title,
            "price_usd": self.price_usd,
            "surface_m2": self.surface_m2,
            "rooms": self.rooms,
            "neighborhood": self.neighborhood,
            "condition": self.condition,
            "price_m2": self.price_m2,
            "url": self.url,
            "first_seen": self.first_seen.isoformat() if self.first_seen else None,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
        }

    def __str__(self) -> str:
        return (
            f"[{self.source.upper()}] {self.title}\n"
            f"  Neighbourhood : {self.neighborhood}\n"
            f"  Rooms         : {self.rooms}  |  Surface : {self.surface_m2} m²\n"
            f"  Price         : USD {self.price_usd:,.0f}  |  Per m² : USD {self.price_m2:,.0f}\n"
            f"  URL           : {self.url}"
        )

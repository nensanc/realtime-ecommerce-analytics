"""
Static reference data for the transaction generator.

This module builds:
  - A product catalog (100 products across 5 categories)
  - A pool of users (1000 simulated customers)

Both are generated once at startup with a fixed seed so runs are
reproducible — re-running the generator yields the same catalog,
which makes debugging streaming bugs tractable.

In a real system this data would live in databases; here we
generate it in memory because the focus is the streaming layer.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, asdict
from typing import Any

from faker import Faker


# ---------------------------------------------------------------------
# Reproducibility: same seed → same catalog and users every run
# ---------------------------------------------------------------------
SEED = 42
Faker.seed(SEED)
random.seed(SEED)


# ---------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class Product:
    """A single product in the catalog."""
    product_id: int
    product_name: str
    category: str
    unit_price: float
    initial_stock: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Location:
    """A geographic location for a user."""
    country: str
    city: str
    latitude: float
    longitude: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class User:
    """A simulated customer."""
    user_id: int
    user_name: str
    location: Location

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d


# ---------------------------------------------------------------------
# Category-aware product templates
# ---------------------------------------------------------------------
# Each category has a list of (product_name_template, price_range).
# Generator picks one template per product and varies the price.
CATEGORY_TEMPLATES: dict[str, list[tuple[str, tuple[float, float]]]] = {
    "Electronics": [
        ("Laptop {model}",        (600.0, 2000.0)),
        ("Smartphone {model}",    (200.0, 1500.0)),
        ("Wireless Headphones",   (30.0,  400.0)),
        ("Smart Watch {model}",   (100.0, 800.0)),
        ("Tablet {model}",        (150.0, 1200.0)),
        ("Bluetooth Speaker",     (25.0,  300.0)),
    ],
    "Clothing": [
        ("Cotton T-Shirt",        (10.0,  40.0)),
        ("Denim Jeans",           (30.0,  120.0)),
        ("Running Shoes",         (50.0,  200.0)),
        ("Winter Jacket",         (80.0,  350.0)),
        ("Casual Sneakers",       (40.0,  180.0)),
    ],
    "Books": [
        ("Programming Guide",     (15.0,  60.0)),
        ("Novel",                 (8.0,   30.0)),
        ("Cookbook",              (12.0,  45.0)),
        ("Biography",             (10.0,  40.0)),
        ("Science Textbook",      (40.0,  150.0)),
    ],
    "Home": [
        ("Coffee Maker",          (40.0,  300.0)),
        ("Vacuum Cleaner",        (80.0,  500.0)),
        ("LED Desk Lamp",         (20.0,  100.0)),
        ("Bedding Set",           (50.0,  250.0)),
        ("Air Purifier",          (100.0, 600.0)),
    ],
    "Sports": [
        ("Yoga Mat",              (15.0,  80.0)),
        ("Dumbbell Set",          (40.0,  300.0)),
        ("Mountain Bike",         (300.0, 2000.0)),
        ("Tennis Racket",         (50.0,  400.0)),
        ("Football",              (15.0,  80.0)),
    ],
}

CATEGORIES = list(CATEGORY_TEMPLATES.keys())


# ---------------------------------------------------------------------
# Colombian cities (weighted toward major urban centers)
# ---------------------------------------------------------------------
COLOMBIAN_CITIES: list[tuple[str, float, float]] = [
    ("Bogotá",      4.7110, -74.0721),
    ("Medellín",    6.2442, -75.5812),
    ("Cali",        3.4516, -76.5320),
    ("Barranquilla",10.9685, -74.7813),
    ("Cartagena",   10.3910, -75.4794),
    ("Bucaramanga", 7.1193, -73.1227),
    ("Rionegro",    6.1471, -75.3736),
    ("Pereira",     4.8133, -75.6961),
    ("Manizales",   5.0703, -75.5138),
    ("Santa Marta", 11.2408, -74.1990),
]

# International cities to add geographic diversity (~30% of users)
INTERNATIONAL_CITIES: list[tuple[str, str, float, float]] = [
    ("USA",       "New York",     40.7128, -74.0060),
    ("USA",       "Los Angeles",  34.0522, -118.2437),
    ("Mexico",    "Mexico City",  19.4326, -99.1332),
    ("Argentina", "Buenos Aires", -34.6037, -58.3816),
    ("Spain",     "Madrid",       40.4168, -3.7038),
    ("Brazil",    "São Paulo",    -23.5505, -46.6333),
    ("Chile",     "Santiago",     -33.4489, -70.6693),
    ("Peru",      "Lima",         -12.0464, -77.0428),
]


# ---------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------
def build_product_catalog(n: int = 100) -> list[Product]:
    """
    Build a catalog of `n` products distributed roughly evenly
    across the 5 categories.
    """
    fake = Faker()
    products: list[Product] = []
    per_category = n // len(CATEGORIES)
    remainder = n % len(CATEGORIES)

    product_id = 1
    for i, category in enumerate(CATEGORIES):
        count = per_category + (1 if i < remainder else 0)
        templates = CATEGORY_TEMPLATES[category]

        for _ in range(count):
            name_template, (price_min, price_max) = random.choice(templates)
            # Some templates have a {model} placeholder — fill it
            model = fake.bothify(text="Pro-####").upper() if "{model}" in name_template else ""
            product_name = name_template.format(model=model).strip()

            products.append(
                Product(
                    product_id=product_id,
                    product_name=product_name,
                    category=category,
                    unit_price=round(random.uniform(price_min, price_max), 2),
                    initial_stock=random.randint(50, 500),
                )
            )
            product_id += 1

    return products


def build_user_pool(n: int = 1000, colombia_ratio: float = 0.70) -> list[User]:
    """
    Build a pool of `n` simulated users.

    Args:
        n: total number of users
        colombia_ratio: fraction of users located in Colombia (default 70%)
    """
    fake = Faker()
    users: list[User] = []
    n_colombia = int(n * colombia_ratio)
    n_international = n - n_colombia

    # Colombian users
    for i in range(1, n_colombia + 1):
        city, lat, lon = random.choice(COLOMBIAN_CITIES)
        users.append(
            User(
                user_id=i,
                user_name=fake.name(),
                location=Location(
                    country="Colombia",
                    city=city,
                    latitude=lat,
                    longitude=lon,
                ),
            )
        )

    # International users
    for i in range(n_colombia + 1, n_colombia + n_international + 1):
        country, city, lat, lon = random.choice(INTERNATIONAL_CITIES)
        users.append(
            User(
                user_id=i,
                user_name=fake.name(),
                location=Location(
                    country=country,
                    city=city,
                    latitude=lat,
                    longitude=lon,
                ),
            )
        )

    # Shuffle so user_ids and locations aren't sorted blocks
    random.shuffle(users)
    return users


# ---------------------------------------------------------------------
# Self-test: print a summary so you can eyeball the catalog
# ---------------------------------------------------------------------
if __name__ == "__main__":
    from collections import Counter

    print("=" * 60)
    print("PRODUCT CATALOG")
    print("=" * 60)
    products = build_product_catalog(n=100)
    print(f"Generated {len(products)} products\n")

    by_category = Counter(p.category for p in products)
    for cat, count in by_category.items():
        prices = [p.unit_price for p in products if p.category == cat]
        print(
            f"  {cat:12s} → {count:3d} products  "
            f"(prices ${min(prices):.2f} - ${max(prices):.2f})"
        )

    print("\nFirst 5 products:")
    for p in products[:5]:
        print(f"  [{p.product_id:3d}] {p.product_name:30s} "
              f"({p.category:11s}) ${p.unit_price:8.2f}  stock={p.initial_stock}")

    print("\n" + "=" * 60)
    print("USER POOL")
    print("=" * 60)
    users = build_user_pool(n=1000)
    print(f"Generated {len(users)} users\n")

    by_country = Counter(u.location.country for u in users)
    for country, count in by_country.most_common():
        pct = 100 * count / len(users)
        print(f"  {country:12s} → {count:4d} users  ({pct:.1f}%)")

    print("\nFirst 5 users:")
    for u in users[:5]:
        print(f"  [{u.user_id:4d}] {u.user_name:25s} "
              f"({u.location.city}, {u.location.country})")

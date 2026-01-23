import yaml
from dataclasses import dataclass

@dataclass
class Upstream:
    host: str
    port: int

@dataclass
class Timeouts:
    connect_ms: int
    read_ms: int
    write_ms: int
    total_ms: int

@dataclass
class Limits:
    max_client_conns: int
    max_conns_per_upstream: int

@dataclass
class Config:
    listen: str
    upstreams: list[Upstream]
    timeouts: Timeouts
    limits: Limits


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return Config(
        listen=raw["listen"],
        upstreams=[Upstream(**u) for u in raw["upstreams"]],
        timeouts=Timeouts(**raw["timeouts"]),
        limits=Limits(**raw["limits"]),
    )
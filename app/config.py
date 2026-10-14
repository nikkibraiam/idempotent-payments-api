import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    # A request holding a key longer than this is assumed dead (crashed
    # worker, killed pod) and another retry may take the key over.
    lock_timeout_seconds: float = 30.0
    pool_size: int = 10

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/payments"),
            lock_timeout_seconds=float(os.getenv("LOCK_TIMEOUT_SECONDS", "30")),
            pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
        )

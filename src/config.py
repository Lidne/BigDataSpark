from __future__ import annotations

from functools import lru_cache

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    db: str = "bigdata_lab"
    user: str = "app"
    password: str = "app"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


class ClickHouseSettings(BaseModel):
    host: str = "localhost"
    port: int = 8123
    db: str = "reports"
    user: str = "default"
    password: str = ""

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:clickhouse://{self.host}:{self.port}/{self.db}"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "bigdata_lab"
    pg_user: str = "app"
    pg_password: str = "app"

    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_db: str = "reports"
    ch_user: str = "default"
    ch_password: str = ""

    @property
    def postgres(self) -> PostgresSettings:
        return PostgresSettings(
            host=self.pg_host,
            port=self.pg_port,
            db=self.pg_db,
            user=self.pg_user,
            password=self.pg_password,
        )

    @property
    def clickhouse(self) -> ClickHouseSettings:
        return ClickHouseSettings(
            host=self.ch_host,
            port=self.ch_port,
            db=self.ch_db,
            user=self.ch_user,
            password=self.ch_password,
        )


def get_settings() -> Settings:
    return Settings()

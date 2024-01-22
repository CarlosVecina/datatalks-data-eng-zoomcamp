#!/usr/bin/env python
# coding: utf-8
import polars as pl
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import ClassVar

TRIPS_TABLE_NAME = "green_taxi_trips"
ZONES_TABLE_NAME = "zones"

class PgConn(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='PG_', env_file='.env', env_file_encoding='utf-8')

    user: str
    pwd: str
    host: str
    port: int
    db: str

    connector: ClassVar[str] = "postgresql"
    
    @property
    def uri(self):
        return f"{self.connector}://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.db}"


df_ny_taxi = pl.read_csv("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz")
df_zones = pl.read_csv("https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv")

conn = PgConn()

df_zones.write_database(ZONES_TABLE_NAME, conn.uri)
df_ny_taxi.write_database(TRIPS_TABLE_NAME, conn.uri)


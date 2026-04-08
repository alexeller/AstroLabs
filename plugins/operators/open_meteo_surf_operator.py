from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dataclass(frozen=True)
class GeoPoint:
    name: str
    latitude: float
    longitude: float


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "poi"


class OpenMeteoSurfDataToS3Operator(BaseOperator):
    """Fetch weather + wave-height data for a POI and store raw JSON in S3.

    Steps:
    1) Geocode `point_of_interest` to latitude/longitude (OpenStreetMap Nominatim).
    2) Call Open-Meteo forecast API for current temperature and wind speed.
    3) Call Open-Meteo marine API for hourly wave height.
    4) Upload a single combined JSON document to S3.

        Output key format (default):
            data/surf_data_raw/<poi_slug>/<run_date>.json

    Params:
      - point_of_interest: Free-text location name.
      - s3_bucket: Destination bucket.
      - s3_prefix: Folder/prefix under bucket (default: data/surf_data_raw).
      - aws_conn_id: Airflow connection id for AWS creds.
      - geocoding_country_code: Optional ISO country code to improve geocoding.
    """

    template_fields = (
        "point_of_interest",
        "s3_bucket",
        "s3_prefix",
        "aws_conn_id",
        "run_date",
        "geocoding_country_code",
    )

    def __init__(
        self,
        *,
        point_of_interest: str,
        s3_bucket: str,
        s3_prefix: str = "data/surf_data_raw",
        aws_conn_id: str = "astro_s3_conn",
        run_date: str | None = None,
        geocoding_country_code: str | None = "US",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.point_of_interest = point_of_interest
        self.run_date = run_date
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip("/")
        self.aws_conn_id = aws_conn_id
        self.geocoding_country_code = geocoding_country_code

    def _geocode(self) -> GeoPoint:
        queries = self._build_geocode_queries(self.point_of_interest)

        # Open-Meteo's geocoding endpoint frequently returns empty results for POI-style
        # strings (e.g. specific beaches). Nominatim is more reliable for this use-case.
        geopoint = self._geocode_nominatim(queries)
        if geopoint is not None:
            return geopoint

        raise AirflowException(
            f"No geocoding results for: {self.point_of_interest} (tried: {queries})"
        )

    def _build_geocode_queries(self, poi: str) -> list[str]:
        q0 = poi.strip()
        q1 = re.sub(r"\s*,\s*", " ", q0)
        q2 = re.sub(r"\s+", " ", q1)
        q3 = re.sub(r"\bRI\b", "Rhode Island", q2, flags=re.IGNORECASE)
        q4 = re.sub(r"\bMA\b", "Massachusetts", q3, flags=re.IGNORECASE)
        queries = [q0, q2, q4, f"{q4} United States"]
        return [q for i, q in enumerate(queries) if q and q not in queries[:i]]

    def _geocode_nominatim(self, queries: list[str]) -> GeoPoint | None:
        url = "https://nominatim.openstreetmap.org/search"

        headers = {
            # Nominatim requires a User-Agent.
            "User-Agent": "AstroLabs-Airflow/1.0",
        }

        for q in queries:
            params: dict[str, Any] = {
                "q": q,
                "format": "json",
                "limit": 1,
            }
            if self.geocoding_country_code:
                params["countrycodes"] = self.geocoding_country_code.lower()
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            results = r.json() or []
            if not results:
                continue

            top = results[0]
            try:
                lat = float(top["lat"])
                lon = float(top["lon"])
            except Exception as e:
                raise AirflowException(
                    f"Invalid Nominatim geocoding response for '{q}' ({self.point_of_interest}): {top}"
                ) from e

            name = top.get("display_name") or q or self.point_of_interest
            return GeoPoint(name=str(name), latitude=lat, longitude=lon)

        self.log.info(
            "Nominatim geocoding returned no results for '%s' (queries=%s)",
            self.point_of_interest,
            queries,
        )
        return None

    def _fetch_weather(self, lat: float, lon: float) -> dict[str, Any]:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,wind_speed_10m",
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _fetch_marine(self, lat: float, lon: float) -> dict[str, Any]:
        url = "https://marine-api.open-meteo.com/v1/marine"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "wave_height",
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def execute(self, context: dict[str, Any]) -> str:
        geopoint = self._geocode()
        self.log.info(
            "Geocoded '%s' -> (lat=%s, lon=%s)",
            self.point_of_interest,
            geopoint.latitude,
            geopoint.longitude,
        )

        weather = self._fetch_weather(geopoint.latitude, geopoint.longitude)
        marine = self._fetch_marine(geopoint.latitude, geopoint.longitude)

        run_date = (self.run_date or "").strip()
        if not run_date:
            logical_date = context.get("logical_date")
            if logical_date is None:
                logical_date = datetime.now(timezone.utc)
            if getattr(logical_date, "tzinfo", None) is None:
                logical_date = logical_date.replace(tzinfo=timezone.utc)
            run_date = logical_date.astimezone(timezone.utc).strftime("%Y%m%d%H")

        poi_slug = _slugify(self.point_of_interest)
        s3_key = f"{self.s3_prefix}/{poi_slug}/{run_date}.json.ready"

        combined = {
            "point_of_interest": self.point_of_interest,
            "resolved_name": geopoint.name,
            "latitude": geopoint.latitude,
            "longitude": geopoint.longitude,
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_date": run_date,
            "weather": weather,
            "marine": marine,
        }

        body = json.dumps(combined, ensure_ascii=False, indent=2).encode("utf-8")

        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        s3.load_bytes(
            bytes_data=body,
            key=s3_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )

        uri = f"s3://{self.s3_bucket}/{s3_key}"
        self.log.info("Wrote surf raw data to %s", uri)
        return uri

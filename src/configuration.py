import logging
from datetime import date
from enum import Enum
from typing import List

from pydantic import BaseModel, Field, ValidationError, field_validator
from keboola.component.exceptions import UserException


class GranularityEnum(str, Enum):
    day = "day"
    month = "month"


class Authentication(BaseModel):
    username: str
    password: str = Field(alias="#password")
    api_base_url: str = Field(
        default="https://webenergis.eu/test/1.wsc/soap",
        description="Energis API Base URL, default 'https://webenergis.eu/test/1.wsc/soap'"
    )

    @field_validator("username", "password")
    def must_not_be_empty(cls, value: str, info) -> str:
        if not value.strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty")
        return value

    @property
    def credentials(self) -> tuple[str, str]:
        return self.username, self.password


class SyncOptions(BaseModel):
    nodes: list[int] = Field(
        default=[],
        description="List of nodes to fetch, e.g. [7090001]"
    )
    date_from: str = Field(
        default="2020-01-01",
        description="Date from which to fetch data, default '2020-01-01'"
    )
    date_to: str = Field(
        default_factory=lambda: str(date.today()),
        description="Date to which to fetch data, default 'today'"
    )
    granularity: GranularityEnum = Field(
        default=GranularityEnum.day,
        description="Granularity of fetched data, default 'day'"
    )

    @field_validator("nodes")
    def must_not_be_empty(cls, values: List[int], info) -> List[int]:
        if len(values) == 0:
            raise ValueError(f"Field '{info.field_name}' cannot be empty")
        return values

    @field_validator("granularity")
    def validate_granularity(cls, value: GranularityEnum) -> GranularityEnum:
        if value not in GranularityEnum:
            raise ValueError(f"Invalid value '{value}' for 'granularity'. Must be 'day' or 'month'")
        return value


class Configuration(BaseModel):
    authentication: Authentication
    sync_options: SyncOptions
    debug: bool = False

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

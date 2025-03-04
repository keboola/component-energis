import logging
from datetime import date
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from keboola.component.exceptions import UserException


class EnvironmentEnum(str, Enum):
    dev = "dev"
    prod = "prod"


ENVIRONMENT_URLS = {
    EnvironmentEnum.dev: "https://webenergis.eu/test/1.wsc/soap.r",
    EnvironmentEnum.prod: "https://bilance.c-energy.cz/cgi-bin/1.wsc/soap.r"
}


class DatasetEnum(str, Enum):
    xexport = "xexport"
    xjournal = "xjournal"
    xparam = "xparam"
    xcorr = "xcorr"


DATASET_UNIQUE_FIELDS = {
    DatasetEnum.xexport: [
        "uzel",
        "cas"
    ],
    DatasetEnum.xjournal: [
        "uzel",
        "cas",
        "udalost",
        "faze"
    ]
}

DATASET_OUTPUT_FIELDS = {
    DatasetEnum.xexport: [
        "uzel",
        "hodnota",
        "cas"
    ],
    DatasetEnum.xjournal: [
        "uzel",
        "popisu",
        "cas",
        "udalost",
        "faze",
        "kp",
        "pozn",
        "inf"
    ]
}


class EventEnum(str, Enum):
    error = "error"
    warning = "warning"
    info = "info"


class PhaseEnum(str, Enum):
    init = "init"
    running = "running"
    complete = "complete"


class NodeTypeEnum(str, Enum):
    sensor = "sensor"
    meter = "meter"


class ParamTypeEnum(str, Enum):
    temperature = "temperature"
    power = "power"


class ValueTypeEnum(str, Enum):
    avg = "avg"
    sum = "sum"


class GranularityEnum(str, Enum):
    year = "year"
    quarterYear = "quarterYear"
    month = "month"
    day = "day"
    hour = "hour"
    quarterHour = "quarterHour"
    minute = "minute"


class Authentication(BaseModel):
    username: str
    password: str = Field(alias="#password")
    environment: EnvironmentEnum = Field(
        default=EnvironmentEnum.prod,
        description="Choose 'dev' for testing or 'prod' for production."
    )

    @field_validator("username", "password")
    def must_not_be_empty(cls, value: str, info) -> str:
        if not value.strip():
            raise ValueError(f"Field '{info.field_name}' cannot be empty")
        return value

    @property
    def credentials(self) -> tuple[str, str]:
        return self.username, self.password

    @property
    def api_base_url(self) -> str:
        """Returns the full API base URL based on the selected environment."""
        return ENVIRONMENT_URLS[self.environment]


class SyncOptions(BaseModel):
    dataset: DatasetEnum = Field(
        default=DatasetEnum.xexport,
        description="Source dataset for data extraction"
    )
    nodes: list[int] = Field(
        default=[],
        description="List of nodes to fetch, e.g. [7090001]"
    )
    incremental: bool = Field(
        default=False,
        description="If true, replace date_to by the current_date"
    )
    date_from: str = Field(
        default="2020-01-01",
        description="Date from which to fetch data, default '2020-01-01'"
    )
    date_to: Optional[str] = Field(
        default=None,
        description="Date to which to fetch data."
    )
    granularity: GranularityEnum = Field(
        default=GranularityEnum.day,
        description="Granularity of fetched data, default 'day'"
    )
    event_type: Optional[EventEnum] = Field(
        default=None,
        description="Event Type of 'error', 'warning', or 'info'"
    )
    phase: Optional[PhaseEnum] = Field(
        default=None,
        description="Phase Type of 'init', 'running', or 'complete'"
    )
    node_type: Optional[NodeTypeEnum] = Field(
        default=None,
        description="Node Type of 'sensor', or 'meter'"
    )
    param_type: Optional[ParamTypeEnum] = Field(
        default=None,
        description="Param Type of 'temperature', or 'power'"
    )
    value_type: Optional[ValueTypeEnum] = Field(
        default=None,
        description="Value Type of 'avg', or 'sum'"
    )

    @field_validator("nodes")
    def must_not_be_empty(cls, values: List[int], info) -> List[int]:
        if len(values) == 0:
            raise ValueError(f"Field '{info.field_name}' cannot be empty")
        return values

    @field_validator("granularity")
    def validate_granularity(cls, value: GranularityEnum) -> GranularityEnum:
        if value not in GranularityEnum:
            allowed_values = "', '".join([e.value for e in GranularityEnum])
            raise ValueError(f"Invalid value '{value}' for 'granularity'. Must be one of {allowed_values}")
        return value

    @model_validator(mode="after")
    def validate_and_set_date_to(self):
        """Ensures date_to is always set correctly based on incremental mode."""
        if self.incremental:
            self.date_to = str(date.today())  # Auto-set to today's date
        elif self.date_to is None:
            raise ValueError("date_to must be specified when incremental=False.")

        return self  # Required for model_validator

    @property
    def resolved_date_to(self) -> str:
        """Ensures date_to is always a string."""
        return self.date_to


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

import logging
from datetime import date
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError, field_validator
from keboola.component.exceptions import UserException
from state_manager import StateManager


class EnvironmentEnum(str, Enum):
    dev = "dev"
    prod = "prod"


ENVIRONMENT_URLS = {
    EnvironmentEnum.dev: "https://webenergis.eu/test/1.wsc/soap.r",
    EnvironmentEnum.prod: "https://bilance.c-energy.cz/cgi-bin/1.wsc/soap.r"
}


class DatasetEnum(str, Enum):
    xexport = "xexport"


DATASET_UNIQUE_FIELDS = {
    DatasetEnum.xexport: [
        "uzel",
        "cas"
    ]
}

DATASET_OUTPUT_FIELDS = {
    DatasetEnum.xexport: [
        "uzel",
        "hodnota",
        "cas"
    ]
}


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
    reload_full_data: bool = Field(
        default=False,
        description="When enabled, retrieves the complete dataset from 'date_from', bypassing incremental loading"
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

    @property
    def resolved_date_to(self) -> str:
        """Ensures date_to is always a string."""
        return self.date_to if self.date_to else str(date.today())


class Configuration(BaseModel):
    authentication: Authentication
    sync_options: SyncOptions
    debug: bool = False

    def __init__(self, state_manager: StateManager, **data):
        try:
            super().__init__(**data)

            last_processed_date = state_manager.get_last_processed_date()
            if last_processed_date and not self.sync_options.reload_full_data:
                self.sync_options.date_from = last_processed_date

            if not self.sync_options.date_to:
                self.sync_options.date_to = self.sync_options.resolved_date_to

            logging.info(f"Using date_from: {self.sync_options.date_from}")
            logging.info(f"Using date_to: {self.sync_options.date_to}")
            logging.info(f"Using granularity: {self.sync_options.granularity.value}")
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")

        if self.debug:
            logging.debug("Component will run in Debug mode")

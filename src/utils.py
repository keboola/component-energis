import re
from datetime import datetime, timedelta
from typing import Generator

from configuration import GranularityEnum

import logging


class MaskSensitiveDataFilter(logging.Filter):
    """
    A logging filter that masks sensitive fields in log messages.

    This filter scans log messages and replaces the values of specified XML fields
    with masked characters, ensuring that sensitive data is not logged.

    Attributes:
        fields_to_mask (list[str]): The list of XML tag names whose values should be masked.
        mask_char (str): The character used for masking sensitive values.
    """

    def __init__(self, fields_to_mask=None, mask_char="*"):
        """
        Initializes the filter with optional fields to mask and mask character.

        Args:
            fields_to_mask (list[str], optional): List of XML tag names to mask. Defaults to a predefined set.
            mask_char (str, optional): The character used for masking. Defaults to "*".
        """
        super().__init__()
        self.fields_to_mask = fields_to_mask or ["username", "password", "exuziv", "exklic"]
        self.mask_char = mask_char

    def filter(self, record):
        """
        Masks sensitive data in the log record message before it's logged.

        Args:
            record (logging.LogRecord): The log record object.

        Returns:
            bool: Always returns True to allow the log message to proceed.
        """
        if isinstance(record.msg, str):  # Ensure the message is a string before modifying
            record.msg = mask_sensitive_data_in_body(record.msg, self.fields_to_mask, self.mask_char)
        return True  # Allow the log message to pass through


def mask_sensitive_data_in_body(body: str, fields_to_mask: list[str] = None, mask_char: str = "*") -> str:
    """
    Masks sensitive fields in the SOAP XML body by replacing their values with masked characters.

    The function searches for XML tags that match the specified field names and replaces their
    contents with a masked version while preserving the first character (if available).

    Args:
        body (str): The raw SOAP XML body as a string.
        fields_to_mask (list[str], optional): The list of XML tag names to mask.
            Defaults to ["username", "password", "exuziv", "exklic"].
        mask_char (str, optional): The character to use for masking. Defaults to "*".

    Returns:
        str: The masked SOAP XML body with sensitive fields obfuscated.
    """
    if fields_to_mask is None:
        fields_to_mask = ["username", "password", "exuziv", "exklic"]

    for field in fields_to_mask:
        # Regex pattern to find <field>value</field> tags
        pattern = f"<{field}>(.*?)</{field}>"

        def mask_match(match: re.Match) -> str:
            value = match.group(1)
            if len(value) > 1:
                masked_value = f"{value[0].lower()}{mask_char * (len(value))}"
            else:
                masked_value = mask_char
            return f"<{field}>{masked_value}</{field}>"

        body = re.sub(pattern, mask_match, body, flags=re.IGNORECASE)

    return body


def generate_logon_request(username: str, password: str) -> tuple[str, dict[str, str]]:
    """
    Generates the SOAP request body and headers for the logonex operation.

    Args:
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        tuple[str, dict[str, str]]: The SOAP request body and headers.
    """
    soap_body = f"""
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
           soap:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"
           xmlns:ene="ENERGIS-URL">
        <soap:Body>
            <ene:logonex>
                <username>{username}</username>
                <password>{password}</password>
            </ene:logonex>
        </soap:Body>
    </soap:Envelope>
    """

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "logonex"
    }

    return soap_body, headers


def generate_xexport_request(
    username: str,
    key: str,
    nodes: list[int],
    date_from: str,
    date_to: str,
    granularity: str,
) -> tuple[str, dict[str, str]]:
    """
    Generates the SOAP request body and headers for the xexport operation.

    Args:
        username (str): The username for authentication.
        key (str): The authentication key.
        nodes (list[int]): List of node IDs to fetch data for.
        date_from (str): Start date in MMDDYYYYHHMM format.
        date_to (str): End date in MMDDYYYYHHMM format.
        granularity (str): The granularity of the data ('m' for month, 'd' for day).

    Returns:
        tuple[str, dict[str, str]]: The SOAP request body and headers.
    """
    nodes_str = ",".join(map(str, nodes))

    soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   soap:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"
                   xmlns:ene="ENERGIS-URL">
        <soap:Header>
            <ene:Auth>
                <exuziv>{username}</exuziv>
                <exklic>{key}</exklic>
            </ene:Auth>
        </soap:Header>
        <soap:Body>
            <ene:xexport>
                <uzel>{nodes_str}</uzel>
                <typuz>2</typuz>
                <per>{granularity}</per>
                <cas>{date_from},{date_to}</cas>
                <typhodn>hodnota</typhodn>
            </ene:xexport>
        </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "xexport"
    }

    return soap_body, headers


def granularity_to_short_code(granularity: GranularityEnum) -> str:
    """Returns a single-letter string based on the GranularityEnum value."""
    mapping = {
        GranularityEnum.year: "r",
        GranularityEnum.quarterYear: "v",
        GranularityEnum.month: "m",
        GranularityEnum.day: "d",
        GranularityEnum.hour: "h",
        GranularityEnum.quarterHour: "c",
        GranularityEnum.minute: "t"
    }

    return mapping.get(granularity)


def granularity_to_filename(granularity: GranularityEnum) -> str:
    """Returns a descriptive filename component based on the GranularityEnum value."""
    mapping = {
        GranularityEnum.year: "year",
        GranularityEnum.quarterYear: "quarter_year",
        GranularityEnum.month: "month",
        GranularityEnum.day: "day",
        GranularityEnum.hour: "hour",
        GranularityEnum.quarterHour: "quarter_hour",
        GranularityEnum.minute: "minute"
    }

    return mapping.get(granularity)


def convert_date_to_mmddyyyyhhmm(date_str: str) -> str:
    """
    Converts a date string from 'YYYY-MM-DD' format to 'MMDDYYYYHHMM'.

    Args:
        date_str (str): Date in 'YYYY-MM-DD' format.

    Returns:
        str: Converted date in 'MMDDYYYYHHMM' format.
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%m%d%Y0000")
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Expected format: YYYY-MM-DD")


def generate_periods(granularity: GranularityEnum, date_from: str, date_to: str) -> Generator[str, None, None]:
    """
    Generates a sequence of period strings based on the given granularity and date range.

    Args:
        granularity (GranularityEnum): The granularity level (year, month, day, etc.).
        date_from (str): The start date in "YYYY-MM-DD" format.
        date_to (str): The end date in "YYYY-MM-DD" format.

    Returns:
        Generator[str, None, None]: A generator yielding period strings in the format `r-1, r-2, ...` or `r`.
    """
    date_from = datetime.strptime(date_from, "%Y-%m-%d")
    date_to = datetime.strptime(date_to, "%Y-%m-%d")

    granularity_steps = {
        GranularityEnum.year: (timedelta(days=365), "r-{index}"),
        GranularityEnum.quarterYear: (timedelta(days=91), "v-{index}"),
        GranularityEnum.month: (timedelta(days=30), "m-{index}"),  # Approximate
        GranularityEnum.day: (timedelta(days=1), "d-{index}"),
        GranularityEnum.hour: (timedelta(hours=1), "h-{index}"),
        GranularityEnum.quarterHour: (timedelta(minutes=15), "c-{index}"),
        GranularityEnum.minute: (timedelta(minutes=1), "t-{index}"),
    }

    if granularity not in granularity_steps:
        raise ValueError(f"Unsupported granularity: {granularity}")

    step, format_str = granularity_steps[granularity]

    if granularity == GranularityEnum.month:
        start_date = date_from.replace(day=1)
        end_date = date_to.replace(day=1)
        months_diff = (end_date.year - start_date.year) * 12 + end_date.month - start_date.month

        for i in range(months_diff + 1):
            yield f"m-{months_diff - i}" if i < months_diff else "m"
        return  # Exit early

    if granularity == GranularityEnum.year:
        start_year = date_from.year
        end_year = date_to.year
        years_diff = end_year - start_year

        for i in range(years_diff + 1):
            yield f"r-{years_diff - i}" if i < years_diff else "r"
        return

    if granularity == GranularityEnum.quarterYear:
        start_quarter = (date_from.year * 4) + (date_from.month - 1) // 3
        end_quarter = (date_to.year * 4) + (date_to.month - 1) // 3
        quarters_diff = end_quarter - start_quarter

        for i in range(quarters_diff + 1):
            yield f"v-{quarters_diff - i}" if i < quarters_diff else "v"
        return

    current_date = date_from
    steps_count = (date_to - date_from) // step

    for i in range(steps_count + 1):
        yield format_str.format(index=steps_count - i) if i < steps_count else format_str[0]
        current_date += step


def format_datetime(value: str, granularity: GranularityEnum) -> str:
    match granularity:
        case GranularityEnum.year:
            return value

        case GranularityEnum.quarterYear:
            quarter_map = {"I": "Q1", "II": "Q2", "III": "Q3", "IV": "Q4"}
            quarter, year = value.split("/")
            return f"{quarter_map.get(quarter, quarter)}/{year}"

        case GranularityEnum.month:
            return value

        case GranularityEnum.day:
            return datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")

        case GranularityEnum.hour:
            day_part, time_part = value.split(" ")
            day = datetime.strptime(day_part, "%d.%m.%Y").strftime("%Y-%m-%d")

            if "-" in time_part and ":" not in time_part:
                start_hour, end_hour = time_part.split("-")
                formatted_start = f"{start_hour}:00"
                formatted_end = f"{end_hour}:00"
            else:
                start_hour, end_hour = time_part.split("-")
                formatted_start = start_hour if ":" in start_hour else f"{start_hour}:00"
                formatted_end = end_hour if ":" in end_hour else f"{end_hour}:00"

            return f"{day} {formatted_start}-{formatted_end}"

        case GranularityEnum.quarterHour:
            day_part, time_part = value.split(" ")
            day = datetime.strptime(day_part, "%d.%m.%Y").strftime("%Y-%m-%d")

            start_time, end_part = time_part.split("-")

            if ":" not in end_part:
                start_hour, start_minute = start_time.split(":")
                end_time = f"{start_hour}:{end_part}"
            else:
                end_time = end_part

            return f"{day} {start_time}-{end_time}"

        case GranularityEnum.minute:
            day_part, time_part = value.split(" ")
            day = datetime.strptime(day_part, "%d.%m.%Y").strftime("%Y-%m-%d")

            if "-" in time_part:
                start_time, end_time = time_part.split("-")

                formatted_start = f"{start_time}:00" if ":" not in start_time else start_time

                if ":" not in end_time:
                    formatted_end = f"{formatted_start[:-2]}{end_time}"
                else:
                    formatted_end = end_time

                return f"{day} {formatted_start}-{formatted_end}"

            formatted_time = f"{time_part}:00" if ":" not in time_part else time_part
            return f"{day} {formatted_time}"

        case _:
            raise ValueError(f"Unsupported granularity: {granularity}")

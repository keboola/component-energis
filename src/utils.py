import re
from datetime import datetime, timedelta
from typing import Generator, Optional

from src.configuration import GranularityEnum, EventEnum, PhaseEnum


def mask_sensitive_data_in_body(body: str, fields_to_mask: list[str] = None, mask_char: str = "*") -> str:
    """
    Masks sensitive fields in the SOAP XML body by showing only the first character.

    Args:
        body (str): The raw SOAP XML body as a string.
        fields_to_mask (list[str], optional): The list of XML tag names to mask.
        Defaults to ["username", "password", "exuziv", "exklic"].
        mask_char (str, optional): The character to use for masking. Defaults to "*".

    Returns:
        str: The masked SOAP XML body.
    """
    if fields_to_mask is None:
        fields_to_mask = ["username", "password", "exuziv", "exklic"]

    for field in fields_to_mask:
        pattern = f"<{field}>(.*?)</{field}>"

        def mask_match(match: re.Match) -> str:
            value = match.group(1)
            if len(value) > 1:
                masked_value = f"{value[0].lower()}{mask_char * (len(value) - 1)}"
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
    granularity: str,
    period: str
) -> tuple[str, dict[str, str]]:
    """
    Generates the SOAP request body and headers for the xexport operation.

    Args:
        username (str): The username for authentication.
        key (str): The authentication key.
        nodes (list[int]): List of node IDs to fetch data for.
        granularity (str): The granularity of the data ('m' for month, 'd' for day).
        period (str): The specific period for data export (e.g., 'm-1', 'd-10').

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
                <cas>{period}</cas>
                <typhodn>hodnota</typhodn>
            </ene:xexport>
        </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "xexport"
    }

    return soap_body, headers


def generate_xjournal_request(
    username: str,
    key: str,
    nodes: list[int],
    date_from: str,
    date_to: str,
    event_type: Optional[EventEnum] = None,
    phase: Optional[PhaseEnum] = None,
) -> tuple[str, dict[str, str]]:
    """
    Generates the SOAP request body and headers for the xjournal operation.

    Args:
        username (str): The username for authentication.
        key (str): The authentication key.
        nodes (list[int]): List of node IDs to fetch event logs for.
        date_from (str): Start date in MMDDYYYYHHMM format.
        date_to (str): End date in MMDDYYYYHHMM format.
        event_type (Optional[EventEnum]): Filter by event type (ERROR, WARNING, INFO).
        phase (Optional[PhaseEnum]): Filter by phase (INIT, RUNNING, COMPLETE).

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
            <ene:xjournal>
                <uzel>{nodes_str}</uzel>
                <cas>{date_from},{date_to}</cas>
                {f"<udalost>{event_type.value}</udalost>" if event_type else ""}
                {f"<faze>{phase.value}</faze>" if phase else ""}
            </ene:xjournal>
        </soap:Body>
    </soap:Envelope>"""

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "xjournal"
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
        GranularityEnum.quarterYear: "quarter",
        GranularityEnum.month: "month",
        GranularityEnum.day: "day",
        GranularityEnum.hour: "hour",
        GranularityEnum.quarterHour: "quarter_hour",
        GranularityEnum.minute: "minute"
    }

    return mapping.get(granularity)


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

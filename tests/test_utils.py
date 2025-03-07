import pytest
import logging
from src import (
    mask_sensitive_data_in_body,
    MaskSensitiveDataFilter,
    granularity_to_short_code,
    granularity_to_filename,
    convert_date_to_mmddyyyyhhmm,
    generate_periods,
    format_datetime,
    generate_logon_request,
    generate_xexport_request
)
from src.configuration import GranularityEnum


@pytest.mark.parametrize("input_text,expected_output", [
    ("<username>admin</username>", "<username>a*****</username>"),
    ("<password>secret123</password>", "<password>s*********</password>"),
    ("<exuziv>test</exuziv>", "<exuziv>t****</exuziv>"),
    ("<exklic>key</exklic>", "<exklic>k***</exklic>"),
])
def test_mask_sensitive_data(input_text, expected_output):
    assert mask_sensitive_data_in_body(input_text) == expected_output


def test_logging_filter():
    filter = MaskSensitiveDataFilter()
    log_record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0, msg="<password>mysecret</password>", args=(), exc_info=None
    )
    filter.filter(log_record)
    assert log_record.msg == "<password>m********</password>"


@pytest.mark.parametrize("granularity,expected", [
    (GranularityEnum.year, "r"),
    (GranularityEnum.quarterYear, "v"),
    (GranularityEnum.month, "m"),
    (GranularityEnum.day, "d"),
    (GranularityEnum.hour, "h"),
    (GranularityEnum.quarterHour, "c"),
    (GranularityEnum.minute, "t"),
])
def test_granularity_to_short_code(granularity, expected):
    assert granularity_to_short_code(granularity) == expected


@pytest.mark.parametrize("granularity,expected", [
    (GranularityEnum.year, "year"),
    (GranularityEnum.quarterYear, "quarter_year"),
    (GranularityEnum.month, "month"),
    (GranularityEnum.day, "day"),
    (GranularityEnum.hour, "hour"),
    (GranularityEnum.quarterHour, "quarter_hour"),
    (GranularityEnum.minute, "minute"),
])
def test_granularity_to_filename(granularity, expected):
    assert granularity_to_filename(granularity) == expected


@pytest.mark.parametrize("date_input,expected_output", [
    ("2025-03-01", "030120250000"),
    ("1999-12-31", "123119990000"),
])
def test_convert_date_to_mmddyyyyhhmm(date_input, expected_output):
    assert convert_date_to_mmddyyyyhhmm(date_input) == expected_output


@pytest.mark.parametrize("granularity,start,end,expected_count", [
    (GranularityEnum.year, "2020-01-01", "2023-01-01", 4),
    (GranularityEnum.quarterYear, "2022-01-01", "2023-01-01", 5),
    (GranularityEnum.month, "2023-01-01", "2023-06-01", 6),
    (GranularityEnum.day, "2023-06-01", "2023-06-07", 7),
    (GranularityEnum.hour, "2023-06-01", "2023-06-01", 1),
])
def test_generate_periods(granularity, start, end, expected_count):
    periods = list(generate_periods(granularity, start, end))
    assert len(periods) == expected_count


@pytest.mark.parametrize("value,granularity,expected", [
    ("2025", GranularityEnum.year, "2025"),
    ("III/2025", GranularityEnum.quarterYear, "Q3/2025"),
    ("06.03.2025", GranularityEnum.day, "2025-03-06"),
    ("06.03.2025 08-09", GranularityEnum.hour, "2025-03-06 08:00-09:00"),
    ("06.03.2025 08:00-09:00", GranularityEnum.hour, "2025-03-06 08:00-09:00"),
    ("06.03.2025 08:00-15", GranularityEnum.quarterHour, "2025-03-06 08:00-08:15"),
    ("06.03.2025 08:00-01", GranularityEnum.minute, "2025-03-06 08:00-08:01"),
    ("06.03.2025 08:00-08:01", GranularityEnum.minute, "2025-03-06 08:00-08:01"),
])
def test_format_datetime(value, granularity, expected):
    assert format_datetime(value, granularity) == expected


def test_generate_logon_request():
    body, headers = generate_logon_request("user", "pass")
    assert "<username>user</username>" in body
    assert "<password>pass</password>" in body
    assert headers["Content-Type"] == "text/xml; charset=utf-8"
    assert headers["SOAPAction"] == "logonex"


def test_generate_xexport_request():
    body, headers = generate_xexport_request("user", "key", [1, 2, 3], "010120250000", "020120250000", "d")
    assert "<exuziv>user</exuziv>" in body
    assert "<exklic>key</exklic>" in body
    assert "<uzel>1,2,3</uzel>" in body
    assert "<cas>010120250000,020120250000</cas>" in body
    assert "<per>d</per>" in body
    assert headers["Content-Type"] == "text/xml; charset=utf-8"
    assert headers["SOAPAction"] == "xexport"

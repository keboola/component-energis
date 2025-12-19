import pytest
from unittest.mock import Mock, patch

from api_client import EnergisClient, GRANULARITY_META
from configuration import Configuration, DatasetEnum, GranularityEnum


@pytest.fixture
def mock_config():
    """Provides a mock Configuration object."""
    mock_config = Mock(spec=Configuration)

    mock_auth = Mock()
    mock_auth.credentials = ("testuser", "testpassword")
    mock_auth.api_base_url = "https://fake-api.com"
    mock_auth.username = "testuser"

    mock_config.authentication = mock_auth

    mock_sync_options = Mock()
    mock_sync_options.dataset = DatasetEnum.xexport
    mock_sync_options.nodes = [7090001]
    mock_sync_options.date_from = "2025-01-01"
    mock_sync_options.date_to = "2025-01-31"
    mock_sync_options.granularity = GranularityEnum.day
    mock_sync_options.resolved_date_to = "2025-01-31"

    mock_config.sync_options = mock_sync_options
    mock_config.debug = False

    return mock_config


@pytest.fixture
def mock_auth_client():
    """Mocks httpx.Client for authentication."""
    mock_client = Mock()
    return mock_client


@pytest.fixture
def client(mock_config, mock_auth_client):
    """Creates an EnergisClient instance with mocked auth client."""
    client = EnergisClient(mock_config)
    client.auth_client = mock_auth_client
    return client


def create_mock_response(status_code, content):
    """Creates a mock HTTP response with given status code and content."""
    response = Mock()
    response.status_code = status_code
    response.content = content.encode("utf-8")
    response.text = content
    return response


def test_authenticate_success(client, mock_auth_client):
    """Tests successful authentication."""
    xml_response = """<response><key>test-api-key</key></response>"""
    mock_auth_client.post.return_value = create_mock_response(200, xml_response)

    auth_key = client.authenticate()

    assert auth_key == "test-api-key"
    mock_auth_client.post.assert_called_once()


def test_authenticate_failure(client, mock_auth_client):
    """Tests authentication failure with HTTP error."""
    mock_auth_client.post.return_value = create_mock_response(
        401, "<error>Unauthorized</error>"
    )

    with pytest.raises(Exception, match="Authentication failed: 401"):
        client.authenticate()


def test_authenticate_retry_on_already_logged_in(client, mock_auth_client):
    """Tests authentication retry logic when user is already logged in."""
    xml_response = """<faultstring>Uživatel již v systému přihlášen</faultstring>"""
    mock_auth_client.post.return_value = create_mock_response(500, xml_response)

    with patch("time.sleep", return_value=None) as mock_sleep:
        with pytest.raises(Exception, match="Maximum retries reached"):
            client.authenticate()

        assert mock_auth_client.post.call_count == client.max_retries
        mock_sleep.assert_called_with(client.retry_delay)


def test_parse_xexport_response_success(client, mock_config):
    """Tests _parse_xexport_response with valid SOAP response."""
    mock_config.sync_options.granularity = GranularityEnum.day

    xml_response = """
    <response>
        <responseData>
            <uzel>7090001</uzel>
            <hodnota>123.45</hodnota>
            <cas>06.03.2025</cas>
        </responseData>
    </response>
    """

    results = client._parse_xexport_response(xml_response.encode("utf-8"))

    assert len(results) == 1
    assert results[0] == {"uzel": "7090001", "hodnota": "123.45", "cas": "2025-03-06"}


def test_parse_xexport_response_hour_granularity(client, mock_config):
    """Tests _parse_xexport_response with hour granularity."""
    mock_config.sync_options.granularity = GranularityEnum.hour

    xml_response = """
    <response>
        <responseData>
            <uzel>7090001</uzel>
            <hodnota>123.45</hodnota>
            <cas>06.03.2025 08-09</cas>
        </responseData>
    </response>
    """

    results = client._parse_xexport_response(xml_response.encode("utf-8"))

    assert len(results) == 1
    assert results[0]["cas"] == "2025-03-06 08:00"


def test_parse_xexport_response_empty(client, mock_config):
    """Tests _parse_xexport_response with no data rows."""
    mock_config.sync_options.granularity = GranularityEnum.day

    xml_response = "<response></response>"

    results = client._parse_xexport_response(xml_response.encode("utf-8"))

    assert len(results) == 0


def test_parse_xexport_response_multiple_rows(client, mock_config):
    """Tests _parse_xexport_response with multiple data rows."""
    mock_config.sync_options.granularity = GranularityEnum.day

    xml_response = """
    <response>
        <responseData>
            <uzel>7090001</uzel>
            <hodnota>100.00</hodnota>
            <cas>01.03.2025</cas>
        </responseData>
        <responseData>
            <uzel>7090002</uzel>
            <hodnota>200.00</hodnota>
            <cas>02.03.2025</cas>
        </responseData>
    </response>
    """

    results = client._parse_xexport_response(xml_response.encode("utf-8"))

    assert len(results) == 2
    assert results[0]["uzel"] == "7090001"
    assert results[1]["uzel"] == "7090002"


def test_convert_date_to_mmddyyyyhhmm():
    """Tests correct conversion of date format."""
    assert EnergisClient.convert_date_to_mmddyyyyhhmm("2025-03-06") == "030620250000"


def test_granularity_to_short_code():
    """Tests mapping of granularity enum to short codes."""
    assert EnergisClient.granularity_to_short_code(GranularityEnum.year) == "r"
    assert EnergisClient.granularity_to_short_code(GranularityEnum.minute) == "t"
    assert EnergisClient.granularity_to_short_code(GranularityEnum.quarterHour) == "c"


def test_granularity_meta_complete_coverage():
    """Ensures all GranularityEnum values have metadata in GRANULARITY_META."""
    for granularity in GranularityEnum:
        assert granularity in GRANULARITY_META, f"Missing metadata for {granularity}"
        meta = GRANULARITY_META[granularity]
        assert isinstance(meta.short_code, str) and len(meta.short_code) == 1
        assert isinstance(meta.points_per_day, int) and meta.points_per_day > 0


class TestFormatDatetime:
    """Tests for format_datetime static method."""

    def test_year_passthrough(self):
        """Year granularity returns value unchanged."""
        assert EnergisClient.format_datetime("2025", GranularityEnum.year) == "2025"

    def test_quarter_year_roman_numerals(self):
        """QuarterYear converts roman numerals to Q1-Q4 format."""
        assert (
            EnergisClient.format_datetime("I/2025", GranularityEnum.quarterYear)
            == "Q1/2025"
        )
        assert (
            EnergisClient.format_datetime("II/2025", GranularityEnum.quarterYear)
            == "Q2/2025"
        )
        assert (
            EnergisClient.format_datetime("III/2025", GranularityEnum.quarterYear)
            == "Q3/2025"
        )
        assert (
            EnergisClient.format_datetime("IV/2025", GranularityEnum.quarterYear)
            == "Q4/2025"
        )

    def test_quarter_year_unknown_quarter(self):
        """QuarterYear with unknown quarter token passes through unchanged."""
        assert (
            EnergisClient.format_datetime("V/2025", GranularityEnum.quarterYear)
            == "V/2025"
        )

    def test_month_passthrough(self):
        """Month granularity returns value unchanged."""
        assert (
            EnergisClient.format_datetime("01/2025", GranularityEnum.month) == "01/2025"
        )

    def test_day_format_conversion(self):
        """Day converts DD.MM.YYYY to YYYY-MM-DD."""
        assert (
            EnergisClient.format_datetime("06.03.2025", GranularityEnum.day)
            == "2025-03-06"
        )
        assert (
            EnergisClient.format_datetime("31.12.2024", GranularityEnum.day)
            == "2024-12-31"
        )

    def test_hour_without_minutes(self):
        """Hour granularity with hour range (no minutes) adds :00."""
        assert (
            EnergisClient.format_datetime("06.03.2025 08-09", GranularityEnum.hour)
            == "2025-03-06 08:00"
        )
        assert (
            EnergisClient.format_datetime("06.03.2025 23-00", GranularityEnum.hour)
            == "2025-03-06 23:00"
        )

    def test_quarter_hour_with_minutes(self):
        """QuarterHour granularity with time range preserves minutes."""
        assert (
            EnergisClient.format_datetime(
                "06.03.2025 08:15-08:30", GranularityEnum.quarterHour
            )
            == "2025-03-06 08:15"
        )
        assert (
            EnergisClient.format_datetime(
                "06.03.2025 23:45-00:00", GranularityEnum.quarterHour
            )
            == "2025-03-06 23:45"
        )

    def test_minute_with_minutes(self):
        """Minute granularity with time range preserves minutes."""
        assert (
            EnergisClient.format_datetime(
                "06.03.2025 08:01-08:02", GranularityEnum.minute
            )
            == "2025-03-06 08:01"
        )

    def test_day_invalid_date_raises(self):
        """Day granularity with invalid date raises ValueError."""
        with pytest.raises(ValueError):
            EnergisClient.format_datetime("31.02.2025", GranularityEnum.day)

    def test_hour_missing_time_part_raises(self):
        """Hour granularity without time part raises ValueError."""
        with pytest.raises(ValueError):
            EnergisClient.format_datetime("06.03.2025", GranularityEnum.hour)

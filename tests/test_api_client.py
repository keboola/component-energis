import pytest
from unittest.mock import Mock, patch

from api_client import EnergisClient
from configuration import Configuration, DatasetEnum, GranularityEnum, SyncOptions
from utils import convert_date_to_mmddyyyyhhmm, granularity_to_short_code

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
def mock_http_client():
    """Mocks HttpClient.post_raw() method."""
    mock_http_client = Mock()
    return mock_http_client


@pytest.fixture
def client(mock_config, mock_http_client):
    """Creates an EnergisClient instance with mocked http_client."""
    client = EnergisClient(mock_config)
    client.http_client = mock_http_client
    return client


def create_mock_response(status_code, content):
    """Creates a mock HTTP response with given status code and content."""
    response = Mock()
    response.status_code = status_code
    response.content = content.encode("utf-8")
    response.text = content
    return response


def test_authenticate_success(client, mock_http_client):
    """Tests successful authentication."""
    xml_response = """<response><key>test-api-key</key></response>"""
    mock_http_client.post_raw.return_value = create_mock_response(200, xml_response)

    auth_key = client.authenticate()

    assert auth_key == "test-api-key"
    mock_http_client.post_raw.assert_called_once()


def test_authenticate_failure(client, mock_http_client):
    """Tests authentication failure with HTTP error."""
    mock_http_client.post_raw.return_value = create_mock_response(401, "<error>Unauthorized</error>")

    with pytest.raises(Exception, match="Authentication failed: 401"):
        client.authenticate()


def test_authenticate_retry_on_already_logged_in(client, mock_http_client):
    """Tests authentication retry logic when user is already logged in."""
    xml_response = """<faultstring>Uživatel již v systému přihlášen</faultstring>"""
    mock_http_client.post_raw.return_value = create_mock_response(500, xml_response)

    with patch("time.sleep", return_value=None) as mock_sleep:
        with pytest.raises(Exception, match="Maximum retries reached"):
            client.authenticate()

        assert mock_http_client.post_raw.call_count == client.max_retries
        mock_sleep.assert_called_with(client.retry_delay)


def test_fetch_data_success(client, mock_http_client, mock_config):
    """Tests fetching and parsing of data successfully."""
    auth_xml = """<response><key>test-api-key</key></response>"""
    data_xml = """
    <response>
        <responseData>
            <uzel>7090001</uzel>
            <hodnota>123.45</hodnota>
            <cas>06.03.2025</cas>
        </responseData>
    </response>
    """

    mock_http_client.post_raw.side_effect = [
        create_mock_response(200, auth_xml),
        create_mock_response(200, data_xml)
    ]

    results = list(client.fetch_data())

    assert len(results) == 1
    assert results[0] == {
        "uzel": "7090001",
        "hodnota": "123.45",
        "cas": "2025-03-06"
    }


def test_fetch_data_failure(client, mock_http_client):
    """Tests handling of failed data request."""
    auth_xml = """<response><key>test-api-key</key></response>"""
    mock_http_client.post_raw.side_effect = [
        create_mock_response(200, auth_xml),
        create_mock_response(500, "Internal Server Error")
    ]

    with pytest.raises(Exception, match="Data request failed: Internal Server Error"):
        list(client.fetch_data())


def test_fetch_data_invalid_xml(client, mock_http_client):
    """Tests handling of invalid XML response from API."""
    auth_xml = """<response><key>test-api-key</key></response>"""
    invalid_xml = "<response><invalid></invalid>"

    mock_http_client.post_raw.side_effect = [
        create_mock_response(200, auth_xml),
        create_mock_response(200, invalid_xml)
    ]

    results = list(client.fetch_data())

    assert len(results) == 0


def test_send_request_success(client, mock_http_client, mock_config):
    """Tests send_request with valid SOAP response."""
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
    mock_http_client.post_raw.return_value = create_mock_response(200, xml_response)

    results = list(client.send_request("https://fake-api.com/data", "<soap_request>", {"Content-Type": "text/xml"}))

    assert len(results) == 1
    assert results[0]["cas"] == "2025-03-06 08:00"


def test_send_request_failure(client, mock_http_client):
    """Tests handling of HTTP error response in send_request."""
    mock_http_client.post_raw.return_value = create_mock_response(500, "Internal Server Error")

    with pytest.raises(Exception, match="Data request failed: Internal Server Error"):
        list(client.send_request("https://fake-api.com/data", "<soap_request>", {"Content-Type": "text/xml"}))


def test_send_request_parsing_failure(client, mock_http_client):
    """Tests handling of parsing errors in send_request."""
    xml_response = "<response><invalid></invalid>"
    mock_http_client.post_raw.return_value = create_mock_response(200, xml_response)

    results = list(client.send_request("https://fake-api.com/data", "<soap_request>", {"Content-Type": "text/xml"}))

    assert len(results) == 0


def test_convert_date_to_mmddyyyyhhmm():
    """Tests correct conversion of date format."""
    assert convert_date_to_mmddyyyyhhmm("2025-03-06") == "030620250000"


def test_granularity_to_short_code():
    """Tests mapping of granularity enum to short codes."""
    assert granularity_to_short_code(GranularityEnum.year) == "r"
    assert granularity_to_short_code(GranularityEnum.minute) == "t"
    assert granularity_to_short_code(GranularityEnum.quarterHour) == "c"

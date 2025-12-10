import io
import logging
import time
from datetime import datetime, timedelta
from typing import Iterator, Dict, Any

from lxml import etree
from requests import Session
from zeep.transports import Transport

from configuration import Configuration, DatasetEnum, GranularityEnum

from utils import (
    generate_logon_request,
    generate_xexport_request,
    mask_sensitive_data_in_body,
    granularity_to_short_code,
    convert_date_to_mmddyyyyhhmm,
    format_datetime
)


class EnergisClient:
    """API Client for Energis API using various WSDLs"""

    CHUNK_SIZE_DAYS = {
        GranularityEnum.year: 365 * 5,
        GranularityEnum.quarterYear: 365 * 2,
        GranularityEnum.month: 365,
        GranularityEnum.day: 180,
        GranularityEnum.hour: 30,
        GranularityEnum.quarterHour: 30,
        GranularityEnum.minute: 7,
    }

    def __init__(self, config: Configuration):
        self.config = config

        logging.getLogger("zeep").setLevel(logging.INFO)
        logging.getLogger("zeep.transport").setLevel(logging.WARNING)

        session = Session()
        session.verify = False

        self.transport = Transport(session=session, timeout=(30, 300))
        self.max_retries = 5
        self.retry_delay = 120
        self.auth_key = None

    def authenticate(self) -> str:
        """Calls the auth endpoint and retrieves the key for further requests."""
        body, headers = generate_logon_request(*self.config.authentication.credentials)
        auth_url = f"{self.config.authentication.api_base_url}?logon"

        if self.config.debug:
            masked_body = mask_sensitive_data_in_body(body)
            logging.debug("Request auth url: %s", auth_url)
            logging.debug("Request header: %s", headers)
            logging.debug("Request body: %s", masked_body)

        retries = 0

        while retries < self.max_retries:
            try:
                response = self.transport.post(address=auth_url, message=body, headers=headers)

                if response.status_code != 200:
                    logging.error("Authentication failed with status code %s", response.status_code)
                    raise Exception(f"Authentication failed: {response.status_code}")

                logging.debug("Authentication response: %s", response.text)

                xml_response = etree.fromstring(response.content)
                key = xml_response.xpath("//key/text()")

                if key:
                    logging.debug("Authentication successful, received key: %s", key[0][:4] + "****")
                    return key[0]

                raise Exception("Authentication failed: No key found in the response.")

            except Exception as e:
                logging.error("Authentication attempt %d failed: %s", retries + 1, str(e))

                xml_response = etree.fromstring(response.content)
                fault_string = xml_response.xpath("//faultstring/text()")

                if fault_string and "již v systému přihlášen" in fault_string[0]:
                    logging.warning("User already logged in. Waiting 120 seconds before retrying...")
                    time.sleep(self.retry_delay)
                    retries += 1
                    continue

                raise e

        raise Exception("Maximum retries reached. Unable to authenticate.")

    def _generate_date_chunks(
        self, date_from: str, date_to: str, granularity: GranularityEnum
    ) -> Iterator[tuple[str, str]]:
        """
        Generates date range chunks based on the granularity to avoid large API payloads.

        Args:
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            granularity: The data granularity level

        Yields:
            Tuples of (chunk_start, chunk_end) dates in YYYY-MM-DD format
        """
        chunk_days = self.CHUNK_SIZE_DAYS.get(granularity, 30)
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end = datetime.strptime(date_to, "%Y-%m-%d")

        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=chunk_days), end)
            yield (current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d"))
            current_start = current_end + timedelta(days=1)

    def fetch_data(self) -> Iterator[Dict[str, Any]]:
        """Fetches data from the Energis API using the xexport SOAP call and returns the data."""
        key = self.authenticate()
        nodes = self.config.sync_options.nodes
        dataset = self.config.sync_options.dataset
        date_from = self.config.sync_options.date_from
        date_to = self.config.sync_options.date_to
        data_url = f"{self.config.authentication.api_base_url}?data"

        if dataset == DatasetEnum.xexport:
            granularity = self.config.sync_options.granularity
            chunks = list(self._generate_date_chunks(date_from, date_to, granularity))
            total_chunks = len(chunks)

            logging.info(
                f"Splitting date range {date_from} to {date_to} into {total_chunks} chunk(s) "
                f"for granularity '{granularity.value}'"
            )

            for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
                logging.info(f"Processing chunk {chunk_idx}/{total_chunks}: {chunk_start} to {chunk_end}")

                body, headers = generate_xexport_request(
                    username=self.config.authentication.username,
                    key=key,
                    nodes=nodes,
                    date_from=convert_date_to_mmddyyyyhhmm(chunk_start),
                    date_to=convert_date_to_mmddyyyyhhmm(chunk_end),
                    granularity=granularity_to_short_code(granularity),
                )

                chunk_row_count = 0
                for row in self.send_request(data_url, body, headers):
                    chunk_row_count += 1
                    yield row
                logging.info(f"Chunk {chunk_idx}/{total_chunks} completed: {chunk_row_count} rows")

    def send_request(self, url: str, body: str, headers: dict) -> Iterator[Dict[str, Any]]:
        """Sends the SOAP request, parses the response, and stores data in memory."""
        if self.config.debug:
            masked_body = mask_sensitive_data_in_body(body)
            logging.debug("Request url: %s", url)
            logging.debug("Request header: %s", headers)
            logging.debug("Request body: %s", masked_body)

        response = self.transport.post(address=url, message=body, headers=headers)

        if response.status_code != 200:
            try:
                xml_response = etree.fromstring(response.content)
                fault_string = xml_response.find(".//faultstring")
                if fault_string is not None:
                    error_message = fault_string.text
                    logging.error("SOAP Fault: %s", error_message)
                    raise Exception(f"Data request failed: {error_message}")
            except Exception as e:
                logging.error("Failed to parse SOAP fault response: %s", str(e))
                raise Exception(f"Data request failed: {response.text}")

        logging.debug("Data request response received")

        try:
            dataset = self.config.sync_options.dataset
            context = etree.iterparse(io.BytesIO(response.content), events=("start", "end"))

            if dataset == DatasetEnum.xexport:
                for event, elem in context:
                    if event == "end" and elem.tag == "responseData":
                        uzel = elem.findtext("uzel")
                        hodnota = elem.findtext("hodnota")
                        cas = elem.findtext("cas")

                        if uzel and hodnota and cas:
                            yield {
                                "uzel": uzel,
                                "hodnota": hodnota,
                                "cas": format_datetime(cas, self.config.sync_options.granularity)
                            }

                        elem.clear()

        except Exception as e:
            logging.error("Failed to parse SOAP response: %s", str(e))
            raise

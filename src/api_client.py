import asyncio
import csv
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterator

import httpx
from lxml import etree

from configuration import Configuration, DatasetEnum, GranularityEnum


@dataclass(frozen=True)
class GranularityMeta:
    """
    Metadata for each granularity level.

    Attributes:
        short_code: Single-letter API code (r, v, m, d, h, c, t)
        points_per_day: Number of data points per node per day
    """

    short_code: str
    points_per_day: int


# Data points per day for each granularity level
# Based on time intervals:
# - minute: 24h × 60min = 1440 points/day
# - quarterHour: 24h × 4 quarters = 96 points/day
# - hour: 24 points/day
# - day/month/quarterYear/year: 1 point per period
GRANULARITY_META: dict[GranularityEnum, GranularityMeta] = {
    GranularityEnum.year: GranularityMeta("r", 1),
    GranularityEnum.quarterYear: GranularityMeta("v", 1),
    GranularityEnum.month: GranularityMeta("m", 1),
    GranularityEnum.day: GranularityMeta("d", 1),
    GranularityEnum.hour: GranularityMeta("h", 24),
    GranularityEnum.quarterHour: GranularityMeta("c", 96),
    GranularityEnum.minute: GranularityMeta("t", 1440),
}


class EnergisClient:
    """API Client for Energis API using async httpx with streaming CSV output."""

    # Number of concurrent requests for chunk processing
    MAX_CONCURRENT = 4

    # HTTP timeouts (connect, read) in seconds
    CONNECT_TIMEOUT = 30
    READ_TIMEOUT = 300

    # Memory limit per chunk (in MB)
    MAX_CHUNK_SIZE_MB = 10

    # Average size of one row in memory (3 columns: uzel, hodnota, cas)
    BYTES_PER_ROW = 200

    # Calculate max rows per chunk from memory limit
    MAX_ROWS_PER_CHUNK = (MAX_CHUNK_SIZE_MB * 1024 * 1024) // BYTES_PER_ROW

    @staticmethod
    def mask_sensitive_data(body: str, mask_char: str = "*") -> str:
        """Masks sensitive fields in the SOAP XML body."""
        fields_to_mask = ["username", "password", "exuziv", "exklic"]
        for field in fields_to_mask:
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

    @staticmethod
    def generate_logon_request(
        username: str, password: str
    ) -> tuple[str, dict[str, str]]:
        """Generates the SOAP request body and headers for the logonex operation."""
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
        headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "logonex"}
        return soap_body, headers

    @staticmethod
    def generate_xexport_request(
        username: str,
        key: str,
        nodes: list[int],
        date_from: str,
        date_to: str,
        granularity: str,
    ) -> tuple[str, dict[str, str]]:
        """Generates the SOAP request body and headers for the xexport operation."""
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
        headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": "xexport"}
        return soap_body, headers

    @staticmethod
    def granularity_to_short_code(granularity: GranularityEnum) -> str:
        """Returns a single-letter string based on the GranularityEnum value."""
        return GRANULARITY_META[granularity].short_code

    @staticmethod
    def convert_date_to_mmddyyyyhhmm(date_str: str) -> str:
        """Converts a date string from 'YYYY-MM-DD' format to 'MMDDYYYYHHMM'."""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            return date_obj.strftime("%m%d%Y0000")
        except ValueError:
            raise ValueError(
                f"Invalid date format: {date_str}. Expected format: YYYY-MM-DD"
            )

    @staticmethod
    def format_datetime(value: str, granularity: GranularityEnum) -> str:
        """Formats datetime value based on granularity."""
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
            case (
                GranularityEnum.hour
                | GranularityEnum.quarterHour
                | GranularityEnum.minute
            ):
                day_part, time_part = value.split(" ")
                day = datetime.strptime(day_part, "%d.%m.%Y").strftime("%Y-%m-%d")
                start_time = time_part.split("-")[0]
                return (
                    f"{day} {start_time}:00"
                    if ":" not in start_time
                    else f"{day} {start_time}"
                )
            case _:
                raise ValueError(f"Unsupported granularity: {granularity}")

    def __init__(self, config: "Configuration"):
        self.config = config

        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)

        # Sync HTTP client for authentication
        self.auth_client = httpx.Client(
            verify=False,
            timeout=httpx.Timeout(self.READ_TIMEOUT, connect=self.CONNECT_TIMEOUT),
        )
        self.max_retries = 5
        self.retry_delay = 120
        self.auth_key = None

    def authenticate(self) -> str:
        """Calls the auth endpoint and retrieves the key for further requests."""
        body, headers = self.generate_logon_request(
            *self.config.authentication.credentials
        )
        auth_url = f"{self.config.authentication.api_base_url}?logon"

        if self.config.debug:
            masked_body = self.mask_sensitive_data(body)
            logging.debug("Request auth url: %s", auth_url)
            logging.debug("Request header: %s", headers)
            logging.debug("Request body: %s", masked_body)

        retries = 0
        response = None

        while retries < self.max_retries:
            try:
                response = self.auth_client.post(
                    auth_url, content=body, headers=headers
                )

                if response.status_code != 200:
                    logging.error(
                        "Authentication failed with status code %s",
                        response.status_code,
                    )
                    raise Exception(f"Authentication failed: {response.status_code}")

                logging.debug("Authentication response: %s", response.text)

                xml_response = etree.fromstring(response.content)
                key = xml_response.xpath("//key/text()")

                if key:
                    logging.debug(
                        "Authentication successful, received key: %s",
                        key[0][:4] + "****",
                    )
                    return key[0]

                raise Exception("Authentication failed: No key found in the response.")

            except Exception as e:
                logging.error(
                    "Authentication attempt %d failed: %s", retries + 1, str(e)
                )

                # Only try to parse SOAP fault if we have a response
                if response is not None:
                    try:
                        xml_response = etree.fromstring(response.content)
                        fault_string = xml_response.xpath("//faultstring/text()")

                        if (
                            fault_string
                            and "již v systému přihlášen" in fault_string[0]
                        ):
                            logging.warning(
                                "User already logged in. Waiting %d seconds before retrying...",
                                self.retry_delay,
                            )
                            time.sleep(self.retry_delay)
                            retries += 1
                            response = None
                            continue
                    except Exception:
                        pass  # Failed to parse SOAP fault response, will re-raise original exception

                raise e

        raise Exception("Maximum retries reached. Unable to authenticate.")

    def _calculate_chunk_days(
        self, granularity: GranularityEnum, num_nodes: int
    ) -> int:
        """
        Calculates chunk size in days based on memory limit.

        Args:
            granularity: Data granularity level
            num_nodes: Number of nodes being queried

        Returns:
            Number of days per chunk (minimum 1)
        """
        meta = GRANULARITY_META[granularity]
        rows_per_day = meta.points_per_day * num_nodes
        chunk_days = max(1, self.MAX_ROWS_PER_CHUNK // rows_per_day)

        logging.info(
            "Chunk size: %d days (%d nodes × %d points/day = %d rows/day)",
            chunk_days,
            num_nodes,
            meta.points_per_day,
            rows_per_day,
        )

        return chunk_days

    def _generate_date_chunks(
        self, date_from: str, date_to: str, granularity: GranularityEnum, num_nodes: int
    ) -> Iterator[tuple[str, str]]:
        """
        Generates date range chunks based on memory limit.

        Args:
            date_from: Start date in YYYY-MM-DD format
            date_to: End date in YYYY-MM-DD format
            granularity: Data granularity level
            num_nodes: Number of nodes being queried

        Yields:
            Tuples of (chunk_start, chunk_end) dates in YYYY-MM-DD format
        """
        chunk_days = self._calculate_chunk_days(granularity, num_nodes)
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end = datetime.strptime(date_to, "%Y-%m-%d")

        current_start = start
        while current_start < end:
            current_end = min(current_start + timedelta(days=chunk_days), end)
            yield current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
            current_start = current_end + timedelta(days=1)

    async def _fetch_chunk_streaming(
        self,
        client: httpx.AsyncClient,
        semaphore: asyncio.Semaphore,
        chunk_idx: int,
        total_chunks: int,
        chunk_start: str,
        chunk_end: str,
        key: str,
        data_url: str,
    ) -> tuple[int, list[dict[str, Any]]]:
        """
        Fetches data for a single chunk using streaming HTTP and incremental XML parsing.

        Uses httpx.stream() to avoid loading entire response into memory,
        and XMLPullParser to parse XML incrementally as bytes arrive.

        Args:
            client: Shared httpx AsyncClient
            semaphore: Semaphore to limit concurrency
            chunk_idx: Index of this chunk (1-based)
            total_chunks: Total number of chunks
            chunk_start: Start date for this chunk
            chunk_end: End date for this chunk
            key: Authentication key
            data_url: URL for data requests

        Returns:
            Tuple of (chunk_idx, list of row dictionaries)
        """
        async with semaphore:
            logging.info(
                "Processing chunk %d/%d: %s to %s",
                chunk_idx,
                total_chunks,
                chunk_start,
                chunk_end,
            )

            body, headers = self.generate_xexport_request(
                username=self.config.authentication.username,
                key=key,
                nodes=self.config.sync_options.nodes,
                date_from=self.convert_date_to_mmddyyyyhhmm(chunk_start),
                date_to=self.convert_date_to_mmddyyyyhhmm(chunk_end),
                granularity=self.granularity_to_short_code(
                    self.config.sync_options.granularity
                ),
            )

            if self.config.debug:
                masked_body = self.mask_sensitive_data(body)
                logging.debug("Request url: %s", data_url)
                logging.debug("Request header: %s", headers)
                logging.debug("Request body: %s", masked_body)

            rows: list[dict[str, Any]] = []

            async with client.stream(
                "POST", data_url, content=body, headers=headers
            ) as response:
                if response.status_code != 200:
                    error_content = await response.aread()
                    try:
                        xml_response = etree.fromstring(error_content)
                        fault_string = xml_response.find(".//faultstring")
                        if fault_string is not None:
                            error_message = fault_string.text
                            logging.error("SOAP Fault: %s", error_message)
                            raise Exception(f"Data request failed: {error_message}")
                    except etree.XMLSyntaxError:
                        pass
                    raise Exception(
                        f"Data request failed: {error_content.decode('utf-8', errors='replace')}"
                    )

                parser = etree.XMLPullParser(events=("end",))
                granularity = self.config.sync_options.granularity

                async for chunk in response.aiter_bytes():
                    parser.feed(chunk)
                    for _, elem in parser.read_events():
                        if elem.tag == "responseData":
                            uzel = elem.findtext("uzel")
                            hodnota = elem.findtext("hodnota")
                            cas = elem.findtext("cas")
                            if uzel and hodnota and cas:
                                rows.append(
                                    {
                                        "uzel": uzel,
                                        "hodnota": hodnota,
                                        "cas": self.format_datetime(cas, granularity),
                                    }
                                )
                            elem.clear()
                            while elem.getprevious() is not None:
                                parent = elem.getparent()
                                if parent is not None:
                                    del parent[0]

            if len(rows) > 0:
                logging.info(
                    "Chunk %d/%d fetched: %d rows", chunk_idx, total_chunks, len(rows)
                )
            else:
                logging.debug(
                    "Chunk %d/%d fetched: no data for this period",
                    chunk_idx,
                    total_chunks,
                )
            return chunk_idx, rows

    def _parse_xexport_response(self, content: bytes) -> list[dict[str, Any]]:
        """
        Parses the xexport SOAP response and extracts data rows.
        Used for testing and fallback scenarios.

        Args:
            content: Raw response content bytes

        Returns:
            List of row dictionaries
        """
        rows = []
        parser = etree.XMLPullParser(events=("end",))
        parser.feed(content)
        for _, elem in parser.read_events():
            if elem.tag == "responseData":
                uzel = elem.findtext("uzel")
                hodnota = elem.findtext("hodnota")
                cas = elem.findtext("cas")
                if uzel and hodnota and cas:
                    rows.append(
                        {
                            "uzel": uzel,
                            "hodnota": hodnota,
                            "cas": self.format_datetime(
                                cas, self.config.sync_options.granularity
                            ),
                        }
                    )
                elem.clear()
        return rows

    async def _fetch_and_write_chunks(
        self,
        chunks: list[tuple[str, str]],
        key: str,
        data_url: str,
        csv_writer: csv.DictWriter,
    ) -> int:
        """
        Fetches all chunks concurrently and writes rows directly to CSV.

        Writes each chunk immediately as it completes (no ordering guarantee).
        This minimizes memory usage since we don't buffer completed chunks.

        Args:
            chunks: List of (chunk_start, chunk_end) tuples
            key: Authentication key
            data_url: URL for data requests
            csv_writer: CSV DictWriter to write rows to

        Returns:
            Total number of rows written
        """
        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT)
        total_chunks = len(chunks)
        total_rows = 0

        logging.info("Using %d concurrent requests", self.MAX_CONCURRENT)

        async with httpx.AsyncClient(
            verify=False,
            timeout=httpx.Timeout(self.READ_TIMEOUT, connect=self.CONNECT_TIMEOUT),
        ) as client:
            tasks = [
                asyncio.create_task(
                    self._fetch_chunk_streaming(
                        client,
                        semaphore,
                        idx,
                        total_chunks,
                        chunk_start,
                        chunk_end,
                        key,
                        data_url,
                    )
                )
                for idx, (chunk_start, chunk_end) in enumerate(chunks, 1)
            ]
            for completed_task in asyncio.as_completed(tasks):
                _, rows = await completed_task
                for row in rows:
                    csv_writer.writerow(row)
                total_rows += len(rows)
        return total_rows

    def fetch_data(self, csv_writer: csv.DictWriter) -> int:
        """
        Fetches data from the Energis API and writes directly to CSV.

        Args:
            csv_writer: CSV DictWriter to write rows to

        Returns:
            Total number of rows written
        """
        key = self.authenticate()
        dataset = self.config.sync_options.dataset
        date_from = self.config.sync_options.date_from
        date_to = self.config.sync_options.date_to
        data_url = f"{self.config.authentication.api_base_url}?data"

        if dataset == DatasetEnum.xexport:
            granularity = self.config.sync_options.granularity
            num_nodes = len(self.config.sync_options.nodes)
            chunks = list(
                self._generate_date_chunks(date_from, date_to, granularity, num_nodes)
            )
            total_chunks = len(chunks)

            logging.info(
                "Fetching %d node(s), date range %s to %s, %d chunk(s), granularity '%s'",
                num_nodes,
                date_from,
                date_to,
                total_chunks,
                granularity.value,
            )

            return asyncio.run(
                self._fetch_and_write_chunks(chunks, key, data_url, csv_writer)
            )
        return 0

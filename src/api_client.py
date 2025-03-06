import asyncio
import logging
import httpx
from lxml import etree
from configuration import Configuration, DatasetEnum
from utils import (
    generate_logon_request,
    generate_xexport_request,
    mask_sensitive_data_in_body,
    granularity_to_short_code,
    generate_periods,
    generate_xjournal_request,
    convert_date_to_mmddyyyyhhmm,
    format_datetime
)


class EnergisClient:
    """Asynchronous Energis API Client with authentication and throttling."""

    def __init__(self, config: Configuration):
        self.config = config
        self.base_url = self.config.authentication.api_base_url
        self.auth_key = None
        self.max_retries = 5
        self.retry_delay = 120
        self.auth_lock = asyncio.Lock()
        self.semaphore = asyncio.Semaphore(self.config.sync_options.max_concurrent_requests)

        self.results = []

    async def authenticate(self) -> str:
        """Calls the auth endpoint and retrieves the key for further requests."""
        async with self.auth_lock:
            if self.auth_key:
                return self.auth_key

            logging.info("Authenticating with the Energis API...")
            body, headers = generate_logon_request(*self.config.authentication.credentials)
            auth_url = f"{self.base_url}?logon"

            if self.config.debug:
                masked_body = mask_sensitive_data_in_body(body)
                logging.debug("Request auth url: %s", auth_url)
                logging.debug("Request header: %s", headers)
                logging.debug("Request body: %s", masked_body)

            async with httpx.AsyncClient(verify=False) as client:
                retries = 0
                while retries < self.max_retries:
                    try:
                        response = await client.post(auth_url, content=body.encode("utf-8"), headers=headers)
                        response.raise_for_status()

                        logging.debug("Authentication response: %s", response.text)

                        xml_response = etree.fromstring(response.content)
                        key = xml_response.xpath("//key/text()")

                        if key:
                            self.auth_key = key[0]
                            logging.debug("Authentication successful, received key: %s", key[0][:4] + "****")
                            return self.auth_key

                        raise Exception("Authentication failed: No key found in the response.")

                    except (httpx.HTTPStatusError, httpx.RequestError) as err:
                        logging.error(f"Authentication attempt {retries + 1} failed: {err}")

                    if "response" in locals() and response is not None:
                        try:
                            xml_response = etree.fromstring(response.content)
                            fault_string = xml_response.xpath("//faultstring/text()")

                            if fault_string and "již v systému přihlášen" in fault_string[0]:
                                logging.warning(f"User already logged in. Retrying after {self.retry_delay} seconds...")
                                await asyncio.sleep(self.retry_delay)
                                retries += 1
                                continue
                        except Exception:
                            logging.warning("Failed to parse error response, retrying...")

                    retries += 1
                    await asyncio.sleep(min(self.retry_delay * (2 ** retries), 60))

                raise Exception("Maximum retries reached. Unable to authenticate.")

    async def fetch_data(self) -> list[dict[str, str]]:
        """Fetches data from the Energis API using the SOAP calls and returns the data."""
        key = await self.authenticate()
        nodes = self.config.sync_options.nodes
        dataset = self.config.sync_options.dataset
        date_from = self.config.sync_options.date_from
        date_to = self.config.sync_options.resolved_date_to
        data_url = f"{self.base_url}?data"

        tasks = []

        if dataset == DatasetEnum.xexport:
            granularity = self.config.sync_options.granularity

            for period in generate_periods(granularity, date_from, date_to):
                body, headers = generate_xexport_request(
                    username=self.config.authentication.username,
                    key=key,
                    nodes=nodes,
                    date_from=convert_date_to_mmddyyyyhhmm(date_from),
                    date_to=convert_date_to_mmddyyyyhhmm(date_to),
                    granularity=granularity_to_short_code(granularity),
                    period=period
                )
                task = asyncio.create_task(self.send_request(data_url, body, headers))
                tasks.append(task)

        elif dataset == DatasetEnum.xjournal:
            body, headers = generate_xjournal_request(
                username=self.config.authentication.username,
                key=key,
                nodes=nodes,
                date_from=convert_date_to_mmddyyyyhhmm(date_from),
                date_to=convert_date_to_mmddyyyyhhmm(date_to),
                event_type=self.config.sync_options.event_type,
                phase=self.config.sync_options.phase,
            )
            task = asyncio.create_task(self.send_request(data_url, body, headers))
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks)

        return self.results

    async def send_request(self, url: str, body: str, headers: dict) -> None:
        """Sends the SOAP request asynchronously, parses the response, and stores data in memory."""
        if self.config.debug:
            masked_body = mask_sensitive_data_in_body(body)
            logging.debug("Request url: %s", url)
            logging.debug("Request header: %s", headers)
            logging.debug("Request body: %s", masked_body)

        async with self.semaphore:
            async with httpx.AsyncClient(verify=False) as client:
                try:
                    response = await client.post(url, content=body.encode("utf-8"), headers=headers)
                    response.raise_for_status()

                    logging.debug("Data request response: %s", response.text)
                    self.parse_response(response.content)

                except httpx.HTTPStatusError as http_err:
                    logging.error(f"HTTP error during data request: {http_err}")
                except httpx.RequestError as req_err:
                    logging.error(f"Network error during data request: {req_err}")
                except Exception as e:
                    logging.warning(f"Unexpected error parsing SOAP response: {str(e)}")

    def parse_response(self, content: bytes) -> None:
        """Parses SOAP XML response and appends data to results."""
        try:
            dataset = self.config.sync_options.dataset
            xml_response = etree.fromstring(content)

            if dataset == DatasetEnum.xexport:
                for response_data in xml_response.xpath("//responseData"):
                    self.results.append({
                        "uzel": response_data.findtext("uzel", ""),
                        "hodnota": response_data.findtext("hodnota", ""),
                        "cas": format_datetime(response_data.findtext("cas", ""), self.config.sync_options.granularity)
                    })

            elif dataset == DatasetEnum.xjournal:
                for response_data in xml_response.xpath("//responseData"):
                    self.results.append({
                        "uzel": response_data.findtext("uzel", ""),
                        "popisu": response_data.findtext("popisu", ""),
                        "cas": response_data.findtext("cas", ""),
                        "udalost": response_data.findtext("udalost", ""),
                        "faze": response_data.findtext("faze", ""),
                        "kp": response_data.findtext("kp", ""),
                        "pozn": response_data.findtext("pozn", ""),
                        "inf": response_data.findtext("inf", "")
                    })

        except Exception as e:
            logging.warning(f"Failed to parse SOAP response: {str(e)}")

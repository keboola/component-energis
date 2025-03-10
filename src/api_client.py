import logging
import time

from lxml import etree
from requests import Session
from zeep.transports import Transport

from configuration import Configuration, DatasetEnum

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

    def __init__(self, config: Configuration):
        self.config = config

        logging.getLogger("zeep").setLevel(logging.INFO)
        logging.getLogger("zeep.transport").setLevel(logging.WARNING)

        session = Session()
        session.verify = False

        self.transport = Transport(session=session)
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

    def fetch_data(self) -> iter:
        """Fetches data from the Energis API using the xexport SOAP call and returns the data."""
        key = self.authenticate()
        nodes = self.config.sync_options.nodes
        dataset = self.config.sync_options.dataset
        date_from = self.config.sync_options.date_from
        date_to = self.config.sync_options.date_to
        data_url = f"{self.config.authentication.api_base_url}?data"

        if dataset == DatasetEnum.xexport:
            granularity = self.config.sync_options.granularity

            body, headers = generate_xexport_request(
                username=self.config.authentication.username,
                key=key,
                nodes=nodes,
                date_from=convert_date_to_mmddyyyyhhmm(date_from),
                date_to=convert_date_to_mmddyyyyhhmm(date_to),
                granularity=granularity_to_short_code(granularity),
            )

            yield from self.send_request(data_url, body, headers)

    def send_request(self, url: str, body: str, headers: dict) -> iter:
        """Sends the SOAP request, parses the response, and stores data in memory."""
        if self.config.debug:
            masked_body = mask_sensitive_data_in_body(body)
            logging.debug("Request url: %s", url)
            logging.debug("Request header: %s", headers)
            logging.debug("Request body: %s", masked_body)

        response = self.transport.post(address=url, message=body, headers=headers)

        if response.status_code != 200:
            logging.error("Data request failed with status code %s", response.status_code)
            raise Exception(f"Data request failed: {response.status_code}")

        logging.debug("Data request response: %s", response.text)

        try:
            dataset = self.config.sync_options.dataset
            xml_response = etree.fromstring(response.content)

            if dataset == DatasetEnum.xexport:
                for response_data in xml_response.xpath("//responseData"):
                    uzel = response_data.findtext("uzel")
                    hodnota = response_data.findtext("hodnota")
                    cas = response_data.findtext("cas")

                    if uzel and hodnota and cas:
                        yield {
                            "uzel": uzel,
                            "hodnota": hodnota,
                            "cas": format_datetime(
                                response_data.findtext("cas", ""),
                                self.config.sync_options.granularity
                            )
                        }

        except Exception as e:
            logging.warning("Failed to parse SOAP response: %s", str(e))

import re


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


def generate_data_request(
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

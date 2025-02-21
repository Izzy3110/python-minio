import datetime


def parse_date(date_str):
    date_format = "%a, %d %b %Y %H:%M:%S GMT"
    parsed_date = datetime.datetime.strptime(date_str, date_format)
    return parsed_date.strftime("%d.%m.%Y %H:%M:%S")


def response_headers_to_json(response_headers) -> dict:
    headers_json = {}
    for header_key, header_item in response_headers.items():
        headers_json[header_key] = header_item.lstrip('"').rstrip('"')
    return headers_json

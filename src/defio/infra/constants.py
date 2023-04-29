import ipaddress
from typing import Final

PROJECT_NAME: Final = "defio"

ALL_NETWORK: Final = ipaddress.IPv4Network("0.0.0.0/0")

DEFAULT_PORT_MYSQL: Final = 3306
DEFAULT_PORT_POSTGRESQL: Final = 5432
DEFAULT_PORT_REDSHIFT: Final = 5439

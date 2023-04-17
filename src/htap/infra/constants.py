import ipaddress

PACKAGE_NAME = "htap"  # Used by `ComponentMixin`

ALL_NETWORK = ipaddress.IPv4Network("0.0.0.0/0")

DEFAULT_PORT_MYSQL = 3306
DEFAULT_PORT_POSTGRESQL = 5432
DEFAULT_PORT_REDSHIFT = 5439

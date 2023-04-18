import ipaddress

PACKAGE_NAME = "htap"

ALL_NETWORK = ipaddress.IPv4Network("0.0.0.0/0")

DEFAULT_PORT_MYSQL = 3306
DEFAULT_PORT_POSTGRESQL = 5432
DEFAULT_PORT_REDSHIFT = 5439

# Used by both `client` and `infra` to relay Pulumi stack outputs
AURORA_KEY_PREFIX = "aurora"
REDSHIFT_KEY_PREFIX = "redshift"
HOST_KEY_SUFFIX = "host"
PORT_KEY_SUFFIX = "port"
USERNAME_KEY_SUFFIX = "username"
PASSWORD_KEY_SUFFIX = "password"
INITIAL_DBNAME_KEY_SUFFIX = "initial-dbname"

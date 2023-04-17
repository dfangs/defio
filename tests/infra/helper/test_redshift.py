import pytest

from htap.infra.helper.redshift import RedshiftNodeType


@pytest.mark.parametrize(
    "node_type, expected_str_value",
    [
        (RedshiftNodeType.RA3_XLPLUS, "ra3.xlplus"),
        (RedshiftNodeType.RA3_16XLARGE, "ra3.16xlarge"),
        (RedshiftNodeType.DS2_8XLARGE, "ds2.8xlarge"),
        (RedshiftNodeType.DC1_LARGE, "dc1.large"),
    ],
)
def test_redshift_node_type(
    node_type: RedshiftNodeType, expected_str_value: str
) -> None:
    assert node_type == expected_str_value

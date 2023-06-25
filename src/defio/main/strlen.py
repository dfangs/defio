from defio.dataset.imdb import IMDB_GZ
from defio.sql.schema import DataType

# Unlike Postgres, Redshift counts the number of _bytes_, not the number of characters
# This may cause a problem when loading multibyte UTF-8 characters, such as `×`
# Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_Character_types.html

if __name__ == "__main__":
    for table in IMDB_GZ.tables:
        df = IMDB_GZ.get_dataframe(table)

        for column in table.columns:
            if column.dtype is not DataType.STRING:
                continue

            notna_series = df[column.name].dropna()

            # Reference: https://stackoverflow.com/a/30686735
            max_length_in_bytes = max(
                len(value.encode("utf-8")) for value in notna_series.array
            )

            print(f"{table.name}.{column.name}: {max_length_in_bytes}")

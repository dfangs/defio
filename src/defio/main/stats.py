from defio.dataset.imdb import IMDB_GZ
from defio.dataset.stats import DataStats

if __name__ == "__main__":
    # GZ is about as fast as TSV
    stats = DataStats.from_dataset(IMDB_GZ, concurrent=True, verbose=True)

    with open(IMDB_GZ.directory / "stats.json", mode="w+", encoding="utf-8") as f:
        stats.dump(f)

import asyncio
import contextlib
import re
from collections.abc import Callable, Sequence
from enum import Enum
from pathlib import Path
from typing import ParamSpec

from defio.dataset.imdb.source.schema import (
    CrewType,
    Genre,
    NameBasic,
    PrincipalCategory,
    TitleAka,
    TitleAltType,
    TitleBasic,
    TitleCrew,
    TitleEpisode,
    TitlePrincipal,
    TitleRating,
    TitleType,
)
from defio.dataset.utils import TsvReader, TsvWriter, compress_to_gzip
from defio.utils.logging import log_around
from defio.utils.time import measure_time


async def generate_tables(
    source_tsv_dir: Path,
    target_tsv_dir: Path,
    target_gz_dir: Path | None = None,
    *,
    verbose: bool = False,
) -> None:
    """
    Reads the TSV files (each representing a table in the IMDB dataset)
    from the source directory and writes the normalized version of the
    corresponding table, which may consist of more than one tables.

    If `target_gz_dir` is provided, then compress the resulting TSV files
    into gzip as well.
    """
    with log_around(
        verbose,
        start="Generating normalized tables of the new IMDB dataset\n---",
        end=lambda: (
            "---\nFinished generating all tables "
            f"in {measurement.total_seconds:.2f} seconds"
        ),
    ):
        with measure_time() as measurement:
            async with asyncio.TaskGroup() as tg:
                # Generate enum-like tables
                for enum_class in (
                    Genre,
                    TitleType,
                    TitleAltType,
                    CrewType,
                    PrincipalCategory,
                ):
                    tg.create_task(
                        asyncio.to_thread(
                            _with_options(
                                _generate_enum_table,
                                enum_class.__name__,
                                target_gz_dir,
                                verbose,
                            ),
                            enum_class,
                            target_tsv_dir,
                        )
                    )

                # Generate the rest of the tables
                for source_name, normalizer in {
                    "title_akas": _normalize_title_akas,
                    "title_basics": _normalize_title_basics,
                    "title_crew": _normalize_title_crew,
                    "title_episode": _normalize_title_episode,
                    "title_principals": _normalize_title_principals,
                    "title_ratings": _normalize_title_ratings,
                    "name_basics": _normalize_name_basics,
                }.items():
                    tg.create_task(
                        asyncio.to_thread(
                            _with_options(
                                normalizer, source_name, target_gz_dir, verbose
                            ),
                            source_tsv_dir,
                            target_tsv_dir,
                        )
                    )


_P = ParamSpec("_P")


def _with_options(
    func: Callable[_P, Sequence[Path]],
    source_name: str,
    target_gz_dir: Path | None,
    verbose: bool,
) -> Callable[_P, None]:
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> None:
        with log_around(
            verbose,
            start=f"Generating tables from `{source_name}`",
            end=lambda: (
                f"Finished generating tables from `{source_name}` "
                f"in {measurement.total_seconds:.2f} seconds"
            ),
        ):
            with measure_time() as measurement:
                target_paths = func(*args, **kwargs)

        if target_gz_dir is None:
            return

        for path in target_paths:
            with log_around(
                verbose,
                start=f"Compressing `{path.name}` into gzip",
                end=lambda path=path: (
                    f"Finished compressing `{path.name}` "
                    f"in {measurement.total_seconds:.2f} seconds"
                ),
            ):
                with measure_time() as measurement:
                    compress_to_gzip(path, target_gz_dir)

    return wrapped


def _generate_enum_table(enum: type[Enum], target_tsv_dir: Path) -> Sequence[Path]:
    assert target_tsv_dir.exists()

    # Convert PascalCase to snake_case
    # Reference: https://stackoverflow.com/a/1176023
    table_name = re.sub(r"(?<!^)(?=[A-Z])", "_", enum.__name__).lower()
    target_tsv_path = target_tsv_dir / f"{table_name}.tsv"

    with TsvWriter.open(
        target_tsv_path, with_index=True, header=["id", "name"]
    ) as writer:
        for member in enum:
            writer.write_line([str(member)])  # Explicitly convert to str

    return [target_tsv_path]


def _normalize_title_akas(source_tsv_dir: Path, target_tsv_dir: Path) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.akas.tsv"
    assert source_tsv_path.exists()

    title_aka_tsv_path = target_tsv_dir / "title_aka.tsv"
    title_aka_type_tsv_path = target_tsv_dir / "title_aka_type.tsv"
    title_aka_attribute_tsv_path = target_tsv_dir / "title_aka_attribute.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitleAka] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitleAka,
                skip_header=True,
            )
        )
        title_aka = stack.enter_context(
            TsvWriter.open(
                title_aka_tsv_path,
                with_index=True,
                header=[
                    "id",
                    "title_id",
                    "title",
                    "region",
                    "language",
                    "is_original_title",
                ],
            )
        )
        title_aka_type = stack.enter_context(
            TsvWriter.open(
                title_aka_type_tsv_path,
                with_index=True,
                header=["id", "title_aka_id", "ta_type_id"],
            )
        )
        title_aka_attribute = stack.enter_context(
            TsvWriter.open(
                title_aka_attribute_tsv_path,
                with_index=True,
                header=["id", "title_aka_id", "ta_attribute"],
            )
        )

        for row in source:
            title_aka.write_line(
                [
                    row.title_id,
                    row.title,
                    row.region,
                    row.language,
                    row.is_original_title,
                ]
            )

            if row.types is not None:
                for ta_type in row.types:
                    title_aka_type.write_line([title_aka.line_number, ta_type])

            if row.attributes is not None:
                for ta_attribute in row.attributes:
                    title_aka_attribute.write_line(
                        [title_aka.line_number, ta_attribute]
                    )

    return [title_aka_tsv_path, title_aka_type_tsv_path, title_aka_attribute_tsv_path]


def _normalize_title_basics(
    source_tsv_dir: Path, target_tsv_dir: Path
) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.basics.tsv"
    assert source_tsv_path.exists()

    title_tsv_path = target_tsv_dir / "title.tsv"
    title_genre_tsv_path = target_tsv_dir / "title_genre.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitleBasic] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitleBasic,
                skip_header=True,
            )
        )
        title = stack.enter_context(
            TsvWriter.open(
                title_tsv_path,
                with_index=False,
                header=[
                    "id",
                    "title_type_id",
                    "primary_title",
                    "original_title",
                    "is_adult",
                    "start_year",
                    "end_year",
                    "runtime_minutes",
                ],
            )
        )
        title_genre = stack.enter_context(
            TsvWriter.open(
                title_genre_tsv_path,
                with_index=False,
                header=["title_id", "genre_id"],
            )
        )

        for row in source:
            title.write_line(
                [
                    row.t_const,
                    row.title_type,
                    row.primary_title,
                    row.original_title,
                    row.is_adult,
                    row.start_year,
                    row.end_year,
                    row.runtime_minutes,
                ]
            )

            if row.genres is not None:
                for genre in row.genres:
                    title_genre.write_line([row.t_const, genre])

    return [title_tsv_path, title_genre_tsv_path]


def _normalize_title_crew(source_tsv_dir: Path, target_tsv_dir: Path) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.crew.tsv"
    assert source_tsv_path.exists()

    crew_tsv_path = target_tsv_dir / "crew.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitleCrew] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitleCrew,
                skip_header=True,
            )
        )
        crew = stack.enter_context(
            TsvWriter.open(
                crew_tsv_path,
                with_index=True,
                header=["id", "title_id", "crew_type_id", "name_id"],
            )
        )

        for row in source:
            if row.directors is not None:
                for n_const in row.directors:
                    crew.write_line([row.t_const, CrewType.DIRECTOR, n_const])

            if row.writers is not None:
                for n_const in row.writers:
                    crew.write_line([row.t_const, CrewType.WRITER, n_const])

    return [crew_tsv_path]


def _normalize_title_episode(
    source_tsv_dir: Path, target_tsv_dir: Path
) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.episode.tsv"
    assert source_tsv_path.exists()

    episode_tsv_path = target_tsv_dir / "episode.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitleEpisode] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitleEpisode,
                skip_header=True,
            )
        )
        episode = stack.enter_context(
            TsvWriter.open(
                episode_tsv_path,
                with_index=True,
                header=[
                    "id",
                    "title_id",
                    "parent_title_id",
                    "season_number",
                    "episode_number",
                ],
            )
        )

        for row in source:
            episode.write_line(
                [row.t_const, row.parent_t_const, row.season_number, row.episode_number]
            )

    return [episode_tsv_path]


def _normalize_title_principals(
    source_tsv_dir: Path, target_tsv_dir: Path
) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.principals.tsv"
    assert source_tsv_path.exists()

    principal_tsv_path = target_tsv_dir / "principal.tsv"
    principal_character_tsv_path = target_tsv_dir / "principal_character.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitlePrincipal] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitlePrincipal,
                skip_header=True,
            )
        )
        principal = stack.enter_context(
            TsvWriter.open(
                principal_tsv_path,
                with_index=True,
                header=["id", "title_id", "principal_category_id", "name_id", "job"],
            )
        )
        principal_character = stack.enter_context(
            TsvWriter.open(
                principal_character_tsv_path,
                with_index=True,
                header=["id", "principal_id", "p_character"],
            )
        )

        for row in source:
            principal.write_line([row.t_const, row.category, row.n_const, row.job])

            if row.characters is not None:
                for character in row.characters:
                    principal_character.write_line([principal.line_number, character])

    return [principal_tsv_path, principal_character_tsv_path]


def _normalize_title_ratings(
    source_tsv_dir: Path, target_tsv_dir: Path
) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "title.ratings.tsv"
    assert source_tsv_path.exists()

    rating_tsv_path = target_tsv_dir / "rating.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[TitleRating] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=TitleRating,
                skip_header=True,
            )
        )
        rating = stack.enter_context(
            TsvWriter.open(
                rating_tsv_path,
                with_index=True,
                header=["id", "title_id", "average_rating", "num_votes"],
            )
        )

        for row in source:
            rating.write_line([row.t_const, row.average_rating, row.num_votes])

    return [rating_tsv_path]


def _normalize_name_basics(
    source_tsv_dir: Path, target_tsv_dir: Path
) -> Sequence[Path]:
    assert source_tsv_dir.exists() and target_tsv_dir.exists()

    source_tsv_path = source_tsv_dir / "name.basics.tsv"
    assert source_tsv_path.exists()

    name_tsv_path = target_tsv_dir / "name.tsv"
    name_profession_tsv_path = target_tsv_dir / "name_profession.tsv"
    name_known_for_title_tsv_path = target_tsv_dir / "name_known_for_title.tsv"

    with contextlib.ExitStack() as stack:
        source: TsvReader[NameBasic] = stack.enter_context(
            TsvReader.open(
                source_tsv_path,
                target_class=NameBasic,
                skip_header=True,
            )
        )
        name = stack.enter_context(
            TsvWriter.open(
                name_tsv_path,
                with_index=False,
                header=["id", "primary_name", "birth_year", "death_year"],
            )
        )
        name_profession = stack.enter_context(
            TsvWriter.open(
                name_profession_tsv_path,
                with_index=True,
                header=["id", "name_id", "profession"],
            )
        )
        name_known_for_title = stack.enter_context(
            TsvWriter.open(
                name_known_for_title_tsv_path,
                with_index=True,
                header=["id", "name_id", "title_id"],
            )
        )

        for row in source:
            name.write_line(
                [row.n_const, row.primary_name, row.birth_year, row.death_year]
            )

            for profession in row.primary_profession:
                name_profession.write_line([row.n_const, profession])

            if row.known_for_titles is not None:
                for title_id in row.known_for_titles:
                    name_known_for_title.write_line([row.n_const, title_id])

    return [name_tsv_path, name_profession_tsv_path, name_known_for_title_tsv_path]

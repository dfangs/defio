from __future__ import annotations

import json
from collections.abc import Sequence
from enum import StrEnum, unique
from typing import final

import attrs
from attrs import define
from typing_extensions import override

from defio.dataset.utils import NullableFields, TsvReadable


@unique
class Genre(StrEnum):
    ACTION = "Action"
    ADULT = "Adult"
    ADVENTURE = "Adventure"
    ANIMATION = "Animation"
    BIOGRAPHY = "Biography"
    COMEDY = "Comedy"
    CRIME = "Crime"
    DOCUMENTARY = "Documentary"
    DRAMA = "Drama"
    FAMILY = "Family"
    FANTASY = "Fantasy"
    FILM_NOIR = "Film-Noir"
    GAME_SHOW = "Game-Show"
    HISTORY = "History"
    HORROR = "Horror"
    MUSIC = "Music"
    MUSICAL = "Musical"
    MYSTERY = "Mystery"
    NEWS = "News"
    REALITY_TV = "Reality-TV"
    ROMANCE = "Romance"
    SCI_FI = "Sci-Fi"
    SHORT = "Short"
    SPORT = "Sport"
    TALK_SHOW = "Talk-Show"
    THRILLER = "Thriller"
    WAR = "War"
    WESTERN = "Western"


@unique
class TitleType(StrEnum):
    MOVIE = "movie"
    SHORT = "short"
    TV_EPISODE = "tvEpisode"
    TV_MINI_SERIES = "tvMiniSeries"
    TV_MOVIE = "tvMovie"
    TV_PILOT = "tvPilot"
    TV_SERIES = "tvSeries"
    TV_SHORT = "tvShort"
    TV_SPECIAL = "tvSpecial"
    VIDEO = "video"
    VIDEO_GAME = "videoGame"


@unique
class TitleAltType(StrEnum):
    ALTERNATIVE = "alternative"
    DVD = "dvd"
    FESTIVAL = "festival"
    IMDB_DISPLAY = "imdbDisplay"
    ORIGINAL = "original"
    TV = "tv"
    VIDEO = "video"
    WORKING = "working"


@unique
class CrewType(StrEnum):
    DIRECTOR = "director"
    WRITER = "writer"


@unique
class PrincipalCategory(StrEnum):
    ACTOR = "actor"
    ACTRESS = "actress"
    ARCHIVE_FOOTAGE = "archive_footage"
    ARCHIVE_SOUND = "archive_sound"
    CINEMATOGRAPHER = "cinematographer"
    COMPOSER = "composer"
    DIRECTOR = "director"
    EDITOR = "editor"
    PRODUCER = "producer"
    PRODUCTION_DESIGNER = "production_designer"
    SELF = "self"
    WRITER = "writer"


@final
@define(frozen=True, eq=False)
class TitleAka(TsvReadable):
    title_id: str
    ordering: int
    title: str
    region: str | None
    language: str | None
    types: Sequence[TitleAltType] | None
    attributes: Sequence[str] | None
    is_original_title: bool | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitleAka:
        assert len(fields) == len(attrs.fields(TitleAka))

        return TitleAka(
            title_id=fields.require(0),
            ordering=int(fields.require(1)),
            title=fields.require(2),
            region=fields.get(3),
            language=fields.get(4),
            types=fields.map(
                5, lambda f: [TitleAltType(aka_type) for aka_type in f.split("\x02")]
            ),
            attributes=fields.map(6, lambda f: f.split("\x02")),
            is_original_title=fields.map(7, lambda f: bool(int(f))),
        )


@final
@define(frozen=True, eq=False)
class TitleBasic(TsvReadable):
    t_const: str  # PKEY
    title_type: TitleType
    primary_title: str
    original_title: str
    is_adult: bool
    start_year: int | None
    end_year: int | None
    runtime_minutes: int | None
    genres: Sequence[Genre] | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitleBasic:
        assert len(fields) == len(attrs.fields(TitleBasic))

        return TitleBasic(
            t_const=fields.require(0),
            title_type=TitleType(fields.require(1)),
            primary_title=fields.require(2),
            original_title=fields.require(3),
            is_adult=bool(int(fields.require(4))),
            start_year=fields.map(5, int),
            end_year=fields.map(6, int),
            runtime_minutes=fields.map(7, int),
            genres=fields.map(8, lambda f: [Genre(genre) for genre in f.split(",")]),
        )


@final
@define(frozen=True, eq=False)
class TitleCrew(TsvReadable):
    t_const: str  # PKEY
    directors: Sequence[str] | None
    writers: Sequence[str] | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitleCrew:
        assert len(fields) == len(attrs.fields(TitleCrew))

        return TitleCrew(
            t_const=fields.require(0),
            directors=fields.map(1, lambda f: f.split(",")),
            writers=fields.map(2, lambda f: f.split(",")),
        )


@final
@define(frozen=True, eq=False)
class TitleEpisode(TsvReadable):
    t_const: str  # PKEY
    parent_t_const: str
    season_number: int | None
    episode_number: int | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitleEpisode:
        assert len(fields) == len(attrs.fields(TitleEpisode))

        return TitleEpisode(
            t_const=fields.require(0),
            parent_t_const=fields.require(1),
            season_number=fields.map(2, int),
            episode_number=fields.map(3, int),
        )


@final
@define(frozen=True, eq=False)
class TitlePrincipal(TsvReadable):
    t_const: str
    ordering: int
    n_const: str
    category: PrincipalCategory
    job: str | None
    characters: Sequence[str] | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitlePrincipal:
        assert len(fields) == len(attrs.fields(TitlePrincipal))

        return TitlePrincipal(
            t_const=fields.require(0),
            ordering=int(fields.require(1)),
            n_const=fields.require(2),
            category=PrincipalCategory(fields.require(3)),
            job=fields.get(4),
            characters=fields.map(5, json.loads),
        )


@final
@define(frozen=True, eq=False)
class TitleRating(TsvReadable):
    t_const: str  # PKEY
    average_rating: float
    num_votes: int

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> TitleRating:
        assert len(fields) == len(attrs.fields(TitleRating))

        return TitleRating(
            t_const=fields.require(0),
            average_rating=float(fields.require(1)),
            num_votes=int(fields.require(2)),
        )


@final
@define(frozen=True, eq=False)
class NameBasic(TsvReadable):
    n_const: str  # PKEY
    primary_name: str
    birth_year: int | None
    death_year: int | None
    primary_profession: Sequence[str]
    known_for_titles: Sequence[str] | None

    @override
    @staticmethod
    def from_tsv(fields: NullableFields) -> NameBasic:
        assert len(fields) == len(attrs.fields(NameBasic))

        return NameBasic(
            n_const=fields.require(0),
            primary_name=fields.require(1),
            birth_year=fields.map(2, int),
            death_year=fields.map(3, int),
            primary_profession=fields.require(4).split(","),
            known_for_titles=fields.map(5, lambda f: f.split(",")),
        )

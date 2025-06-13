#!/usr/bin/env python3
import os
import sys
import csv
from enum import Enum
import argparse
from mutagen.id3 import ID3
from mutagen.mp3 import MP3
from typing import Dict, Union


class Tag(Enum):
    # Defining map_from as an array to support fallback extraction tag names
    ALBUM = {"tag_name": "album", "map_from": "TALB", "is_int": False}
    ARTIST = {"tag_name": "artist", "map_from": "TPE1", "is_int": False}
    GENRE = {"tag_name": "genre", "map_from": "TCON", "is_int": False}
    TITLE = {"tag_name": "title", "map_from": "TIT2", "is_int": False}
    TRACK = {"tag_name": "track", "map_from": "TRCK", "is_int": True}
    YEAR = {"tag_name": "year", "map_from": "TDRC", "is_int": True}
    LENGTH = {"tag_name": "length", "map_from": "TLEN", "is_int": True}

    @classmethod
    def from_tag_name(cls, tag_name: str) -> Union["Tag", None]:
        for tag in cls:
            if tag.tag_name == tag_name:
                return tag

        return None

    @property
    def tag_name(self) -> str:
        return self.value["tag_name"]

    @property
    def map_from(self) -> str:
        return self.value["map_from"]

    @property
    def is_int(self) -> bool:
        return self.value["is_int"]

    def conditionally_quote(self, s):
        return s if self.value["is_int"] else f'"{s}"'


def get_tag(id3: ID3, tag_name: str) -> str:
    v = id3.get(tag_name)
    if tag_name == "TLEN":
        print(f'TLEN: {v}', file=sys.stderr)

    return str(v.text[0] if v else "")


def truncate_at(s: str, ch: str):
    idx = s.find(ch) if s else -1
    return s[:idx] if idx >= 0 else s


def get_tags(filename: str) -> Union[Dict[str, Union[str, int]], None]:
    print(filename, file=sys.stderr)
    try:
        tags = ID3(filename)
    except Exception:
        return None

    my_tags = {tag.tag_name: get_tag(tags, tag.map_from) for tag in Tag}

    # Clip any MM-DD data on the year
    YEAR = Tag.YEAR.tag_name
    my_tags[YEAR] = truncate_at(my_tags[YEAR], "-")

    # Clip total track from track number; e.g., "02/09" -> 2
    TRACK = Tag.TRACK.tag_name
    my_tags[TRACK] = truncate_at(my_tags[TRACK], "/")

    if not my_tags.get(Tag.LENGTH.tag_name):
        audio = MP3(filename)
        my_tags[Tag.LENGTH.tag_name] = int(audio.info.length * 1000)  # seconds -> msec

    print(f'Audio length: {my_tags.get(Tag.LENGTH.tag_name)}', file=sys.stderr)
    return my_tags


def filename_extension(filename: str) -> Union[str, None]:
    ext = None if not filename else os.path.splitext(filename)[1]
    return None if not ext else ext.lower()


def is_audio_file(filename: str) -> bool:
    return filename_extension(filename) in [".mp3", ".flac", ".m4a"]


def quoted(tag: str, v: Union[str, int]) -> str:
    return str(v) if tag in ["track", "year"] else f'"{v}"'


def get_and_print_tags(writer: csv.writer, filename: str) -> None:
    try:
        tags = get_tags(filename)
        if not tags:
            return
    except Exception as e:
        raise Exception(f"Error getting tags for {filename}: {e}")

    # For protection against the error
    # "UnicodeEncodeError: 'utf-8' codec can't encode character '\udc92' in position 134: surrogates not allowed"
    # in the print statement
    fn = filename.encode("utf-8", "surrogateescape").decode("ISO-8859-1")
    if fn != filename:
        print(f'Warning: corrupted filename at "{fn}"', file=sys.stderr)

    writer.writerow([tags[tag.tag_name] for tag in Tag] + [fn])


def walk_and_display_metadata(path: str) -> None:
    writer = csv.writer(sys.stdout, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow([tag.name.lower() for tag in Tag] + ["filename"])

    for root, _, filenames in os.walk(path):
        for filename in filenames:
            if is_audio_file(filename):
                try:
                    get_and_print_tags(writer, os.path.join(root, filename))
                except Exception as e:
                    print(str(e), file=sys.stderr)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="*")
    args = parser.parse_args()

    paths = args.paths if len(args.paths) > 0 else ["."]

    for path in paths:
        walk_and_display_metadata(path)

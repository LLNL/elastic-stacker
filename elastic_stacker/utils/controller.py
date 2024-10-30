import json
import os
import re

import httpx
from pathlib import Path
from collections import defaultdict

PURGE_PROMPT = """
{count} files in the data directory do not match with a resource on the server.
Unless this operation was filtered to only some of the resources, that means
those resources were probably deleted since the last system dump.

Files to be purged:
{purge_list}

Delete these {count} resource dump files? [y/N]:"""


def without_keys(d: dict, keys: list[str]):
    """
    Returns the given dict d without the specified keys, which may be nested
    keys separated by dots (e.g. foo.bar.baz). This thin wrapper splits the keys
    at the dots before passing them down to _without_keys where the real work
    happens.
    """
    keys = {tuple(key.split(".")) for key in keys}
    return _without_keys(d, keys)


def _without_keys(d: dict, keys: set[tuple[str]]):
    """
    Given a nested dict and a set of keys to delete, selectively walk down the
    structure, deleting keys as as they're found. Only walks the parts of the
    tree that are candidates for deletion.

    This is frankly over-engineered, but is about as fast as you can do this;
    the efficiency comes from never walking a part of the tree more than once.
    For example; if I want to delete a.b.c.d.e.f and a.b.c.g.h.i, I still walk
    the tree under a.b.c only once, instead of twice, by first building up a
    list of keys to delete at each next level of the tree.
    """

    delete_map = defaultdict(set)
    for k in keys:
        if len(k) == 1:
            # None means this key should be deleted at this level
            delete_map[k[0]] = None
        elif delete_map[k[0]] is not None:
            # if k[0] is present at this level, delete k[1:] at the next level
            delete_map[k[0]].add(k[1:])

    keys = list(d.keys())

    for k in keys:
        if delete_map[k] is None:
            del d[k]
        elif delete_map[k] and isinstance(d[k], dict):
            d[k] = _without_keys(d[k], delete_map[k])

    return d


def _abs_path(p: os.PathLike):
    return Path(p).expanduser().resolve()


def _walk_files_in_path(p: Path, include_dirs: bool = False):
    walk_root = str(_abs_path(p))
    for root, dirs, files in os.walk(walk_root):
        for filename in files:
            yield Path(root) / filename


class GenericController:
    """
    Stacker groups API resources as "controllers" which have methods
    for handling the specific behavior for interacting with those API
    resources.

    These controllers have some methods in common related to file reading and
    writing and argument processing, so they all inherit from this base class.
    """

    _client: httpx.Client
    _options: dict
    _resource_directory: str = ""
    _subs: dict
    _touched_files: set[Path]
    _excluded_attributes: list = []

    def __init__(self, client: httpx.Client, subs: dict = {}, **options):
        self._client = client
        self._options = options
        self._subs = subs
        for name, sub in self._subs.items():
            self._subs[name]["search"] = re.compile(sub["search"])

        self._touched_files = set()

    def _run_substitutions(self, value):
        for name, sub in sorted(self._subs.items()):
            search = sub["search"]
            replace = sub["replace"]
            value = re.sub(search, replace, value)
        return value

    def _write_file(self, path: os.PathLike, obj: dict):
        obj = without_keys(obj, self._excluded_attributes)
        path = _abs_path(path)
        output = json.dumps(obj, indent=4, sort_keys=True)
        output = self._run_substitutions(output)
        with open(path, "w") as fh:
            fh.write(output)
        self._touched_files.add(path)

    def _read_file(self, path: os.PathLike):
        with open(path, "r") as fh:
            value = fh.read()
        value = self._run_substitutions(value)
        return json.loads(value)

    def _clean_params(self, params: dict):
        # httpx includes query parameters even if their value is None
        # (see https://www.python-httpx.org/compatibility/#query-parameters).
        # usually I'd add a pre-request hook to remove null parameters, but
        # httpx also does not let the user modify the request before it's sent
        # (see https://www.python-httpx.org/compatibility/#event-hooks)
        return {k: v for k, v in params.items() if v is not None}

    def _get_working_dir(
        self, data_directory: os.PathLike = None, create=False
    ) -> Path:
        # TODO: this work should be done in the constructor
        if data_directory is None:
            data_directory = self._options.get("data_directory")
        else:
            data_directory = Path(data_directory)

        working_directory = data_directory / self._resource_directory

        if create:
            working_directory.mkdir(parents=True, exist_ok=True)

        if not working_directory.is_dir():
            raise NotADirectoryError(
                "The data_directory {} is not valid directory".format(working_directory)
            )

        self.working_directory = _abs_path(working_directory)
        return working_directory

    def _untouched_files(self, relative=False):
        untouched = set()
        for p in _walk_files_in_path(self.working_directory):
            abs_p = _abs_path(p)
            rel_p = p.relative_to(self.working_directory.parents[0])
            if _abs_path(p) not in self._touched_files:
                if relative:
                    untouched.add(rel_p)
                else:
                    untouched.add(abs_p)
        return untouched

    def _purge_untouched_files(self, force: bool = False):
        untouched = self._untouched_files()
        relative_untouched = map(str, self._untouched_files(relative=True))
        if not untouched:
            return
        purge_list = "\n".join(relative_untouched)
        prompt = PURGE_PROMPT.format(count=len(untouched), purge_list=purge_list)
        confirmed = force or input(prompt) in {"Y", "y", "yes", "Yes", "YES"}
        if confirmed:
            for f in untouched:
                f.unlink()
        else:
            print("Cancelling purge of deleted files.")


class ElasticsearchAPIController(GenericController):
    def _depaginate(self, method, key, page_size=10, **kwargs):
        """
        Elasticsearch presents some of its APIs paginated, so rather than dump
        all of them in one request we can turn that pagination into a nice,
        Pythonic generator.
        """
        offset = 0
        results = {"count": float("inf")}
        while offset < results["count"]:
            results = method(offset=offset, size=page_size, **kwargs)
            for result in results[key]:
                offset += 1
                yield result


class FleetAPIController(GenericController):
    def _depaginate(self, method, perPage: int = None, **kwargs):
        """
        Fleet Server has paginated APIs too, but where Elasticsearch accepts
        an offset parameter ("from"), Fleet accepts only a page number,
        so the pagination logic has to be a little different.
        """
        page = 1
        results = {"items": True}
        while results["items"]:  # returns the empty list when complete
            results = method(page=page, perPage=perPage, **kwargs)
            for result in results["items"]:
                yield result
            page += 1

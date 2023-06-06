import os

import logging
import pathlib

import httpx
import dotwiz
import certifi
from marshmallow import (
    Schema,
    fields,
    validate,
    pre_load,
    post_load,
    ValidationError,
)

from api_clients import KibanaClient, ElasticsearchClient

# TODO: debug logging for the schema loader
logger = logging.getLogger("elastic_stacker")

GLOBAL_DEFAULT_PROFILE = {
    "elasticsearch": {"base_url": "https://localhost:9200"},
    "kibana": {"base_url": "https://localhost:5601"},
    "load": {"include_managed": False},
    "dump": {
        "temp_copy": False,
        "delete_after_import": False,
        "allow_failure": False,
        "retries": 0
    },
    "data_directory": "./stacker_dump"
}


class PathField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return pathlib.Path(value).expanduser()


class PathValidator(validate.Validator):
    def __init__(self, should_exist=True, file_ok=True, dir_ok=True) -> None:
        self.should_exist = should_exist
        self.file_ok = file_ok
        self.dir_ok = dir_ok

    def __call__(self, value: os.PathLike) -> os.PathLike:
        path = os.path.expanduser(value)
        is_file = os.path.isfile(path)
        is_dir = os.path.isdir(path)
        exists = is_file or is_dir

        if exists ^ self.should_exist:
            raise ValidationError(
                "path should {maybe_not}exist".format(
                    maybe_not="" if self.should_exist else "not "
                )
            )
        if is_file ^ self.file_ok:
            raise ValidationError(
                "path should {maybe_not}be a file".format(
                    maybe_not="" if self.file_ok else "not "
                )
            )
        if is_dir ^ self.dir_ok:
            raise ValidationError(
                "path should {maybe_not}be a directory".format(
                    maybe_not="" if self.dir_ok else "not "
                )
            )
        return value


class BasicAuthSchema(Schema):
    username = fields.String(required=True)
    password = fields.String(required=True)

    @post_load
    def make_auth_tuple(self, data, **kwargs):
        return (data["username"], data["password"])


class TLSConfigSchema(Schema):
    cert = PathField(validate=PathValidator(dir_ok=False))
    key = PathField(validate=PathValidator(dir_ok=False))


class APIClientConfigSchema(Schema):
    base_url = fields.Url()
    headers = fields.Dict(keys=fields.String(), values=fields.String())
    verify = PathField(validate=PathValidator(dir_ok=False, should_exist=True))
    auth = fields.Nested(BasicAuthSchema())
    tls = fields.Nested(TLSConfigSchema())

    @post_load
    def fix_tls(self, client_settings, **kwargs):
        # httpx `cert` argument is a tuple of (cert, key) and TOML doesn't have tuples
        # so it got a nested table instead
        if "tls" in client_settings:
            tls = client_settings["tls"]
            if tls["cert"]:
                if tls["key"]:
                    client_settings["cert"] = (tls["cert"], tls["key"])
                else:
                    client_settings["cert"] = tls["cert"]
            del client_settings["tls"]
        return client_settings

class DumperConfigSchema(Schema):
    include_managed = fields.Boolean()
    data_directory = PathField(validate=PathValidator(file_ok=False))

class LoaderConfigSchema(Schema):
    data_directory = PathField(validate=PathValidator(file_ok=False, should_exist=True))
    temp_copy = fields.Boolean()
    delete_after_import = fields.Boolean()
    allow_failure = fields.Boolean()
    retries = fields.Integer()

class ProfileSchema(Schema):
    data_directory = PathField(validate=PathValidator(file_ok=False))
    client = fields.Nested(APIClientConfigSchema)
    kibana = fields.Nested(APIClientConfigSchema)
    elasticsearch = fields.Nested(APIClientConfigSchema)
    load = fields.Nested(LoaderConfigSchema)
    dump = fields.Nested(LoaderConfigSchema)


class ConfigFileSchema(Schema):
    default = fields.Nested(ProfileSchema())
    profile = fields.Mapping(fields.String(), fields.Nested(ProfileSchema()))

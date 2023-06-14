import os

import logging
from pathlib import Path
import re

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

# TODO: debug logging for the schema loader
logger = logging.getLogger("elastic_stacker")


class BaseSchema(Schema):
    @pre_load
    def clear_none_values(self, data, **kwargs):
        # none values in the data interfere with the precedence of defaults.
        to_pop = []
        for k, v in data.items():
            if v is None:
                to_pop.append(k)
        for k in to_pop:
            data.pop(k)
        return data


class PathField(fields.Field):
    def _serialize(self, value, attr, obj, **kwargs):
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return Path(value).expanduser()


class PathValidator(validate.Validator):
    def __init__(
        self,
        exist_ok: bool = True,
        must_exist: bool = False,
        file_ok: bool = True,
        dir_ok: bool = True,
    ) -> None:
        self.must_exist = must_exist
        self.exist_ok = exist_ok
        self.file_ok = file_ok
        self.dir_ok = dir_ok

    def __call__(self, value: os.PathLike) -> os.PathLike:
        path = os.path.expanduser(value)
        is_file = os.path.isfile(path)
        is_dir = os.path.isdir(path)
        exists = is_file or is_dir

        if exists and not self.exist_ok:
            raise ValidationError("path should not exist")
        if self.must_exist and not exists:
            raise ValidationError("path must exist")
        if is_file and not self.file_ok:
            raise ValidationError("path should not be a file")
        if is_dir and not self.dir_ok:
            raise ValidationError("path should not be a directory")
        return value


class BasicAuthSchema(BaseSchema):
    username = fields.String(required=True)
    password = fields.String(required=True)

    @post_load
    def make_auth_tuple(self, data, **kwargs):
        return (data["username"], data["password"])


class TLSConfigSchema(BaseSchema):
    cert = PathField(validate=PathValidator(dir_ok=False))
    key = PathField(validate=PathValidator(dir_ok=False))


class APIClientConfigSchema(BaseSchema):
    base_url = fields.Url()
    headers = fields.Dict(keys=fields.String(), values=fields.String())
    verify = PathField(validate=PathValidator(dir_ok=False, must_exist=True))
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


class ControllerOptionsSchema(BaseSchema):
    data_directory = PathField(validate=PathValidator(file_ok=False))
    watcher_users = fields.Dict(fields.String, fields.String)
    # TODO: add all the dumpers' and loaders' one-off arguments here (include_managed, etc.)


class SubstitutionSchema(BaseSchema):
    search = fields.String(required=True)
    replace = fields.String(required=True)


class ProfileSchema(BaseSchema):
    client = fields.Nested(APIClientConfigSchema)
    kibana = fields.Nested(APIClientConfigSchema)
    elasticsearch = fields.Nested(APIClientConfigSchema)
    options = fields.Nested(ControllerOptionsSchema)
    substitutions = fields.Dict(fields.String(), fields.Nested(SubstitutionSchema()))


class ConfigFileSchema(BaseSchema):
    default = fields.Nested(ProfileSchema())
    profiles = fields.Dict(fields.String(), fields.Nested(ProfileSchema()))

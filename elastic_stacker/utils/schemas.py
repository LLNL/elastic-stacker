from enum import Enum
import os

import logging
from pathlib import Path

from marshmallow import (
    Schema,
    fields,
    validate,
    pre_load,
    post_load,
    ValidationError,
)

LOGLEVELS = ["DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"]
LOGLEVELS = LOGLEVELS + [l.lower() for l in LOGLEVELS]

logger = logging.getLogger("elastic_stacker")


class BaseSchema(Schema):
    """
    All schemas inherit from this one, which introduces a preprocessing step.
    We want the behavior to be that if the value for a given key is None, it
    is omitted from the config.
    """

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
    """
    A special Marshmallow field which deserializes as a pathlib.Path.
    """

    def _serialize(self, value, attr, obj, **kwargs):
        return str(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return Path(value).expanduser()


class PathValidator(validate.Validator):
    """
    A Marshmallow validator for path fields to introduce additional checks on
    the file state (exists, is_dir, is_file, etc.0)
    """

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
    """
    A schema for a pair of username and password for authentication.
    Returns a tuple for the sake of, e.g. an httpx.Client.
    """

    username = fields.String(required=True)
    password = fields.String(required=True)

    @post_load
    def make_auth_tuple(self, data, **kwargs):
        return (data["username"], data["password"])


class LoggerConfigSchema(BaseSchema):
    """
    schema for logger configuration
    """

    level = fields.String(validate=validate.OneOf(LOGLEVELS))
    ecs = fields.Boolean()


class TLSConfigSchema(BaseSchema):
    """
    A schema for a paired certificate and private key, e.g. for mutual TLS
    authentication.
    """

    cert = PathField(validate=PathValidator(dir_ok=False))
    key = PathField(validate=PathValidator(dir_ok=False))


class APIClientConfigSchema(BaseSchema):
    """
    The schema for a generic API client. Supports a subset of the arguments to
    httpx.Client.__init__, so you can unpack the resultant object directly
    into the constructor.
    """

    base_url = fields.Url()
    headers = fields.Dict(keys=fields.String(), values=fields.String())
    verify = PathField(validate=PathValidator(dir_ok=False, must_exist=True))
    auth = fields.Nested(BasicAuthSchema())
    tls = fields.Nested(TLSConfigSchema())
    timeout = fields.Float()


class ControllerOptionsSchema(BaseSchema):
    """
    A schema for additional options to be used by all controllers.
    In the future, this should provide default values for all arguments
    accepted by controller.dump() and .load() (e.g. include_managed.)
    """

    data_directory = PathField(validate=PathValidator(file_ok=False))
    watcher_users = fields.Dict(fields.String, fields.String)
    # TODO: add all the dumpers' and loaders' one-off arguments here (include_managed, etc.)


class SubstitutionSchema(BaseSchema):
    """
    A schema for a regex substitution, and the string to replace it with.
    """

    search = fields.String(required=True)
    replace = fields.String(required=True)


class ProfileSchema(BaseSchema):
    """
    The schema for a Stacker configuration profile, consisting of
    Elasticsearch and Kibana client configs, dump/load options, and regex
    substitutions for pre/postprocessing.
    Note that kibana and elasticsearch both inherit from and override "client"
    """

    client = fields.Nested(APIClientConfigSchema)
    kibana = fields.Nested(APIClientConfigSchema)
    elasticsearch = fields.Nested(APIClientConfigSchema)
    options = fields.Nested(ControllerOptionsSchema)
    substitutions = fields.Dict(fields.String(), fields.Nested(SubstitutionSchema()))
    log = fields.Nested(LoggerConfigSchema)


class ConfigFileSchema(BaseSchema):
    """
    The schema for the config file at large.
    Defines a set of default values, plus some number of config profiles
    which override the default values.
    """

    default = fields.Nested(ProfileSchema())
    profiles = fields.Dict(fields.String(), fields.Nested(ProfileSchema()))

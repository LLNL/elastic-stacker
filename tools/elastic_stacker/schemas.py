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
logger = logging.getLogger("kibanaio")

GLOBAL_DEFAULTS = {
    "elasticsearch": {"base_url": "https://localhost:9200"},
    "kibana": {"base_url": "https://localhost:5601"},
}

USER_DEFAULTS = {}


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
    def make_auth(self, data, **kwargs):
        return httpx.BasicAuth(**data)


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
    def fill_defaults_and_correct_names(self, client_settings, **kwargs):
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


class KibanaClientSchema(APIClientConfigSchema):
    @post_load
    def make_client(self, data, **kwargs):
        return KibanaClient(**data)


class ElasticsearchClientSchema(APIClientConfigSchema):
    @post_load
    def make_client(self, data, **kwargs):
        return ElasticsearchClient(**data)

class StackConfigSchema(APIClientConfigSchema):
    kibana = fields.Nested(APIClientConfigSchema)
    elasticsearch = fields.Nested(APIClientConfigSchema)
    @pre_load
    def load_defaults(self, data, **kwargs):
        """
        All this shuffling and updating is to apply the following hierarchy of defaults:
        - client-specific (e.g. stack.pre.kibana.base_url = "https://foo.bar")
        - stack-specific (e.g. stack.pre.headers = {Authorization: "BazQux=="})
        - config defaults (e.g. default.stack.ca = "./ca.crt")
        - hardcoded global defaults from the top of this file
        """
        final_data = GLOBAL_DEFAULTS
        stack_defaults = dict(data)
        if "elasticsearch" in stack_defaults:
            del stack_defaults["elasticsearch"]
        if "kibana" in stack_defaults:
            del stack_defaults["kibana"]

        for client_type in ["elasticsearch", "kibana"]:
            final_data[client_type] = GLOBAL_DEFAULTS[client_type]
            final_data[client_type].update(USER_DEFAULTS.get(client_type, {}))
            final_data[client_type].update(stack_defaults)
            final_data[client_type].update(data.get(client_type, {}))

        return final_data

class StackSchema(StackConfigSchema):
    @post_load
    def make_clients(self, data, **kwargs):
        data["kibana"] = KibanaClient(**data["kibana"])
        data["elasticsearch"] = ElasticsearchClient(**data["elasticsearch"])
        return data


class DefaultsConfigSchema(Schema):
    stack = fields.Nested(StackConfigSchema())


class ConfigSchema(Schema):
    default = fields.Nested(DefaultsConfigSchema())
    stack = fields.Mapping(fields.String(), fields.Nested(StackSchema()))

    @pre_load
    def populate_defaults(self, data, **kwargs):
        global USER_DEFAULTS
        USER_DEFAULTS = DefaultsConfigSchema().load(data.get("default", {}))
        return data

    @post_load
    def make_dotwiz(self, data, **kwargs):
        # marshmallow schemas load to a dict by default,
        # but when deeply nested, the stacks of brackets and quotes looks pretty cluttered
        # after all, who wants thing["foo"]["bar"]["baz"]
        # or worse, because you may not know whether any given key was provided in the config
        # you get thing.get(foo, {}).get(bar, {}).get(baz, None) which is atrocious
        #
        # DotWiz is like a Namespace meets a default dict,
        # so you can access thing.foo.bar.baz and if any of the intermediate values are missing
        # it'll just return None. Much nicer for big nested documents like this.
        return dotwiz.DotWiz(data)

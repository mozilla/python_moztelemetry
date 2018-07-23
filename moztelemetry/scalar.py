"""This module contains:

Classes:
    - Scalar: A class which represents a scalar, instantiated by the scalar name

Objects:
    - SCALARS_YAML_PATH: The path to the Scalars.yaml file
    - REVISIONS: A map of release to the tip of the repo branch
"""


import requests
import yaml

from expiringdict import ExpiringDict
from .parse_scalars import ScalarType


SCALARS_YAML_PATH = '/toolkit/components/telemetry/Scalars.yaml'

REVISIONS = {'nightly': 'https://hg.mozilla.org/mozilla-central/rev/tip',
             'beta': 'https://hg.mozilla.org/releases/mozilla-beta/rev/tip',
             'release': 'https://hg.mozilla.org/releases/mozilla-release/rev/tip'}


class MissingScalarError(KeyError):
    pass


class Scalar(object):
    """A class representing a scalar"""

    _definition_cache = ExpiringDict(max_len=2**10, max_age_seconds=3600)

    def __init__(self, name, value, channel=None, revision=None, scalars_url=None):
        """
        Initialize a scalar from it's name.

        :param name: The name of the scalar
        :param value: The value of the scalar
        :param channel: The channel for which the scalar is defined. One of
                        nightly, aurora, beta, release. Defaults to nightly
        :param revision: The url of the revision to use for the scalar definition
        :param scalars_url: The url of the scalars.yml file to use for the
                            scalar definitions
        """
        picked = sum([bool(channel), bool(revision), bool(scalars_url)])

        if picked == 0:
            channel = 'nightly'
        elif picked > 1:
            raise ValueError('Can only use one of (channel, revision, scalars_url)')

        if channel:
            revision = REVISIONS[channel]  # raises error on improper channel
        if revision:
            scalars_url = revision.replace('rev', 'raw-file') + SCALARS_YAML_PATH

        self.definition = Scalar._get_scalar_definition(scalars_url, name)
        self.name = name
        self.value = value
        self.scalars_url = scalars_url

    def get_name(self):
        """Get the name of the scalar"""
        return self.name

    def get_value(self):
        """Get the value of the scalar"""
        return self.value

    def get_kind(self):
        """Get the kind of the scalar"""
        return self.definition.kind

    def __str__(self):
        return str(self.get_value())

    def __add__(self, other):
        if self.definition.kind != 'uint':
            raise AttributeError('Addition not specified for non-integer scalars')
        return Scalar(self.get_name(), self.value + other.value, scalars_url=self.scalars_url)

    @staticmethod
    def _parse_scalars(scalars):
        """Parse the scalars from the YAML file content to a dictionary of ScalarType(s).
        :return: A dictionary { 'full.scalar.label': ScalarType }
        """
        scalar_dict = {}

        # Scalars are defined in a fixed two-level hierarchy within the definition file.
        # The first level contains the category name, while the second level contains the
        # probe name (e.g. "category.name: probe: ...").
        for category_name in scalars:
            category = scalars[category_name]

            for probe_name in category:
                # We found a scalar type. Go ahead and parse it.
                scalar_definition = category[probe_name]
                # We pass |strict_type_checks=False| as we don't want to do any check
                # server side. This includes skipping the checks for the required keys.
                scalar_info = ScalarType(category_name, probe_name, scalar_definition,
                                         strict_type_checks=False)
                scalar_dict[scalar_info.label] = scalar_info

        return scalar_dict

    @staticmethod
    def _get_scalar_definition(url, metric):
        if url not in Scalar._definition_cache:
            content = requests.get(url).content
            Scalar._definition_cache[url] = Scalar._parse_scalars(yaml.load(content))

        definitions = Scalar._definition_cache[url]

        if metric.lower() not in definitions:
            raise MissingScalarError("Definition not found for {}".format(metric))

        return definitions.get(metric.lower(), {})

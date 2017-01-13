import requests
import yaml

from expiringdict import ExpiringDict


SCALARS_YAML_PATH = '/toolkit/components/telemetry/Scalars.yaml'

REVISIONS = {'nightly': 'https://hg.mozilla.org/mozilla-central/rev/tip',
             'aurora': 'https://hg.mozilla.org/releases/mozilla-aurora/rev/tip',
             'beta': 'https://hg.mozilla.org/releases/mozilla-beta/rev/tip',
             'release': 'https://hg.mozilla.org/releases/mozilla-release/rev/tip'}


class Scalar:
    """A class representing a scalar"""

    REQUIRED_FIELDS = {'bug_numbers', 'description', 'expires', 'kind',
                       'notification_emails'}

    OPTIONAL_FIELDS = {'cpp_guard', 'release_channel_collection', 'keyed'}

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

        definition = Scalar._get_scalar_definition(scalars_url, name)

        missing_fields = Scalar.REQUIRED_FIELDS - definition.viewkeys()
        assert not missing_fields, \
            "Definition is missing required fields {}".format(','.join(missing_fields))

        self.name = name
        self.value = value
        self.definition = definition
        self.scalars_url = scalars_url

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def get_definition(self):
        return self.definition

    def __str__(self):
        return str(self.get_value())

    def __add__(self, other):
        if self.definition['kind'] != 'uint':
            raise AttributeError('Addition not specified for non-integer scalars')
        return Scalar(self.name, self.value + other.value, scalars_url=self.scalars_url)

    @staticmethod
    def _yaml_unnest(defs, prefix=''):
        """The yaml definition file is nested - this functions unnests it.
        # example
        >>> test = {'browser.nav': {'clicks': {'description': 'desc', 'expires': 'never'}}}
        >>> yaml_unnest(test)
        # {'browser.nav.clicks': {'description': 'desc', 'expires': 'never'}}
        """
        def stop(val):
            return type(val) is not dict or val.viewkeys() & Scalar.REQUIRED_FIELDS

        new_defs, found = {}, list(defs.iteritems())

        while found:
            key, value = found.pop()
            if stop(value):
                new_defs[key] = value
            else:
                found += [('{}.{}'.format(key, k), v) for k, v in value.iteritems()]

        return new_defs

    @staticmethod
    def _get_scalar_definition(url, metric):
        if url not in Scalar._definition_cache:
            content = requests.get(url).content
            definitions = Scalar._yaml_unnest(yaml.load(content))
            Scalar._definition_cache[url] = definitions
        else:
            definitions = Scalar._definition_cache[url]

        return definitions.get(metric, {})


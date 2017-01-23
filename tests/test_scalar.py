import pytest
import responses

from moztelemetry.scalar import Scalar


scalar_file = '/toolkit/components/telemetry/Scalars.yaml'
nightly_file = 'https://hg.mozilla.org/mozilla-central/raw-file/tip' + scalar_file
release_file = 'https://hg.mozilla.org/releases/mozilla-release/raw-file/tip' + scalar_file
scalar_str = ''.join(open('tests/Scalars.yaml', 'r').readlines())


@pytest.fixture
def add_response():
    responses.add(responses.GET, nightly_file, scalar_str)
    responses.add(responses.GET, release_file, scalar_str)


@responses.activate
def test_int_scalar(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    scalar = Scalar(name, value)

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert scalar.get_definition().get('kind') == 'uint'
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_string_scalar(add_response):
    name, value = 'telemetry.test.string_kind', 'string'
    scalar = Scalar(name, value)

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert scalar.get_definition().get('kind') == 'string'
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_boolean_scalar(add_response):
    name, value = 'telemetry.test.boolean_kind', True
    scalar = Scalar(name, value)

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert scalar.get_definition().get('kind') == 'boolean'
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_int_addition(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    scalar = Scalar(name, value) + Scalar(name, value)

    assert scalar.get_value() == value * 2
    assert scalar.get_name() == name
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_string_addition(add_response):
    name, value = 'telemetry.test.string_kind', 'string'

    with pytest.raises(AttributeError):
        Scalar(name, value) + Scalar(name, value)


@responses.activate
def test_boolean_addition(add_response):
    name, value = 'telemetry.test.boolean_kind', True

    with pytest.raises(AttributeError):
        Scalar(name, value) + Scalar(name, value)


@responses.activate
def test_release_channel(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    scalar = Scalar(name, value, channel='release')

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_release_revision(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    scalar = Scalar(name, value,
                    revision='https://hg.mozilla.org/releases/mozilla-release/rev/tip')

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_release_url(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    scalar = Scalar(name, value,
                    scalars_url=release_file)

    assert scalar.get_value() == value
    assert scalar.get_name() == name
    assert Scalar.REQUIRED_FIELDS.issubset(scalar.get_definition().viewkeys())


@responses.activate
def test_channel_and_revision(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    with pytest.raises(ValueError):
        Scalar(name, value, channel='nightly',
               revision='https://hg.mozilla.org/releases/mozilla-release/rev/tip')


@responses.activate
def test_cache(add_response):
    name, value = 'telemetry.test.unsigned_int_kind', 8
    Scalar(name, value)
    scalar_2 = Scalar(name, value)

    assert scalar_2.get_value() == value
    assert scalar_2.get_name() == name
    assert Scalar.REQUIRED_FIELDS.issubset(scalar_2.get_definition().viewkeys())

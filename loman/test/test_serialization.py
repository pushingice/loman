import io
from loman.serialization import SerializationCatalog, default_registry


def round_trip(obj):
    buf = io.BytesIO()

    ser = SerializationCatalog(buf, 'w', default_registry)
    ser.serialize_item("foo", obj)
    ser.close()

    ser = SerializationCatalog(buf, 'r', default_registry)
    d2 = ser.deserialize_item("foo")
    ser.close()
    return d2


def test_bool_round_trip():
    d = False
    d2 = round_trip(d)
    assert d2 == d

    d = True
    d2 = round_trip(d)
    assert d2 == d


def test_int_round_trip():
    d = 42
    d2 = round_trip(d)
    assert d2 == d


def test_float_round_trip():
    d = 42.
    d2 = round_trip(d)
    assert d2 == d

    d = 3.141592653
    d2 = round_trip(d)
    assert d2 == d


def test_string_round_trip():
    d = 'foo'
    d2 = round_trip(d)
    assert d2 == d

    d = 'This is a longer string to trigger writing a file'
    d2 = round_trip(d)
    assert d2 == d


def test_list_round_trip():
    l = ['abc', 'This is a longer string to trigger writing a file']
    l2 = round_trip(l)
    assert l2 == l


def test_dict_round_trip():
    d = {
        'x': 'abc',
        'y': 'This is a longer string to trigger writing a file'
    }
    d2 = round_trip(d)
    assert d2 == d

import io
from loman.serialization import Serialization, StringSerializer


def test_string_roundtrip():
    buf = io.BytesIO()

    d = {'x': 'foo'}
    ser = Serialization(buf, 'w')
    ser.register_serializer(StringSerializer())
    ser.serialize_dict(d)
    ser.close()

    ser = Serialization(buf, 'r')
    ser.register_serializer(StringSerializer())
    d2 = ser.deserialize_all()
    ser.close()
    assert d2 == d
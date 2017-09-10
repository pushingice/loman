import io
from loman.serialization import Serialization, SerializerRegistry, StringSerializer


def test_string_roundtrip():
    reg = SerializerRegistry()
    reg.register_serializer(StringSerializer())

    buf = io.BytesIO()

    d = {'x': 'foo'}
    ser = Serialization(buf, 'w', reg)
    ser.serialize_dict(d)
    ser.close()

    ser = Serialization(buf, 'r', reg)
    d2 = ser.deserialize_all()
    ser.close()
    assert d2 == d

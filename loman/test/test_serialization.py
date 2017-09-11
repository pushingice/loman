import io
from loman.serialization import SerializationCatalog, SerializerRegistry, StringSerializer, ListSerializer


def test_string_roundtrip():
    reg = SerializerRegistry()
    reg.register_serializer(StringSerializer())

    buf = io.BytesIO()

    d = {'x': 'foo', 'y': 'This is a longer string to trigger writing a file'}
    ser = SerializationCatalog(buf, 'w', reg)
    ser.serialize_dict(d)
    ser.close()

    ser = SerializationCatalog(buf, 'r', reg)
    d2 = ser.deserialize_all()
    ser.close()
    assert d2 == d


def test_list_roundtrip():
    reg = SerializerRegistry()
    reg.register_serializer(StringSerializer())
    reg.register_serializer(ListSerializer(reg))

    buf = io.BytesIO()

    d = {'x': ['abc', 'This is a longer string to trigger writing a file']}
    ser = SerializationCatalog(buf, 'w', reg)
    ser.serialize_dict(d)
    ser.close()

    with open("test.zip", 'wb') as outfile:
        outfile.write(buf.getvalue())

    ser = SerializationCatalog(buf, 'r', reg)
    d2 = ser.deserialize_all()
    ser.close()
    assert d2 == d
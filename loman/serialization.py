import json
import uuid
import zipfile


class StringSerializer(object):
    def __init__(self):
        pass

    @property
    def name(self):
        return "str"

    @property
    def supported_classes(self):
        return [str]

    def can_serialize(self, value):
        return isinstance(value, str)

    def serialize(self, obj):
        return obj.encode('utf-8')

    def deserialize(self, b):
        return b.decode('utf-8')


class SerializerRegistry(object):
    def __init__(self):
        self.serializers_by_name = {}
        self.serializers_by_class = {}

    def register_serializer(self, serializer):
        for cls in serializer.supported_classes:
            self.serializers_by_class[cls] = serializer
            self.serializers_by_name[serializer.name] = serializer

    def get_serializer_for_value(self, value):
        return self.serializers_by_class[value.__class__]

    def get_serializer_by_name(self, name):
        return self.serializers_by_name[name]


class Serialization(object):
    def __init__(self, file, mode, serializer_registry):
        assert mode in ['r', 'w']
        self.mode = mode
        self.zipfile = zipfile.ZipFile(file, mode, zipfile.ZIP_DEFLATED)
        self.serializer_registry = serializer_registry
        self.catalog = {}
        if mode == 'r':
            with self.zipfile.open("catalog", 'r') as f:
                self.catalog = json.loads(f.read().decode('utf-8'))

    def serialize_item(self, key, value):
        serializer = self.serializer_registry.get_serializer_for_value(value)
        filename, _ = self.catalog.get(key, (None, None))
        if filename is None:
            filename = uuid.uuid4().hex
        self.zipfile.writestr(filename, serializer.serialize(value))
        self.catalog[key] = (filename, serializer.name)

    def serialize_dict(self, d):
        for k, v in d.items():
            self.serialize_item(k, v)

    def get_keys(self):
        return self.catalog.keys()

    def deserialize_item(self, key):
        filename, typename = self.catalog[key]
        serializer = self.serializer_registry.get_serializer_by_name(typename)
        with self.zipfile.open(filename, 'r') as f:
            return serializer.deserialize(f.read())

    def deserialize_all(self):
        d = {}
        for k in self.get_keys():
            d[k] = self.deserialize_item(k)
        return d

    def close(self):
        if self.mode == 'w':
            catalog_data = json.dumps(self.catalog).encode('utf-8')
            self.zipfile.writestr('catalog', catalog_data)
        self.zipfile.close()

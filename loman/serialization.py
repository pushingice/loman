import json
import os
import uuid
import zipfile
import tempfile


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

    def serialize(self, obj, get_file):
        if len(obj) > 10:
            f = get_file()
            f.write(obj.encode('utf-8'))
        else:
            return obj

    def deserialize(self, b, f):
        print("f=", f)
        if f is None:
            return b
        else:
            return f.read().decode('utf-8')


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
        filename, _, _ = self.catalog.get(key, (None, None, None))
        if filename is None:
            filename = uuid.uuid4().hex

        l = []
        try:
            def get_file():
                f = tempfile.NamedTemporaryFile(delete=False)
                l.append(f)
                return f

            write_str = serializer.serialize(value, get_file)

            if len(l) == 0:
                self.catalog[key] = (None, serializer.name, write_str)
            else:
                f = l[0]
                f.close()
                self.zipfile.write(f.name, filename)
                self.catalog[key] = (filename, serializer.name, write_str)
        finally:
            if len(l) == 1:
                f = l[0]
                f.close()
                os.remove(f.name)

    def serialize_dict(self, d):
        for k, v in d.items():
            self.serialize_item(k, v)

    def get_keys(self):
        return self.catalog.keys()

    def deserialize_item(self, key):
        filename, typename, str_rep = self.catalog[key]
        try:
            info = self.zipfile.getinfo(filename)
            f = self.zipfile.open(filename, 'r')
        except KeyError:
            f = None
        serializer = self.serializer_registry.get_serializer_by_name(typename)
        return serializer.deserialize(str_rep, f)

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

import json
import os
import uuid
import zipfile
import tempfile
import abc
from collections import namedtuple

import six


class SerializerABC(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def name(self):
        pass

    @abc.abstractmethod
    def supported_classes(self):
        pass

    @abc.abstractmethod
    def can_serialize(self, value):
        pass

    @abc.abstractmethod
    def serialize(self, obj, cat):
        pass

    @abc.abstractmethod
    def deserialize(self, cat_data, cat):
        pass


class StringSerializer(SerializerABC):
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

    def serialize(self, obj, cat):
        if len(obj) > 10:
            fwo = cat.open_file_write()
            fwo.file.write(obj.encode('utf-8'))
            fwo.file.close()
            return 'f', fwo.filename
        else:
            return 'l', obj

    def deserialize(self, cat_data, cat):
        ser_type, data = cat_data
        if ser_type == 'f':
            f = cat.open_file_read(data)
            return f.read().decode('utf-8')
        elif ser_type == 'l':
            return data


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


class SerializationCatalog(object):
    FileWriteObject = namedtuple('FileWriteObject', ['filename', 'file'])
    CatalogEntry = namedtuple('CatalogEntry', ['typename', 'data'])

    def __init__(self, file, mode, serializer_registry):
        assert mode in ['r', 'w']
        self.mode = mode
        self.zipfile = zipfile.ZipFile(file, mode, zipfile.ZIP_DEFLATED)
        self.serializer_registry = serializer_registry
        self.catalog = {}
        if mode == 'r':
            with self.zipfile.open("catalog", 'r') as f:
                string_dict = json.loads(f.read().decode('utf-8'))
                self.catalog = {k: SerializationCatalog.CatalogEntry(*v) for k, v in six.iteritems(string_dict)}
        self.string_table = {}
        if mode == 'r':
            with self.zipfile.open("string_table", 'r') as f:
                self.string_table = json.loads(f.read().decode('utf-8'))
        self.read_files = []
        self.write_files = []

    def serialize_item(self, key, value):
        serializer = self.serializer_registry.get_serializer_for_value(value)
        catalog_data = serializer.serialize(value, self)
        self.catalog[key] = SerializationCatalog.CatalogEntry(serializer.name, catalog_data)
        for f in self.read_files:
            f.close()
        self.read_files = []
        for fwo in self.write_files:
            self.zipfile.write(fwo.file.name, fwo.filename)
            fwo.file.close()
            os.remove(fwo.file.name)
        self.write_files = []

    def serialize_dict(self, d):
        for k, v in d.items():
            self.serialize_item(k, v)

    def get_keys(self):
        return self.catalog.keys()

    def deserialize_item(self, key):
        catalog_entry = self.catalog[key]
        serializer = self.serializer_registry.get_serializer_by_name(catalog_entry.typename)
        return serializer.deserialize(catalog_entry.data, self)

    def deserialize_all(self):
        d = {}
        for k in self.get_keys():
            d[k] = self.deserialize_item(k)
        return d

    def open_file_read(self, name):
        f = self.zipfile.open(name)
        self.read_files.append(f)
        return f

    def open_file_write(self):
        filename = uuid.uuid4().hex
        f = tempfile.NamedTemporaryFile(delete=False)
        fwo = SerializationCatalog.FileWriteObject(filename, f)
        self.write_files.append(fwo)
        return fwo

    def add_string(self, value):
        key = uuid.uuid4().hex
        self.string_table[key] = value
        return key

    def close(self):
        if self.mode == 'w':
            catalog_data = json.dumps(self.catalog).encode('utf-8')
            self.zipfile.writestr('catalog', catalog_data)
        if self.mode == 'w':
            string_table_data = json.dumps(self.catalog).encode('utf-8')
            self.zipfile.writestr('string_table', string_table_data)
        self.zipfile.close()

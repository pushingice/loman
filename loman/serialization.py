import json
import os
import uuid
import zipfile
import tempfile
import abc
from collections import namedtuple
import six

SERIALIZATION_TYPE_LITERAL = 0
SERIALIZATION_TYPE_STRING = 1
SERIALIZATION_TYPE_FILE = 2


class SerializerABC(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, registry):
        pass

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


class BoolSerializer(SerializerABC):
    def __init__(self, registry):
        super(BoolSerializer, self).__init__(registry)

    @property
    def name(self):
        return "bool"

    @property
    def supported_classes(self):
        return [bool]

    def can_serialize(self, value):
        return isinstance(value, bool)

    def serialize(self, obj, cat):
        return obj

    def deserialize(self, cat_data, cat):
        return cat_data


class IntSerializer(SerializerABC):
    def __init__(self, registry):
        super(IntSerializer, self).__init__(registry)

    @property
    def name(self):
        return "int"

    @property
    def supported_classes(self):
        return [int]

    def can_serialize(self, value):
        return isinstance(value, int)

    def serialize(self, obj, cat):
        return obj

    def deserialize(self, cat_data, cat):
        return cat_data


class FloatSerializer(SerializerABC):
    def __init__(self, registry):
        super(FloatSerializer, self).__init__(registry)

    @property
    def name(self):
        return "float"

    @property
    def supported_classes(self):
        return [float]

    def can_serialize(self, value):
        return isinstance(value, float)

    def serialize(self, obj, cat):
        return obj

    def deserialize(self, cat_data, cat):
        return cat_data


class StringSerializer(SerializerABC):
    def __init__(self, registry):
        super(StringSerializer, self).__init__(registry)

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
            return SERIALIZATION_TYPE_FILE, fwo.filename
        else:
            return SERIALIZATION_TYPE_LITERAL, obj

    def deserialize(self, cat_data, cat):
        ser_type, data = cat_data
        if ser_type == SERIALIZATION_TYPE_FILE:
            f = cat.open_file_read(data)
            return f.read().decode('utf-8')
        elif ser_type == SERIALIZATION_TYPE_LITERAL:
            return data


class ListSerializer(SerializerABC):
    def __init__(self, registry):
        super(ListSerializer, self).__init__(registry)
        self.registry = registry

    @property
    def name(self):
        return "list"

    @property
    def supported_classes(self):
        return [list]

    def can_serialize(self, value):
        return isinstance(value, list)

    def serialize(self, obj, cat):
        ser_list = []
        for x in obj:
            cat_entry = cat.create_catalog_entry(x)
            ser_list.append(cat_entry)
        ser_list_json = json.dumps(ser_list)
        if len(obj) < 50:
            string_key = cat.add_string(ser_list_json)
            return SERIALIZATION_TYPE_STRING, string_key
        else:
            fwo = cat.open_file_write()
            fwo.file.write(ser_list_json.encode('utf-8'))
            fwo.file.close()
            return SERIALIZATION_TYPE_FILE, fwo.filename

    def deserialize(self, cat_data, cat):
        ser_type, data = cat_data
        if ser_type == SERIALIZATION_TYPE_FILE:
            f = cat.open_file_read(data)
            ser_list_json = f.read().decode('utf-8')
        elif ser_type == SERIALIZATION_TYPE_STRING:
            ser_list_json = cat.get_string(data)
        else:
            raise ValueError()
        ser_list = json.loads(ser_list_json)
        l = [cat.read_catalog_entry(CatalogEntry(*x)) for x in ser_list]
        return l


class DictSerializer(SerializerABC):
    def __init__(self, registry):
        super(DictSerializer, self).__init__(registry)
        self.registry = registry

    @property
    def name(self):
        return "dict"

    @property
    def supported_classes(self):
        return [dict]

    def can_serialize(self, value):
        return isinstance(value, dict)

    def serialize(self, obj, cat):
        ser_dict = {}
        for k, v in six.iteritems(obj):
            cat_entry = cat.create_catalog_entry(v)
            ser_dict[k] = cat_entry
        ser_dict_json = json.dumps(ser_dict)
        if len(obj) < 50:
            string_key = cat.add_string(ser_dict_json)
            return SERIALIZATION_TYPE_STRING, string_key
        else:
            fwo = cat.open_file_write()
            fwo.file.write(ser_dict_json.encode('utf-8'))
            fwo.file.close()
            return SERIALIZATION_TYPE_FILE, fwo.filename

    def deserialize(self, cat_data, cat):
        ser_type, data = cat_data
        if ser_type == SERIALIZATION_TYPE_FILE:
            f = cat.open_file_read(data)
            ser_dict_json = f.read().decode('utf-8')
        elif ser_type == SERIALIZATION_TYPE_STRING:
            ser_dict_json = cat.get_string(data)
        else:
            raise ValueError()
        ser_dict = json.loads(ser_dict_json)
        l = {k: cat.read_catalog_entry(CatalogEntry(*v)) for k, v in six.iteritems(ser_dict)}
        return l


serializers = [BoolSerializer, IntSerializer, FloatSerializer, StringSerializer, ListSerializer, DictSerializer]


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


def create_default_registry():
    reg = SerializerRegistry()
    for serializer in serializers:
        reg.register_serializer(serializer(reg))
    return reg


default_registry = create_default_registry()

CatalogEntry = namedtuple('CatalogEntry', ['typename', 'data'])
FileWriteObject = namedtuple('FileWriteObject', ['filename', 'file'])


class SerializationCatalog(object):
    def __init__(self, file, mode, serializer_registry):
        assert mode in ['r', 'w']
        self.mode = mode
        self.zipfile = zipfile.ZipFile(file, mode, zipfile.ZIP_DEFLATED)
        self.serializer_registry = serializer_registry
        self.catalog = {}
        if mode == 'r':
            with self.zipfile.open("catalog", 'r') as f:
                string_dict = json.loads(f.read().decode('utf-8'))
                self.catalog = {k: CatalogEntry(*v) for k, v in six.iteritems(string_dict)}
        self.string_table = {}
        if mode == 'r':
            with self.zipfile.open("string_table", 'r') as f:
                self.string_table = json.loads(f.read().decode('utf-8'))
        self.read_files = []
        self.write_files = []

    def create_catalog_entry(self, value):
        serializer = self.serializer_registry.get_serializer_for_value(value)
        catalog_data = serializer.serialize(value, self)
        return CatalogEntry(serializer.name, catalog_data)

    def serialize_item(self, key, value):
        self.catalog[key] = self.create_catalog_entry(value)
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

    def read_catalog_entry(self, catalog_entry):
        serializer = self.serializer_registry.get_serializer_by_name(catalog_entry.typename)
        return serializer.deserialize(catalog_entry.data, self)

    def deserialize_item(self, key):
        catalog_entry = self.catalog[key]
        return self.read_catalog_entry(catalog_entry)

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
        fwo = FileWriteObject(filename, f)
        self.write_files.append(fwo)
        return fwo

    def add_string(self, value):
        key = uuid.uuid4().hex
        self.string_table[key] = value
        return key

    def get_string(self, key):
        return self.string_table[key]

    def close(self):
        if self.mode == 'w':
            catalog_data = json.dumps(self.catalog).encode('utf-8')
            self.zipfile.writestr('catalog', catalog_data)
        if self.mode == 'w':
            string_table_data = json.dumps(self.string_table).encode('utf-8')
            self.zipfile.writestr('string_table', string_table_data)
        self.zipfile.close()

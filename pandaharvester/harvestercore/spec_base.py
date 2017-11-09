"""
Base class for XyzSpec

"""

import json
import pickle
from future.utils import iteritems


# encoder for non-native json objects
class PythonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (set, dict, list, bytearray)):
            return json.JSONEncoder.default(self, obj)
        return {'_non_json_object': pickle.dumps(obj)}


# hook for decoder
def as_python_object(dct):
    if '_non_json_object' in dct:
        return pickle.loads(str(dct['_non_json_object']))
    return dct


# base class for XyzSpec
class SpecBase(object):
    # to be set
    attributesWithTypes = ()
    zeroAttrs = ()

    # constructor
    def __init__(self):
        # remove types
        object.__setattr__(self, 'attributes', [])
        object.__setattr__(self, 'serializedAttrs', set())
        for attr in self.attributesWithTypes:
            attr, attrType = attr.split(':')
            attrType = attrType.split()[0]
            self.attributes.append(attr)
            if attrType in ['blob']:
                self.serializedAttrs.add(attr)
        # install attributes
        for attr in self.attributes:
            if attr in self.zeroAttrs:
                object.__setattr__(self, attr, 0)
            else:
                object.__setattr__(self, attr, None)
        # map of changed attributes
        object.__setattr__(self, 'changedAttrs', {})

    # override __setattr__ to collect changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal:
            self.changedAttrs[name] = value

    # reset changed attribute list
    def reset_changed_list(self):
        object.__setattr__(self, 'changedAttrs', {})

    # force update
    def force_update(self, name):
        if name in self.attributes:
            self.changedAttrs[name] = getattr(self, name)

    # pack into attributes
    def pack(self, values):
        if hasattr(values, '_asdict'):
            values = values._asdict()
        for attr in self.attributes:
            val = values[attr]
            if attr in self.serializedAttrs:
                val = json.loads(val, object_hook=as_python_object)
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    def column_names(cls):
        ret = ""
        for attr in cls.attributesWithTypes:
            attr = attr.split(':')[0]
            ret += "{0},".format(attr)
        ret = ret[:-1]
        return ret

    column_names = classmethod(column_names)

    # return expression of bind variables for INSERT
    def bind_values_expression(cls):
        ret = "VALUES("
        for attr in cls.attributesWithTypes:
            attr = attr.split(':')[0]
            ret += ":%s," % attr
        ret = ret[:-1]
        ret += ")"
        return ret

    bind_values_expression = classmethod(bind_values_expression)

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bind_update_changes_expression(self):
        ret = ""
        for attr in self.attributes:
            if attr in self.changedAttrs:
                ret += '%s=:%s,' % (attr, attr)
        ret = ret[:-1]
        ret += ' '
        return ret

    # return map of values
    def values_map(self, only_changed=False):
        ret = {}
        for attr in self.attributes:
            # only changed attributes
            if only_changed:
                if attr not in self.changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
                if attr in self.zeroAttrs:
                    val = 0
                else:
                    val = None
            if attr in self.serializedAttrs:
                val = json.dumps(val, cls=PythonObjectEncoder)
            ret[':%s' % attr] = val
        return ret

    # return list of values
    def values_list(self, only_changed=False):
        ret = []
        for attr in self.attributes:
            # only changed attributes
            if only_changed:
                if attr not in self.changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
                if attr in self.zeroAttrs:
                    val = 0
                else:
                    val = None
            if attr in self.serializedAttrs:
                val = json.dumps(val, cls=PythonObjectEncoder)
            ret.append(val)
        return ret

    # get dict of changed attributes
    def get_changed_attributes(self):
        retDict = dict()
        for attr in self.changedAttrs:
            retDict[attr] = getattr(self, attr)
        return retDict

    # set attributes
    def set_attributes_with_dict(self, attr_dict):
        for attr, val in iteritems(attr_dict):
            setattr(self, attr, val)

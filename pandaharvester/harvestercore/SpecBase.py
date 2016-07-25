"""
Base class for XyzSpec

"""

import json


class SpecBase(object):
    # to be set
    attributesWithTypes = ()
    zeroAttrs  = ()


    # constructor
    def __init__(self):
        # remove types
        object.__setattr__(self,'attributes',[])
        object.__setattr__(self,'serializedAttrs',set())
        for attr in self.attributesWithTypes:
            attr,attrType = attr.split(':')
            self.attributes.append(attr)
            if attrType in ['blob']:
                self.serializedAttrs.add(attr)
        # install attributes
        for attr in self.attributes:
            if attr in self.zeroAttrs:
                object.__setattr__(self,attr,0)
            else:
                object.__setattr__(self,attr,None)
        # map of changed attributes
        object.__setattr__(self,'changedAttrs',{})



    # override __setattr__ to collecte the changed attributes
    def __setattr__(self,name,value):
        oldVal = getattr(self,name)
        object.__setattr__(self,name,value)
        newVal = getattr(self,name)
        # collect changed attributes
        if oldVal != newVal:
            self.changedAttrs[name] = value



    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self,'changedAttrs',{})



    # pack into attributes
    def pack(self,values):
        for attr in self.attributes:
            val = values[attr]
            if attr in self.serializedAttrs:
                val = json.loads(val)
            object.__setattr__(self,attr,val)



    # return column names for INSERT
    def columnNames(cls):
        ret = ""
        for attr in cls.attributesWithTypes:
            attr = attr.split(':')[0]
            ret += "{0},".format(attr)
        ret = ret[:-1]
        return ret
    columnNames = classmethod(columnNames)



    # return expression of bind variables for INSERT
    def bindValuesExpression(cls):
        ret = "VALUES("
        for attr in cls.attributesWithTypes:
            attr = attr.split(':')[0]
            ret += ":%s," % attr
        ret = ret[:-1]
        ret += ")"            
        return ret
    bindValuesExpression = classmethod(bindValuesExpression)



    # return map of values
    def valuesMap(self,onlyChanged=False):
        ret = {}
        for attr in self.attributes:
            # only changed attributes
            if onlyChanged:
                if not attr in self.changedAttrs:
                    continue
            val = getattr(self,attr)
            if val == None:
                if attr in self.zeroAttrs:
                    val = 0
                else:
                    val = None
            if attr in self.serializedAttrs:
                val = json.dumps(val)
            ret[':%s' % attr] = val
        return ret



    # return list of values
    def valuesList(self,onlyChanged=False):
        ret = []
        for attr in self.attributes:
            # only changed attributes
            if onlyChanged:
                if not attr in self.changedAttrs:
                    continue
            val = getattr(self,attr)
            if val == None:
                if attr in self.zeroAttrs:
                    val = 0
                else:
                    val = None
            if attr in self.serializedAttrs:
                val = json.dumps(val)
            ret.append(val)
        return ret

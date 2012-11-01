#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

import urllib, urllib2
import base64
import json
import datetime
import collections

API_BASE = 'https://api.parse.com/1'
API_ROOT = '%s/classes' % API_BASE
API_USERS = '%s/users' % API_BASE
API_ROLES = '%s/roles' % API_BASE
API_FILES = '%s/files' % API_BASE
API_PUSH  = '%s/push' % API_BASE
API_INSTALLATIONS = '%s/installations' % API_BASE
APPLICATION_ID = ''
MASTER_KEY = ''

SPECIAL_CLASSES = ['_User', '_Role', 'User', 'Role', 'File', 'Push', '_File', '_Push', 'Installation', '_Installation']
RESERVED_OBJECT_PROPERTIES = ['_class_name', '_object_id', '_updated_at', '_created_at', '_updated_keys', '_baseURL', '_url']

import logging
logging.getLogger().setLevel(logging.DEBUG)

class ParseBinaryDataWrapper(str):
    pass



class ParseBase(object):
    _url = None
    _class_name = None
    def __init__(self, class_name=None):
        self._baseURL = API_ROOT
        if class_name:
            self._class_name = class_name
            if class_name is '_User' or class_name is 'User':
                self._baseURL = API_USERS
            elif class_name is '_Role':
                self._baseURL = API_ROLES

    def _setURL(self, uri):
        self._url = self._baseURL + uri

    def _executeCall(self, uri, http_verb, data=None):
        self._setURL(uri)
        url = self._url
        request = urllib2.Request(url, data)
        request.add_header('Content-type', 'application/json')

        # we could use urllib2's authentication system, but it seems like overkill for this
        auth_header =  "Basic %s" % base64.b64encode('%s:%s' % (APPLICATION_ID, MASTER_KEY))
        request.add_header("Authorization", auth_header)

        request.get_method = lambda: http_verb

        # TODO: add error handling for server response
        response = urllib2.urlopen(request)
        response_body = response.read()
        response_dict = json.loads(response_body)

        return response_dict

    def _ISO8601ToDatetime(self, date_string):
        # TODO: verify correct handling of timezone
        date_string = date_string[:-1] + 'UTC'
        date = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%Z")
        return date


class ParseObject(ParseBase):
    def __init__(self, class_name, attrs_dict=None):
        super(ParseObject, self).__init__(class_name)
        self._class_name = class_name
        self._object_id = None
        self._updated_at = None
        self._created_at = None
        self._updated_keys = dict()

        if attrs_dict:
            self._populateFromDict(attrs_dict)

    def objectId(self):
        return self._object_id

    def updatedAt(self):
        return self._updated_at and self._ISO8601ToDatetime(self._updated_at) or None

    def createdAt(self):
        return self._created_at and self._ISO8601ToDatetime(self._created_at) or None

    def save(self):
        if self._object_id:
            self._update()
        else:
            self._create()

    def delete(self):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: DELETE
        uri = self._baseURI()
        uri = '%s/%s' % (uri, self._object_id)

        self._executeCall(uri, 'DELETE')

        self = self.__init__(None)

    def _populateFromDict(self, attrs_dict):
        self._object_id = attrs_dict['objectId']
        self._created_at = attrs_dict['createdAt']
        self._updated_at = attrs_dict['updatedAt']

        del attrs_dict['objectId']
        del attrs_dict['createdAt']
        del attrs_dict['updatedAt']

        attrs_dict = dict(map(self._convertFromParseType, attrs_dict.items()))
        self.__dict__.update(attrs_dict)

    def _convertToParseType(self, prop):
        key, value = prop

        if type(value) == ParseObject:
            #if value._object_id is None:
            #    logging.debug("Object will save")
                
            #    logging.debug('Value after save')
            value.save()
            logging.debug("Object exists")
            logging.debug(value)
            value = {'__type': 'Pointer',
                'className': value._class_name,
                'objectId': value._object_id}
            
        elif type(value) == datetime.datetime:
            value = {'__type': 'Date',
                    'iso': value.isoformat()[:-3] + 'Z'} # take off the last 3 digits and add a Z
        elif type(value) == ParseBinaryDataWrapper:
            value = {'__type': 'Bytes',
                    'base64': base64.b64encode(value)}
        elif type(value) == ParseRelation:
            value = {'__type' : 'Relation',
                    'className' : value._class_name}
        return (key, value)

    def _convertFromParseType(self, prop):
        key, value = prop

        if type(value) == dict and value.has_key('__type'):
            if value['__type'] == 'Pointer':
                pass
                #value = ParseQuery(value['className']).get(value['objectId'])
                #value = {"_class_name":value['className'], }
            elif value['__type'] == 'Date':
                value = self._ISO8601ToDatetime(value['iso'])
            elif value['__type'] == 'Bytes':
                value = ParseBinaryDataWrapper(base64.b64decode(value['base64']))
            elif value['__type'] == 'Relation':
                value = ParseRelation(key, value['className'], self)
                pass
            elif value['__type'] == 'Object':
                value = ParseObject(value['className'], value)
            else:
                raise Exception('Invalid __type: %s' % value['__type'])
        return (key, value)

    def _getJSONProperties(self):

        properties_list = self.__dict__.items()

        # filter properties that start with an underscore
        properties_list = filter(lambda prop: prop[0][0] != '_', properties_list)

        #properties_list = [(key, value) for key, value in self.__dict__.items() if key[0] != '_']

        properties_list = map(self._convertToParseType, properties_list)
        
        properties_dict = dict(properties_list)
        json_properties = json.dumps(properties_dict)

        return json_properties

    def _getJSONUpdatedProperties(self):

        properties_list = self._updated_keys.items()

        # filter properties that start with an underscore
        properties_list = filter(lambda prop: prop[0][0] != '_', properties_list)

        #properties_list = [(key, value) for key, value in self.__dict__.items() if key[0] != '_']

        properties_list = map(self._convertToParseType, properties_list)
        
        properties_dict = dict(properties_list)
        json_properties = json.dumps(properties_dict)

        return json_properties

    def _baseURI(self):
        if self._class_name in SPECIAL_CLASSES:
            return ''
        else:
            return '/%s' % self._class_name
            pass

    def _create(self):
        # URL: /1/classes/<className>
        # HTTP Verb: POST

        uri = self._baseURI()

        data = self._getJSONProperties()

        response_dict = self._executeCall(uri, 'POST', data)
        
        self._created_at = self._updated_at = response_dict['createdAt']
        self._object_id = response_dict['objectId']

    def _update(self):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: PUT
        uri = self._baseURI()
        uri = '%s/%s' % (uri, self._object_id)

        #data = self._getJSONProperties()
        data = self._getJSONUpdatedProperties()
        response_dict = self._executeCall(uri, 'PUT', data)

        self._updated_at = response_dict['updatedAt']

    def __setattr__(self, name, value):
        self.__dict__[name] = value
        if name not in RESERVED_OBJECT_PROPERTIES:
            try:
                self._updated_keys[name] = value
            except:
                self._updated_keys = dict()
                self._updated_keys[name] = value

    def __str__(self):
        return str(self.__dict__)

class ParseRelation(ParseBase):
    def __init__(self, key, class_name, owner):
        self._key = key
        self._class_name = class_name
        self._owner = owner

    def _parseToDict(self, obj):
        o = {}
        o["__type"] = "Pointer"
        o["className"] = obj._class_name
        o["objectId"] = obj._object_id
        return o
 
    def _setURL(self, uri):
        if self._owner._class_name is 'User':
            self.url = API_USERS + uri
        else:
            self.url = API_ROOT + uri

    def add(self, anObject):
        return self._relationOperationWithObjects(anObject, "AddRelation")


    def delete(self, anObject):
        return self._relationOperationWithObjects(anObject, "RemoveRelation")


    def _relationOperationWithObjects(self, anObject, op):
        objects = []
        if type(anObject) == type(list()):
            for obj in anObject:
                objects.append(self._parseToDict(obj))
        else:
            objects.append(self._parseToDict(anObject))
        uri = ''
        if self._owner._class_name is 'User':
            uri = '/%s' % self._owner._object_id
        else:
            uri = '/%s/%s' % (self._owner._class_name, self._owner._object_id)
        data = {}
        operation = {}
        operation["__op"] = op
        operation["objects"] = objects
        data[self._key] = operation
        response  = self._executeCall(uri, 'PUT', json.dumps(data))
        return response



class ParseQuery(ParseBase):
    def __init__(self, class_name):
        super(ParseQuery, self).__init__(class_name)
        self._class_name = class_name
        self._where = collections.defaultdict(dict)
        self._options = {}
        self._object_id = ''
        self._includes = []

    def eq(self, name, value):
        self._where[name] = value
        return self

    # It's tempting to generate the comparison functions programatically,
    # but probably not worth the decrease in readability of the code.
    def lt(self, name, value):
        self._where[name]['$lt'] = value
        return self
        
    def lte(self, name, value):
        self._where[name]['$lte'] = value
        return self
        
    def gt(self, name, value):
        self._where[name]['$gt'] = value
        return self
        
    def gte(self, name, value):
        self._where[name]['$gte'] = value
        return self

    def ne(self, name, value):
        self._where[name]['$ne'] = value
        return self

    def order(self, order, decending=False):
        # add a minus sign before the order value if decending == True
        self._options['order'] = decending and ('-' + order) or order
        return self

    def limit(self, limit):
        self._options['limit'] = limit
        return self

    def skip(self, skip):
        self._options['skip'] = skip
        return self

    def get(self, object_id):
        self._object_id = object_id
        return self._fetch(single_result=True)

    def include(self, key):
        self._includes.append(key)

    def fetch(self):
        # hide the single_result param of the _fetch method from the library user
        # since it's only useful internally
        return self._fetch() 

    def _baseURI (self):
        return '/%s' % self._class_name

    def _buildURI (self):
        uri = self._baseURI()
        includeQS = self._includeQS()
        if self._object_id:
            uri = '%s/%s' % (uri, self._object_id)
            if includeQS:
                uri = '%s?%s' % (uri, includeQS)
        else:
            options = dict(self._options) # make a local copy
            if self._where:
                # JSON encode WHERE values
                where = json.dumps(self._where)
                options.update({'where': where})

            uri = '%s?%s' % (uri, urllib.urlencode(options))
            if includeQS:
                uri = '%s&%s' % (uri, includeQS)
        return uri

    def _includeQS(self):
        qs = None
        if self._includes and len(self._includes)>0:
            #include = json.dumps(self._includes)
            #qs = urllib.urlencode({'include':include})
            qs = "include=%s" % ','.join(self._includes)
        return qs
    
    def _fetch(self, single_result=False):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: GET
        uri = self._buildURI()
        
        response_dict = self._executeCall(uri, 'GET')

        if single_result:
            return ParseObject(self._class_name, response_dict)
        else:
            return [ParseObject(self._class_name, result) for result in response_dict['results']]
 
class ParseUserQuery(ParseQuery):
    def __init__(self):
        super(ParseUserQuery, self).__init__("User")
        #self._class_name = "User"
        self._where = collections.defaultdict(dict)
        self._options = {}
        self._object_id = ''
        self._includes = []  

    def _baseURI (self):
        return ''

class ParseACL(dict):
    def __init__(self):
        super(ParseACL, self).__init__()

    def publicRead(self, val):
        self._r("*", val)

    def publicWrite(self, val):
        self._w("*", val)


    def publicRW(self, val):
        self._rw("*",val)
        pass

    def userRead(self, user_id, val):
        self._r(user_id, val)
        pass

    def userWrite(self, user_id, val):
        self._w(user_id, val)
        pass

    def userRW(self, user_id, val):
        self._rw(user_id, val)
        pass

    def roleRead(self, role_name, val):
        self._r("role:%s" % role_name, val)
    def roleWrite(self, role_name, val):
        self._w("role:%s" % role_name, val)

    def roleRW(self, role_name, val):
        self._rw("role:%s" % role_name, val)

    def _r(self, gp, val):
        self._right("read", gp, val) 

    def _w(self, gp, val):
        self._right("write", gp, val)

    def _right(self, right, gp, val):
        if right not in ['read','write']:
            return
        if gp not in self:
            self[gp] = {}
        if val:
            self[gp][right] = val
        else:
            if right in self[gp]:
                del self[gp][right]

    def _rw(self, gp, val):
        self._r(gp, val)
        self._w(gp, val)

UserQuery = ParseUserQuery
Relation = ParseRelation
Query = ParseQuery
Object = ParseObject
ACL = ParseACL

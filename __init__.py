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

API_ROOT = 'https://api.parse.com/1/classes'
API_USER_ROOT = 'https://api.parse.com/1/users'
API_ROLE_ROOT = 'https://api.parse.com/1/roles'
APPLICATION_ID = ''
MASTER_KEY = ''


class ParseBinaryDataWrapper(str):
    pass



class ParseBase(object):
    url = None

    def _setURL(self, uri):
        self.url = API_ROOT + uri

    def _executeCall(self, uri, http_verb, data=None):
        self._setURL(uri)
        url = self.url
        request = urllib2.Request(url, data)
        print url
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
        self._class_name = class_name
        self._object_id = None
        self._updated_at = None
        self._created_at = None

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

        uri = '/%s/%s' % (self._class_name, self._object_id)

        self._executeCall(uri, 'DELETE')

        self = self.__init__(None)

    def _populateFromDict(self, attrs_dict):
        print 'Start populating %s ' % self._class_name
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
            value = {'__type': 'Pointer',
                    'className': value._class_name,
                    'objectId': value._object_id}
        elif type(value) == datetime.datetime:
            value = {'__type': 'Date',
                    'iso': value.isoformat()[:-3] + 'Z'} # take off the last 3 digits and add a Z
        elif type(value) == ParseBinaryDataWrapper:
            value = {'__type': 'Bytes',
                    'base64': base64.b64encode(value)}

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
                pass
            elif value['__type'] == 'Object':
                print 'Found Object %s<br/>' %value['className']
                value = ParseObject(value['className'], value)
            else:
                raise Exception('Invalid __type: %s' % value['__type'])
            #print e
        print '%s:%s<br/>' % (key, value)
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

    def _create(self):
        # URL: /1/classes/<className>
        # HTTP Verb: POST

        uri = '/%s' % self._class_name

        data = self._getJSONProperties()

        response_dict = self._executeCall(uri, 'POST', data)
        
        self._created_at = self._updated_at = response_dict['createdAt']
        self._object_id = response_dict['objectId']

    def _update(self):
        # URL: /1/classes/<className>/<objectId>
        # HTTP Verb: PUT

        uri = '/%s/%s' % (self._class_name, self._object_id)

        data = self._getJSONProperties()

        response_dict = self._executeCall(uri, 'PUT', data)

        self._updated_at = response_dict['updatedAt']

    def __str__(self):
        return str(self.__dict__)


class ParseQuery(ParseBase):
    def __init__(self, class_name):
        super(ParseQuery, self).__init__()
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
            print uri
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
        
        print uri
        response_dict = self._executeCall(uri, 'GET')

        if single_result:
            print '<br/>----- Response Dict---- <br/>'
            print response_dict
            print '<br/>---- Response Dict---- <br/>'
            return ParseObject(self._class_name, response_dict)
        else:
            return [ParseObject(self._class_name, result) for result in response_dict['results']]
 
class ParseUserQuery(ParseQuery):
    def __init__(self):
        super(ParseUserQuery, self).__init__("User")
        #ParseQuery.__init__("User")
        self._class_name = "User"
        self._where = collections.defaultdict(dict)
        self._options = {}
        self._object_id = ''
        self._includes = []

    def _setURL(self, uri):
        self.url = API_USER_ROOT + uri

    def _baseURI(self):
        return ''      

from metadata_consts import *
import copy

base_map = {
    u'facetRanges': [],
    u'facets': {},
    u'highlighting': {},
    u'limit': 0,
    u'offset': 0,
    u'qtime': 0,
    u'results': [],
    u'totalMatched': 0
}


highlight_map_value = {
    INTERNAL_TYPE: [],
    ORIGINAL_NAME: [],
    TYPE: [],
    u'sourceType': [u'<em>HIVE</em>'],
    u'fileSystemPath': [],
    u'owner': []
}


def get_base_entity(type):
    e = {
        DESCRIPTION: None,
        ORIGINAL_DESCRIPTION: None,
        ID: None,
        NAME: None,
        ORIGINAL_NAME: None,
        PARENT_PATH: None,
        INTERNAL_TYPE: None,
        TAGS: [],
        TYPE: None,
        DATA_TYPE: None
    }
    if type == 'database':
        e[TYPE] = DATABASE_TYPE
        e[INTERNAL_TYPE] = e[META_CLASS_NAME] = I_DATABASE
    elif type == 'table':
        e[TYPE] = TABLE_TYPE
        e[INTERNAL_TYPE] = e[META_CLASS_NAME] = I_TABLE
    elif type == 'column':
        e[TYPE] = FIELD_TYPE
        e[INTERNAL_TYPE] = e[META_CLASS_NAME] = I_FIELD
    else:
        return None
    return e


def create_highlight_entry(type, name):
    h = copy.deepcopy(highlight_map_value)
    h[ORIGINAL_NAME].append(unicode("<em>%s:%s</em>" % (type, name)))
    if type == 'column':
        h[INTERNAL_TYPE].append(HIGHLIGHT_TYPE_HV_FIELD)
        h[TYPE].append(HIGHLIGHT_TYPE_FIELD)
    elif type == 'table':
        h[INTERNAL_TYPE].append(HIGHLIGHT_TYPE_HV_TABLE)
        h[TYPE].append(HIGHLIGHT_TYPE_TABLE)
    elif type == 'database':
        h[INTERNAL_TYPE].append(HIGHLIGHT_TYPE_HV_DATABASE)
        h[TYPE].append(HIGHLIGHT_TYPE_DATABASE)
    return h


def create_entity_entry(type, base_map):
    payload = {}
    name = base_map[NAME]
    description = base_map[DESCRIPTION]
    if type == 'table':
        payload = get_base_entity('table')
        payload['parentPath'] = unicode(base_map[PARENT_PATH])
    elif type == 'column':
        payload = get_base_entity('column')
        payload['parentPath'] = unicode(base_map[PARENT_PATH])
        payload['dataType'] = unicode(base_map[DATA_TYPE].lower())
    elif type == 'database':
        payload = get_base_entity('database')
    payload['originalName'] = unicode(name)
    payload['originalDescription'] = payload['description'] = unicode(description)
    return payload

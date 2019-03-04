import requests
from dropdown_search_model import *


def update_search_ds():

    def get_catalog_map(catalogs):
        catalog_map = {}
        for c in catalogs:
            universe = c['universe']
            name = c['name']
            key = "%s_%s" % (universe, name)
            catalog_map[key] = c
        return catalog_map

    def get_namespace_map(namespaces):
        namespace_map = {}
        for n in namespaces:
            universe = n['universe']
            catalog = n['catalog']
            namespace = n['namespace']['name']
            key = "%s_%s_%s" % (universe, catalog, namespace)
            namespace_map[key] = n
        return namespace_map

    def get_db_description(universe, catalog, namespace, catalog_map, namespace_map):
        catalog_id = "%s_%s" % (universe, catalog)
        namespace_id = "%s_%s_%s" % (universe, catalog, namespace)
        catalog = catalog_map[catalog_id]
        owner = catalog['publisherRef']
        catalog_desc = catalog['description']
        namespace = namespace_map[namespace_id]
        namespace_desc = namespace['namespace']['description']
        return "Team Owner: %s -- Universe_Catalog Description: %s Namespace Description: %s" % (
        owner, catalog_desc, namespace_desc)

    def get_tables_result_tuple(catalog_map, namespace_map, raw_tables):
        databases = {}
        tables = {}
        columns = {}

        for raw_table in raw_tables:
            universe = raw_table['tableContext']['universe']
            catalog = raw_table['tableContext']['catalog']
            namespace = raw_table['tableContext']['namespace']
            raven_db_name = "%s_%s_%s" % (universe, catalog, namespace)
            raven_db_description = get_db_description(universe, catalog, namespace, catalog_map, namespace_map)

            # Adding database entity
            if raven_db_name not in databases:
                db_entity = get_base_entity('database')
                db_entity[ID] = raven_db_name
                db_entity[NAME] = db_entity[ORIGINAL_NAME] = raven_db_name
                db_entity[HIGHLIGHT_NAME] = raven_db_name
                db_entity[DESCRIPTION] = db_entity[ORIGINAL_DESCRIPTION] = raven_db_description
                databases[raven_db_name] = [db_entity]

            # Adding table entity
            table_entity = get_base_entity('table')
            raven_table_name = raw_table['tableContext']['table']['name']
            raven_table_description = "%s Table Description: %s" % (
            raven_db_description, raw_table['tableContext']['table']['schema']['description'])
            table_entity[ID] = "%s:%s" % (raven_db_name, raven_table_name)
            table_entity[NAME] = table_entity[ORIGINAL_NAME] = raven_table_name
            table_entity[HIGHLIGHT_NAME] = raven_table_name
            table_entity[DESCRIPTION] = table_entity[ORIGINAL_DESCRIPTION] = raven_table_description
            table_entity[PARENT_PATH] = raven_db_name
            if raven_table_name in tables:
                tables[raven_table_name].extend([table_entity])  # Note: Assumes every table map value is unique
            else:
                tables[raven_table_name] = [table_entity]

            # Adding attribute entities
            raven_table_attributes = raw_table['tableContext']['table']['schema']['attributes']
            for attr in raven_table_attributes:
                attr_name = attr['name']
                attr_description = attr['descriptor']['description']
                attr_type = attr['descriptor']['type']
                raven_column_description = "%s Column Description: %s" % (raven_table_description, attr_description)
                column_entity = get_base_entity('column')
                column_entity[ID] = "%s:%s.%s" % (raven_db_name, raven_table_name, attr_name)
                column_entity[NAME] = column_entity[ORIGINAL_NAME] = "%s.%s" % (raven_table_name, attr_name)
                column_entity[HIGHLIGHT_NAME] = attr_name
                column_entity[DESCRIPTION] = column_entity[ORIGINAL_DESCRIPTION] = raven_column_description
                column_entity[PARENT_PATH] = raven_db_name
                column_entity[DATA_TYPE] = attr_type
                if attr_name in columns:
                    columns[attr_name].extend([column_entity])  # Note: Assumes every column map value is unique
                else:
                    columns[attr_name] = [column_entity]

        return databases, tables, columns

    def transform_to_search_ds(p):
        payload = {}
        api_catalogs = p[0]
        api_namespaces = p[1]
        api_tables = p[2]

        catalog_map = get_catalog_map(api_catalogs)
        namespace_map = get_namespace_map(api_namespaces)
        tables_tuple = get_tables_result_tuple(catalog_map, namespace_map, api_tables)
        payload['database'] = tables_tuple[0]
        payload['table'] = tables_tuple[1]
        payload['column'] = tables_tuple[2]

        return payload

    api_types = ["catalogs", "namespaces", "tables"]
    rest_payloads = []
    endpt_prefix = 'http://staging-raven.data.bazaarvoice.com/api/documentation'
    for api_type in api_types:
        endpt = "%s/%s" % (endpt_prefix, api_type)
        r = requests.get(endpt)
        rest_payloads.append(r.json())
    return transform_to_search_ds(rest_payloads)
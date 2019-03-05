#!/usr/bin/env python
# -- coding: utf-8 --
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from configparser import ConfigParser
import psycopg2
import copy
import json

from metadata_consts import *
from metadata.catalog.base import Api


LOG = logging.getLogger(__name__)


class RavenApi(Api):
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

  @staticmethod
  def get_base_entity(type):
    e = {
          DESCRIPTION: None,
          ORIGINAL_DESCRIPTION: None,
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
    elif type == 'view':
        e[TYPE] = VIEW_TYPE
        e[INTERNAL_TYPE] = e[META_CLASS_NAME] = I_VIEW
    elif type == 'column':
        e[TYPE] = FIELD_TYPE
        e[INTERNAL_TYPE] = e[META_CLASS_NAME] = I_FIELD
    else:
        return None
    return e

  highlight_map_value = {
      INTERNAL_TYPE: [],
      ORIGINAL_NAME: [],
      TYPE: [],
      u'sourceType': [u'<em>HIVE</em>'],
      u'fileSystemPath': [],
      u'owner': []
  }

  catalogs_json_list = '[{"id":"4acc58bb-d757-3e10-ab5c-393e14fa685d","universe":"master","name":"reporting","description":"internal","publisherRef":"SocialAnalytics"},{"id":"3ca620b3-f965-31f8-8096-9535f838836d","universe":"demo","name":"example","description":"example","publisherRef":"demo_publisher"},{"id":"7499d57d-d04d-375a-9561-9fd716532002","universe":"uat","name":"bulk","description":"All data produced by group bulk in universe uat by user release","publisherRef":"bulk_team_bazaarvoice_com"},{"id":"965e0a07-f200-39ac-8a0b-fdc4cdec2efb","universe":"uat","name":"magpie","description":"Raw Magpie Staging logs","publisherRef":"magpie"},{"id":"a3ab35ea-c133-37c3-a545-805060d03ea6","universe":"uat","name":"daas","description":"Internal data related to Data as a Service Team","publisherRef":"universal_catalog"},{"id":"cbc84419-a5e2-3c13-b66f-bef566869ed3","universe":"uat","name":"pixel_import","description":"pixel import event logs","publisherRef":"notifications"},{"id":"11ca1dc8-61ba-36f0-b270-3c76e16e3541","universe":"bazaar","name":"rmorgan","description":"Data published by Robby","publisherRef":"rmorgan"},{"id":"41458dc3-7186-3c07-a7ce-17f9e57175db","universe":"bazaar","name":"internal","description":"Derived data tables for use internally","publisherRef":"magpie"},{"id":"46d73baf-614a-3124-9bd8-b0ab8e54a84c","universe":"bazaar","name":"bulk","description":"Bulk exported datasets","publisherRef":"bulk-exports"},{"id":"5143ac3d-9887-3b7c-beac-07298ea6118a","universe":"bazaar","name":"advertising","description":"Data generated for advertising purposes.","publisherRef":"advertising"},{"id":"65e9f19f-b0b1-3b7a-a69d-d4916c5d2050","universe":"bazaar","name":"pixel_import","description":"pixel import event logs","publisherRef":"notifications"},{"id":"8196eebf-897b-32ca-9ae1-82001302c40a","universe":"bazaar","name":"nir","description":"NIR work in progress","publisherRef":"SocialAnalytics"},{"id":"844d18e3-60b1-32e5-a147-8969e22a6f6f","universe":"bazaar","name":"connections","description":"Records made available by the Connections team","publisherRef":"Connections"},{"id":"861f9a05-a9bb-31d7-b45e-e136b2707338","universe":"bazaar","name":"apii","description":"APII data","publisherRef":"apii"},{"id":"894f425d-f115-37d1-967f-768471405adc","universe":"bazaar","name":"switchboard","description":"Information about Switchboard syndication clients and edges.","publisherRef":"switchboard"},{"id":"93884470-679b-341c-9688-dd6cc1c37415","universe":"bazaar","name":"ml","description":"Data for the machine learning team","publisherRef":"ml"},{"id":"9e82808f-9f6d-3006-b588-aaa4f3cd8705","universe":"bazaar","name":"bulkdocs","description":"Bulk Query Documents","publisherRef":"adatbulk"},{"id":"a535e17d-23a6-3ce2-8fdc-6bbe248d96df","universe":"bazaar","name":"consus","description":"Consus generated dataset. Owned by Magpie team","publisherRef":"magpie"},{"id":"adc2914f-f6e9-3a35-a0cc-d9183782f124","universe":"bazaar","name":"ads_vlas","description":"advertising experiments","publisherRef":"vlas"},{"id":"b0695b7d-19af-3a7a-8010-e16c223bd31f","universe":"bazaar","name":"emo","description":"Records made available by the Emo team.","publisherRef":"emo"},{"id":"b19cf5e9-c04e-34f2-8bd4-ad9e34aaad81","universe":"bazaar","name":"contentops","description":"internal metrics for content operations","publisherRef":"SocialAnalytics"},{"id":"bd830c4b-8e4c-3544-886f-b8f44da62743","universe":"bazaar","name":"martech","description":"internal","publisherRef":"dataproduct"},{"id":"bf613b4a-b22e-357c-996a-50dee0017156","universe":"bazaar","name":"sampling","description":"Sampling communities data","publisherRef":"sampling"},{"id":"d3c01468-7c54-3535-8748-132cef1cedf3","universe":"bazaar","name":"curations","description":"Content and authorship records that are used by Curations.","publisherRef":"curations"},{"id":"daedbcfa-12fb-324f-9f35-04ce871fd505","universe":"bazaar","name":"bulk_bnorton","description":"internal","publisherRef":"bulk_team"},{"id":"db0d9664-5477-301f-9bf3-6761be9107fc","universe":"bazaar","name":"addstructure","description":"A catalog for the Addstructure team","publisherRef":"addstructure"},{"id":"dd40f791-c995-3113-b9cc-33c0b179310b","universe":"bazaar","name":"experiment","description":"Data from or for experiments","publisherRef":"SocialAnalytics"},{"id":"eb153fe2-bf6a-357f-a5db-f6e6a78ec03a","universe":"bazaar","name":"catalog","description":"Data related to products, brands and categories.","publisherRef":"universal_catalog"},{"id":"f1c8620c-64ad-337b-bb23-d594a1923162","universe":"bazaar","name":"quandl","description":"internal","publisherRef":"SocialAnalytics"},{"id":"f34b0e2e-d384-3940-b74d-2b33498af38c","universe":"bazaar","name":"magpie","description":"Raw Magpie logs","publisherRef":"magpie"},{"id":"f6e841ce-27b5-3845-91ad-518de2295e9f","universe":"bazaar","name":"rosetta","description":"Rosetta data","publisherRef":"rosetta"},{"id":"cebe38af-b472-3f88-b70d-14b46dd8a0db","universe":"reporting","name":"aggregate","description":"internal","publisherRef":"SocialAnalytics"}]'

  def __init__(self, user=None):
    self.user = user
    self.db = self.get_conn()
    self.tuple = self.get_tuple_catalog_map_universe_set(self.catalogs_json_list)
    self.catalog_map = self.tuple[0]
    self.universe_set = self.tuple[1]

  def get_tuple_catalog_map_universe_set(self, json_str):
      catalogs = json.loads(json_str)
      catalog_map = {}
      universe_set = set()
      for c in catalogs:
          name = c['name']
          universe = c['universe']
          # owner = c['publisherRef']
          key = "%s_%s" % (universe, name)
          universe_set.add(universe)
          catalog_map[key] = c
      return catalog_map, universe_set

  def create_highlight_entry(self, type, originalName):
      h = copy.deepcopy(self.highlight_map_value)
      h[ORIGINAL_NAME].append(unicode(originalName))
      if type == 'column':
          h[INTERNAL_TYPE].append(HIGHLIGHT_TYPE_HV_FIELD)
          h[TYPE].append(HIGHLIGHT_TYPE_FIELD)
      elif type == 'table':
          h[INTERNAL_TYPE].append(HIGHLIGHT_TYPE_HV_TABLE)
          h[TYPE].append(HIGHLIGHT_TYPE_TABLE)
      return h

  def create_entity(self, type, base_map, col_name=None, col_type=None, db_name=None):
      payload = {}
      name = ""
      catalog = base_map['tableContext']['catalog']
      universe = base_map['tableContext']['universe']
      namespace = base_map['tableContext']['namespace']
      description = base_map['tableContext']['table']['description']
      parent_path = "%s_%s_%s" % (universe, catalog, namespace)
      if type == 'table':
          # payload = copy.deepcopy(self.base_table_entity)
          payload = RavenApi.get_base_entity('table')
          name = base_map['tableContext']['table']['name']
      elif type == 'column':
          # payload = copy.deepcopy(self.base_col_entity)
          payload = RavenApi.get_base_entity('column')
          payload['dataType'] = unicode(col_type.lower())
          name = "%s.%s" % (base_map['tableContext']['table']['name'], col_name)
      catalog_map_key = "%s_%s" % (universe, catalog)
      owner = self.catalog_map[catalog_map_key]['publisherRef']
      owner_result = "(Team Owner: %s)" % (owner)
      description_display = "%s - %s" % (owner_result, description)
      payload['originalName'] = unicode(name)
      ## Alternative search result text view
      # name_display = "%s %s" % (name, owner_result)
      # payload['originalName'] = unicode(name_display)
      payload['originalDescription'] = payload['description'] = unicode(description_display)
      # HUE database related to this entity
      payload['parentPath'] = unicode(parent_path)
      return payload

  def get_table_entities(self, cur, query_s, limit):
      entities_payload = []
      highlight_payload = {}
      # regex search on tables()
      table_sql = "SELECT data FROM tables WHERE data->'tableContext'->'table'->>'name' ilike '%%%s%%' limit %s;" % (
      query_s, limit)
      cur.execute(table_sql)
      for elem in cur.fetchall():
          base_map = elem[0]
          id = base_map['tableContext']['table']['id']
          name = base_map['tableContext']['table']['name']
          h = self.create_highlight_entry('table', name)
          highlight_payload[id] = h
          r = self.create_entity('table', base_map)
          entities_payload.append(r)
      return entities_payload, highlight_payload

  def get_col_entities(self, cur, query_s, limit):
      entities_payload = []
      highlight_payload = {}
      # regex search on columns
      col_sql = "SELECT t.data, cols->>'name' AS name, cols->'descriptor'->>'type' AS type FROM tables t, jsonb_array_elements(t.data->'tableContext'->'table'->'schema'->'attributes') cols WHERE cols->>'name' LIKE '%%%s%%' limit %s;" % (
      query_s, limit)
      cur.execute(col_sql)
      for elem in cur.fetchall():
          base_map = elem[0]
          col_name = elem[1]
          col_type = elem[2]
          par_id = base_map['tableContext']['table']['id']
          id = "%s-%s" % (par_id, col_name)
          name = "%s.%s" % (base_map['tableContext']['table']['name'], col_name)
          h = self.create_highlight_entry('column', name)
          highlight_payload[id] = h
          r = self.create_entity('column', base_map, col_name, col_type)
          entities_payload.append(r)
      return entities_payload, highlight_payload

  def search_entities_interactive(self, query_s=None, limit=100, **filters):
    facet = None
    if ':' in query_s:
        pvt = query_s.find(':')
        facet = query_s[:query_s.find(':')]
        query_s = query_s[pvt+1:]
    else:
        query_s = query_s[1:]
    payload = copy.deepcopy(self.base_map)
    payload['limit'] = limit
    cur = None
    try:
        cur = self.db.cursor()
        if not cur:
            print("Couldn't establish a cursor")
            return payload
        if 'facetFields' in filters and 'tags' in filters['facetFields']:
            payload['facets']['tags'] = self.tags.keys()
        else:
            if not facet:
                table_tuple = self.get_table_entities(cur, query_s, limit)
                payload['results'].extend(table_tuple[0])
                payload['highlighting'].update(table_tuple[1])
                column_tuple = self.get_col_entities(cur, query_s, limit)
                payload['results'].extend(column_tuple[0])
                payload['highlighting'].update(column_tuple[1])
            elif facet == 'column':
                column_tuple = self.get_col_entities(cur, query_s, limit)
                payload['results'].extend(column_tuple[0])
                payload['highlighting'].update(column_tuple[1])
            elif facet == 'table':
                table_tuple = self.get_table_entities(cur, query_s, limit)
                payload['results'].extend(table_tuple[0])
                payload['highlighting'].update(table_tuple[1])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
            print('Cursor is now closed.')
    return payload

  ## dummy api for column/table search
  # def search_entities_interactive(self, query_s=None, limit=100, **filters):
  #   return {u'highlighting': {u'27': {u'sourceType': [u'<em>HIVE</em>'], u'originalName': [u'<em>sample_08</em>'], u'owner': [u'<em>admin</em>'], u'type': [u'<em>TABLE</em>'], u'fileSystemPath': [u'<em>hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_08</em>'], u'internalType': [u'<em>hv_table</em>']}, u'1144700': {u'sourceType': [u'<em>HIVE</em>'], u'originalName': [u'<em>sample_07_parquet</em>'], u'owner': [u'<em>admin</em>'], u'type': [u'<em>TABLE</em>'], u'fileSystemPath': [u'<em>hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07_parquet</em>'], u'internalType': [u'<em>hv_table</em>']}, u'22': {u'sourceType': [u'<em>HIVE</em>'], u'description': [u'<em>Job</em> <em>data</em>'], u'originalName': [u'<em>sample_07</em>'], u'owner': [u'<em>admin</em>'], u'type': [u'<em>TABLE</em>'], u'fileSystemPath': [u'<em>hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07</em>'], u'internalType': [u'<em>hv_table</em>']}}, u'facets': {}, u'qtime': 1339, u'facetRanges': [], u'results': [{u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_08', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': None, u'inputFormat': u'org.apache.hadoop.mapred.TextInputFormat', u'tags': None, u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_08', u'dd': u'xx'}, u'identity': u'27', u'outputFormat': u'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##503', u'created': u'2018-03-30T17:14:44.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_08', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}, {u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07_parquet', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': None, u'inputFormat': u'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat', u'tags': None, u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07_parquet'}, u'identity': u'1144700', u'outputFormat': u'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##718', u'created': u'2018-04-17T06:16:17.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_07_parquet', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}, {u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': u'Job data', u'inputFormat': u'org.apache.hadoop.mapred.TextInputFormat', u'tags': None, u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07'}, u'identity': u'22', u'outputFormat': u'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##503', u'created': u'2018-03-30T17:14:42.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_07', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}], u'totalMatched': 3, u'limit': 45, u'offset': 0}

  def find_entity(self, source_type, type, name, **filters):
    return [{u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': u'Job data', u'inputFormat': u'org.apache.hadoop.mapred.TextInputFormat', u'tags': None, u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07'}, u'identity': u'22', u'outputFormat': u'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##503', u'created': u'2018-03-30T17:14:42.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_07', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}]


  def get_entity(self, entity_id):
    return {u'customProperties': None, u'deleteTime': None, u'description': None, u'dataType': u'int', u'type': u'FIELD', u'internalType': u'hv_column', u'sourceType': u'HIVE', u'tags': None, u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'originalDescription': None, u'metaClassName': u'hv_column', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07'}, u'identity': u'26', u'firstClassParentId': u'22', u'name': None, u'extractorRunId': u'8##1', u'sourceId': u'8', u'packageName': u'nav', u'parentPath': u'/default/sample_07', u'originalName': u'total_emp'}


  def update_entity(self, entity, **metadata):
    return {}


  def add_tags(self, entity_id, tags):
    return {u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': u'Job data', u'inputFormat': u'org.apache.hadoop.mapred.TextInputFormat', u'tags': [u'usage'], u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07'}, u'identity': u'22', u'outputFormat': u'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##503', u'created': u'2018-03-30T17:14:42.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_07', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}


  def delete_tags(self, entity_id, tags):
    return {}


  def update_properties(self, entity_id, properties, modified_custom_metadata=None, deleted_custom_metadata_keys=None):
    # For updating comments of table or columns
    # Returning the entity but not used currently
    return {u'clusteredByColNames': None, u'customProperties': {}, u'owner': u'admin', u'serdeName': None, u'deleteTime': None, u'fileSystemPath': u'hdfs://self-service-analytics-1.gce.cloudera.com:8020/user/hive/warehouse/sample_07', u'sourceType': u'HIVE', u'serdeLibName': u'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe', u'lastModifiedBy': None, u'sortByColNames': None, u'partColNames': None, u'type': u'TABLE', u'internalType': u'hv_table', u'description': u'Adding an description', u'inputFormat': u'org.apache.hadoop.mapred.TextInputFormat', u'tags': [u'usage'], u'deleted': False, u'technicalProperties': None, u'userEntity': False, u'serdeProps': None, u'originalDescription': None, u'compressed': False, u'metaClassName': u'hv_table', u'properties': {u'__cloudera_internal__hueLink': u'http://self-service-analytics-1.gce.cloudera.com:8889/metastore/table/default/sample_07'}, u'identity': u'22', u'outputFormat': u'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat', u'firstClassParentId': None, u'name': None, u'extractorRunId': u'8##503', u'created': u'2018-03-30T17:14:42.000Z', u'sourceId': u'8', u'lastModified': None, u'packageName': u'nav', u'parentPath': u'/default', u'originalName': u'sample_07', u'lastAccessed': u'1970-01-01T00:00:00.000Z'}

  ########
  # db methods
  meta_data_dbname = 'meta_data'

  def config(self, filename='/Users/josh.guan/work/demo/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    cfg = {}
    if parser.has_section(section):
      params = parser.items(section)
      for param in params:
        cfg[param[0]] = param[1]
    else:
      raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return cfg

  def get_conn(self):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
      # read connection parameters
      params = self.config()

      # connect to the PostgreSQL server
      print('Connecting to the PostgreSQL database...')
      conn = psycopg2.connect(**params)
    except (Exception, psycopg2.DatabaseError) as error:
      print(error)
    return conn
#!/usr/bin/env python
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
import posixpath
import threading

from django.utils.translation import ugettext as _

from desktop.conf import DEFAULT_USER
from desktop.lib.exceptions_renderable import PopupException
from desktop.lib.rest.http_client import HttpClient
from desktop.lib.rest.resource import Resource

from hadoop import cluster


LOG = logging.getLogger(__name__)

_API_VERSION = 'v1'
_JSON_CONTENT_TYPE = 'application/json'

API_CACHE = None
API_CACHE_LOCK = threading.Lock()


def get_timeline_server(username=None):
  global API_CACHE

  if API_CACHE is None:
    API_CACHE_LOCK.acquire()
    try:
      if API_CACHE is None:
        yarn_cluster = cluster.get_cluster_conf_for_job_submission()
        if yarn_cluster is None:
          raise PopupException(_('No Timeline Server are available.'))
        API_CACHE = TimelineServerApi(yarn_cluster.TIMELINE_SERVER_API_URL.get(), yarn_cluster.SECURITY_ENABLED.get(), yarn_cluster.SSL_CERT_CA_VERIFY.get())
    finally:
      API_CACHE_LOCK.release()

  API_CACHE.setuser(username) # Set the correct user

  return API_CACHE


class TimelineServerApi(object):

  def __init__(self, timeline_api_url, security_enabled=False, ssl_cert_ca_verify=False):
    self._url = posixpath.join(timeline_api_url, 'ws', _API_VERSION)
    self._client = HttpClient(self._url, logger=LOG)
    self._root = Resource(self._client)
    self._security_enabled = security_enabled
    self._thread_local = threading.local() # To store user info
    self.from_failover = False

    if self._security_enabled:
      self._client.set_kerberos_auth()

    self._client.set_verify(ssl_cert_ca_verify)

  def _get_params(self):
    params = {}

    if self.username != DEFAULT_USER.get(): # We impersonate if needed
      params['doAs'] = self.username
      if not self.security_enabled:
        params['user.name'] = DEFAULT_USER.get()

    return params

  def __str__(self):
    return "TimelineServerApi at %s" % (self._url,)

  def setuser(self, user):
    curr = self.user
    self._thread_local.user = user
    return curr

  @property
  def user(self):
    return self.username # Backward compatibility

  @property
  def username(self):
    try:
      return self._thread_local.user
    except AttributeError:
      return DEFAULT_USER.get()

  @property
  def url(self):
    return self._url

  @property
  def security_enabled(self):
    return self._security_enabled

  def timeline(self, entity_type, **kwargs):
    params = self._get_params()
    params.update(kwargs)
    return self._execute(self._root.get, 'timeline/%s' % entity_type, params=params, headers={'Accept': _JSON_CONTENT_TYPE})

  def _execute(self, function, *args, **kwargs):
    response = None
    try:
      response = function(*args, **kwargs)
    except Exception, e:
      raise PopupException(_('YARN Timeline API returned a failed response: %s') % e)
    return response

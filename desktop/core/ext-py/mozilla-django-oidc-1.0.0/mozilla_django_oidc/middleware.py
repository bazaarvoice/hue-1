import logging
import time
from datetime import datetime
try:
    from urllib.parse import urlencode
except ImportError:
    # Python < 3
    from urllib import urlencode

import django
try:
    from django.urls import reverse
except ImportError:
    # Django < 2.0.0
    from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect, JsonResponse
from django.utils.crypto import get_random_string
from django.utils.functional import cached_property

from mozilla_django_oidc.utils import (
    absolutify,
    import_from_settings,
    is_authenticated
)

from mozilla_django_oidc.auth import OIDCAuthenticationBackend

LOGGER = logging.getLogger(__name__)


# Django 1.10 makes changes to how middleware work. In Django 1.10+, we want to
# use the mixin so that our middleware works as is.
if django.VERSION >= (1, 10):
    from django.utils.deprecation import MiddlewareMixin
else:
    class MiddlewareMixin(object):
        pass


class SessionRefresh(MiddlewareMixin, OIDCAuthenticationBackend):
    """Refreshes the session with the OIDC RP after expiry seconds

    For users authenticated with the OIDC RP, verify tokens are still valid and
    if not, force the user to re-authenticate silently.

    """

    @cached_property
    def exempt_urls(self):
        """Generate and return a set of url paths to exempt from SessionRefresh

        This takes the value of ``settings.OIDC_EXEMPT_URLS`` and appends three
        urls that mozilla-django-oidc uses. These values can be view names or
        absolute url paths.

        :returns: list of url paths (for example "/oidc/callback/")

        """
        exempt_urls = list(import_from_settings('OIDC_EXEMPT_URLS', []))
        exempt_urls.extend([
            'oidc_authentication_init',
            'oidc_authentication_callback',
            'oidc_logout',
        ])

        return set([
            url if url.startswith('/') else reverse(url)
            for url in exempt_urls
        ])

    def is_refreshable_url(self, request):
        """Takes a request and returns whether it triggers a refresh examination

        :arg HttpRequest request:

        :returns: boolean

        """
        return (
            request.method == 'GET' and
            is_authenticated(request.user) and
            request.path not in self.exempt_urls
        )

    @staticmethod
    def to_datetime(timestamp_seconds):
        return datetime.fromtimestamp(timestamp_seconds).strftime('%Y-%m-%d %H:%M:%S')

    def process_request(self, request):
        if not self.is_refreshable_url(request):
            LOGGER.debug('request is not refreshable')
            return

        expiration = request.session.get('oidc_id_token_expiration', 0)
        now = time.time()

        # 10 minutes before of token expires, we perform a token refresh flow if the refresh token is available
        # else, when the session has expired, we have to perform another authentication flow
        dt_expiration = self.to_datetime(expiration)
        dt_now = self.to_datetime(now)
        if expiration > now + 10 * 60:
            # The id_token is still valid, so we don't have to do anything.
            LOGGER.debug('id token is still valid (%s > %s)', dt_expiration, dt_now)
            return
        elif now < expiration < now + 10 * 60:
            LOGGER.debug('id token will expire within the next 10 minutes (%s > %s). refreshing token ...', dt_expiration, dt_now)

            token_payload = {
                'client_id': self.OIDC_RP_CLIENT_ID,
                'client_secret': self.OIDC_RP_CLIENT_SECRET,
                'grant_type': 'refresh_token',
                'refresh_token': request.session.get('oidc_refresh_token'),
                'scope': import_from_settings('OIDC_RP_SCOPES', 'openid email')
            }

            token_info = self.get_token(token_payload)
            id_token = token_info.get('id_token')
            access_token = token_info.get('access_token')
            token_expiration = token_info.get('expires_in')
            refresh_token = token_info.get('refresh_token')

            verified_id = self.verify_token(id_token)

            if verified_id:
                session = request.session

                if import_from_settings('OIDC_STORE_ACCESS_TOKEN', False):
                    session['oidc_access_token'] = access_token

                if import_from_settings('OIDC_STORE_ID_TOKEN', False):
                    session['oidc_id_token'] = id_token

                if import_from_settings('OIDC_STORE_REFRESH_TOKEN', False):
                    session['oidc_refresh_token'] = refresh_token

                session['oidc_id_token_expiration'] = time.time() + token_expiration
                LOGGER.info('OIDC access token refresh succeeded. Token expires on %s', self.to_datetime(session['oidc_id_token_expiration']))
            else:
                LOGGER.warn('OIDC access token refresh verification failed')

            return None
        else:
            LOGGER.debug('id token has expired since %s', dt_expiration)
            # The id_token has expired, so we have to re-authenticate silently.
            auth_url = import_from_settings('OIDC_OP_AUTHORIZATION_ENDPOINT')
            client_id = import_from_settings('OIDC_RP_CLIENT_ID')
            state = get_random_string(import_from_settings('OIDC_STATE_SIZE', 32))

            # Build the parameters as if we were doing a real auth handoff, except
            # we also include prompt=none.
            params = {
                'response_type': 'code',
                'client_id': client_id,
                'redirect_uri': absolutify(
                    request,
                    reverse('oidc_authentication_callback')
                ),
                'state': state,
                'scope': import_from_settings('OIDC_RP_SCOPES', 'openid email'),
                'prompt': 'none',
            }

            if import_from_settings('OIDC_USE_NONCE', True):
                nonce = get_random_string(import_from_settings('OIDC_NONCE_SIZE', 32))
                params.update({
                    'nonce': nonce
                })
                request.session['oidc_nonce'] = nonce

            request.session['oidc_state'] = state
            request.session['oidc_login_next'] = request.get_full_path()

            query = urlencode(params)
            redirect_url = '{url}?{query}'.format(url=auth_url, query=query)
            if request.is_ajax():
                # Almost all XHR request handling in client-side code struggles
                # with redirects since redirecting to a page where the user
                # is supposed to do something is extremely unlikely to work
                # in an XHR request. Make a special response for these kinds
                # of requests.
                # The use of 403 Forbidden is to match the fact that this
                # middleware doesn't really want the user in if they don't
                # refresh their session.
                response = JsonResponse({'refresh_url': redirect_url}, status=403)
                response['refresh_url'] = redirect_url
                return response
            return HttpResponseRedirect(redirect_url)

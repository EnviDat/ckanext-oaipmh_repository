'''Serving controller interface for OAI-PMH
'''
import logging

from ckan.lib.base import render
from flask import Blueprint, request, make_response
from ckanext.oaipmh_repository.oaipmh_repository import OAIPMHRepository

log = logging.getLogger(__name__)


def get_blueprints(name, module):
    # Create Blueprint for plugin
    blueprint = Blueprint(name, module)

    blueprint.add_url_rule(
        # 'oai',
        u'/oai',
        u'index',
        index
    )

    return blueprint


def index():
    """Return the result of the handled request of a batching OAI-PMH
    server implementation.
    """
    url_params = request.args
    if 'verb' in url_params:
        verb = url_params.get('verb')
        if verb:
            log.debug('verb: %s', verb)
            repository = OAIPMHRepository()
            params = url_params.copy()
            params.pop('verb', None)
            content = repository.handle_request(verb, params, request.url)
            headers = {u'Content-Type': 'text/xml; charset=UTF-8'}
            return make_response(content, 200, headers)

    return render('oaipmh_repository/oaipmh_repository.html')

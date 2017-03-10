'''Serving controller interface for OAI-PMH
'''
import logging

import requests

from ckan.lib.base import BaseController, render
from pylons import request, response

log = logging.getLogger(__name__)

class OAIPMHController(BaseController):
    '''Controller for OAI-PMH server implementation. Returns only the index
    page if no verb is specified.
    '''
    def index(self):
        '''Return the result of the handled request of a batching OAI-PMH
        server implementation.
        '''
        if 'verb' in request.params:
            verb = request.params['verb'] if request.params['verb'] else None
            if verb:
                log.debug('verb: %s', verb)
                url_base =  'http://envidat01.wsl.ch:8080/oai-pmh-rest-0.1.0/oai?'
                url = url_base+request.url.split('?')[1]
                r = requests.get(url)
                response.content_type = 'text/xml'
                response.headers['content-type'] = 'text/xml; charset=utf-8'
                return(r.content)

        #else:
        return render('oaipmh_repository/oaipmh_repository.html')

from pylons import config
from ckan.lib.helpers import url_for
from datetime import datetime

from xmltodict import unparse
import collections

from logging import getLogger
log = getLogger(__name__)

class OAIPMHRepository(object):

    def __init__(self):
        self.dateformat = "%Y-%m-%dT%H:%M:%SZ"
        self.verb_handlers = {
            'Identify': self.identify,
            'GetRecord': self.get_record,
            'ListIdentifiers': self.list_identifiers,
            'ListMetadataFormats': self.list_metadata_formats,
            'ListRecords': self.list_records,
            'ListSets': self.list_sets
        }
        
    def handle_request(self, verb, params, url):
        handler = self.verb_handlers.get(verb)
        if not handler:
            verb = 'error'
            handler = self.bad_verb
        content = handler(params)
        xmldict = self._envelop(verb, params, url, content)
        return unparse(xmldict)

    def bad_verb(self, params):
        return {'@code':'badVerb', '#text': 'Illegal OAI verb' }
        
    def identify(self, params):
        identify_dict = collections.OrderedDict()
        identify_dict['repositoryName'] = config.get('site.title') if config.get('site.title') else 'repository'
        identify_dict['baseURL'] = url_for(controller='ckanext.oaipmh_repository.controller:OAIPMHController', action='index')
        identify_dict['protocolVersion'] = '2.0'
        if config.get('email_to'):
            identify_dict['adminEmail'] = config.get('email_to', 'undefined')
        identify_dict['earliestDatestamp'] = datetime(2004, 1, 1).strftime(self.dateformat)
        identify_dict['deletedRecord'] = 'no'
        identify_dict['granularity'] ='YYYY-MM-DD'
        return identify_dict
        
    def get_record(self, params):
        return {'#text': 'get_record: implementation pending' }
        
    def list_identifiers(self, params):
        return {'#text': 'list_identifiers: implementation pending' }
        
    def list_metadata_formats(self, params):
        return {'#text': 'list_identifiers: implementation pending' }
        
    def list_records(self, params):
        return {'#text': 'list_records: implementation pending' }
        
    def list_sets(self, params):
        return {'#text': 'list_sets: implementation pending' }

    def _envelop(self, verb, params, url, content):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['OAI-PMH'] = collections.OrderedDict()
        oaipmh_dict['OAI-PMH']['@xmlns']='http://www.openarchives.org/OAI/2.0/'
        oaipmh_dict['OAI-PMH']['@xmlns:xsi']='http://www.w3.org/2001/XMLSchema-instance'
        oaipmh_dict['OAI-PMH']['@xsi:schemaLocation'] = 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'

        oaipmh_dict['OAI-PMH']['responseDate'] = datetime.now().strftime(self.dateformat) #'2017-02-08T12:00:01Z'
        oaipmh_dict['OAI-PMH']['request'] = {'@verb':verb, '#text':str(url)}
        if len(params)>1:
            for param in params:
                if param != 'verb':
                    oaipmh_dict['OAI-PMH']['request']['@'+str(param)] = str(params[param])
        
        # Verb dict
        oaipmh_dict['OAI-PMH'][verb] = collections.OrderedDict()
        if not isinstance(content, dict):
            content = {'#text': str(content)}
        oaipmh_dict['OAI-PMH'][verb] = content
        
        return oaipmh_dict















from ckan.lib.helpers import url_for

from datetime import datetime
from ckan.common import config

from xmltodict import unparse
import collections
import sys
import urllib.parse

from ckanext.package_converter.model.metadata_format import MetadataFormats, XMLMetadataFormat
from ckanext.package_converter.model.record import XMLRecord, Record

import ckan.plugins as plugins

import ckanext.oaipmh_repository.oaipmh_error as oaipmh_error
from ckanext.oaipmh_repository.record_access import RecordAccessService

from logging import getLogger

log = getLogger(__name__)


class OAIPMHRepository(plugins.SingletonPlugin):

    def __init__(self):
        self.dateformat = config.get('oaipmh_repository.dateformat', '%Y-%m-%dT%H:%M:%SZ')
        self.verb_handlers = {
            'Identify': self.identify,
            'GetRecord': self.get_record,
            'ListIdentifiers': self.list_identifiers,
            'ListMetadataFormats': self.list_metadata_formats,
            'ListRecords': self.list_records,
            'ListSets': self.list_sets
        }
        self.id_prefix = config.get('oaipmh_repository.id_prefix', 'oai:ckan:id:')
        self.id_field = config.get('oaipmh_repository.id_field', 'name')
        self.regex = config.get('oaipmh_repository.regex', '*')
        self.max_results = int(config.get('oaipmh_repository.max', '10'))
        self.oai_repository_site_id = config.get('oaipmh_repository.site_id', 'repository')
        self.deleted_record = config.get('oaipmh_repository.deleted_record', 'no')

        self.ckan_solr_url = config.get('solr_url')
        self.validate = config.get('oaipmh_repository.validate', 'False')

        self.record_access = RecordAccessService(self.dateformat, self.id_prefix,
                                                 self.id_field, self.regex,
                                                 self.ckan_solr_url, self.max_results)
        log.debug(self)

    def handle_request(self, verb, params, url):
        oaipmh_verb = 'error'
        try:
            handler = self.verb_handlers[verb]
            log.debug(handler)
            content = handler(params)
            oaipmh_verb = verb
        except oaipmh_error.OAIPMHError as e:
            content = e.as_xml_dict()
        except KeyError as e:
            log.debug(e)
            log.debug(sys.exc_info())
            content = oaipmh_error.BadVerbError().as_xml_dict()
        except:
            e = sys.exc_info()[1]
            code = type(e).__name__
            message = str(e)
            content = oaipmh_error.OAIPMHError(code, message).as_xml_dict()

        xmldict = self._envelop(oaipmh_verb, params, url, content)
        # print(xmldict)
        if self.validate == "True":
            if not self._is_valid_oai_pmh_record(xmldict, metadata_prefix=params.get('metadataPrefix')):
                log.error('OAI-PMH Response Validation FAILED')
            else:
                log.debug('OAI-PMH Response Validation SUCCESS')
                log.debug('OAI-PMH Response is error? = {0}'.format(self._is_error_oai_pmh_record(xmldict)))
        else:
            log.debug('OAI-PMH Response Validation SKIPPED')
            log.debug('OAI-PMH Response is error? = {0}'.format(self._is_error_oai_pmh_record(xmldict)))

        return unparse(xmldict, pretty=True)

    def identify(self, params={}):
        if params:
            raise oaipmh_error.BadArgumentError()

        identify_dict = collections.OrderedDict()
        identify_dict['repositoryName'] = self.oai_repository_site_id
        identify_dict['baseURL'] = config.get('ckan.site_url') + url_for(
            controller='oaipmh_repository', action='index')
        identify_dict['protocolVersion'] = '2.0'
        identify_dict['adminEmail'] = config.get('email_to', 'admin@server.domain')
        identify_dict['earliestDatestamp'] = datetime(2004, 1, 1).strftime(self.dateformat)
        identify_dict['deletedRecord'] = self.deleted_record
        identify_dict['granularity'] = 'YYYY-MM-DD'
        return identify_dict

    def list_metadata_formats(self, params):
        if set(params.keys()).difference(['identifier']):
            raise oaipmh_error.BadArgumentError()
        # return all the XML formats
        metadata_formats = MetadataFormats().get_all_metadata_formats()
        if not metadata_formats:
            raise oaipmh_error.NoMetadataFormatsError()
        formats_dict = collections.OrderedDict()
        formats_dict['metadataFormat'] = []
        for metadata_format in metadata_formats:
            if issubclass(type(metadata_format), XMLMetadataFormat) and (
                    metadata_format.get_format_name() != 'oai_pmh'):
                format_dict = collections.OrderedDict()
                format_dict['metadataPrefix'] = metadata_format.get_format_name()
                format_dict['schema'] = metadata_format.get_xsd_url()
                format_dict['metadataNamespace'] = metadata_format.get_namespace()
                formats_dict['metadataFormat'] += [format_dict]
        return formats_dict

    def get_record(self, params):
        if set(params.keys()) != set(['identifier', 'metadataPrefix']):
            raise oaipmh_error.BadArgumentError()
        return self.record_access.get_record(params.get('identifier'), params.get('metadataPrefix'))

    def list_identifiers(self, input_params):
        params = self._validate_params_list(input_params)
        return (self.record_access.list_identifiers(params.get('metadataPrefix'),
                                                    params.get('from'),
                                                    params.get('until'),
                                                    params.get('offset', 0)))

    def list_records(self, input_params):
        params = self._validate_params_list(input_params)
        return (self.record_access.list_records(params.get('metadataPrefix'),
                                                params.get('from'),
                                                params.get('until'),
                                                params.get('offset', 0)))

    def list_sets(self, params):
        raise oaipmh_error.NoSetHierarchyError()
        # return {'#text': 'list_sets: implementation pending'}

    def _validate_params_list(self, params):
        # validate and replace from resumptionToken
        if set(params.keys()).difference(['metadataPrefix', 'from', 'until', 'resumptionToken']):
            if 'set' in params.keys():
                raise oaipmh_error.NoSetHierarchyError()
            raise oaipmh_error.BadArgumentError()
        if 'resumptionToken' in params.keys():
            if len(params.keys()) > 1:
                raise oaipmh_error.BadArgumentError('ResumptionToken can not be used together with other arguments')
            else:
                return self._params_from_token(params)
        else:
            if 'metadataPrefix' not in params.keys():
                raise oaipmh_error.BadArgumentError('Missing required argument metadataPrefix')
        return params

    def _params_from_token(self, params):
        try:
            token_params = {}
            token = params['resumptionToken']
            uncoded_token = urllib.unquote(token).decode('utf8')
            log.debug(uncoded_token)
            token_params_list = urllib.parse.parse_qs(uncoded_token)
            token_params['metadataPrefix'] = token_params_list['metadataPrefix'][0]
            token_params['offset'] = token_params_list['offset'][0]
            token_params['until'] = token_params_list['until'][0]
            if token_params_list.get('from'):
                token_params['from'] = token_params_list['from'][0]
            return token_params
        except Exception as e:
            raise oaipmh_error.BadResumptionTokenError(str(e))

    def _envelop(self, verb, params, url, content):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['OAI-PMH'] = collections.OrderedDict()
        oaipmh_dict['OAI-PMH']['@xmlns'] = 'http://www.openarchives.org/OAI/2.0/'
        oaipmh_dict['OAI-PMH']['@xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        oaipmh_dict['OAI-PMH']['@xsi:schemaLocation'] = 'http://www.openarchives.org/OAI/2.0/ ' \
                                                        'http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd '
        # '2017-02-08T12:00:01Z'
        oaipmh_dict['OAI-PMH']['responseDate'] = datetime.now().strftime(self.dateformat)
        oaipmh_dict['OAI-PMH']['request'] = collections.OrderedDict()
        oaipmh_dict['OAI-PMH']['request']['#text'] = str(url).split('?')[0]

        if verb != 'error':
            oaipmh_dict['OAI-PMH']['request']['@verb'] = verb

            if len(params) >= 1:
                for param in params:
                    if param != 'verb':
                        oaipmh_dict['OAI-PMH']['request']['@' + str(param)] = str(params[param])

            # Verb dict
            oaipmh_dict['OAI-PMH'][verb] = collections.OrderedDict()
            if not isinstance(content, dict) and not isinstance(content, list):
                content = {'#text': str(content)}
            oaipmh_dict['OAI-PMH'][verb] = content

        else:
            oaipmh_dict['OAI-PMH'][verb] = content.get('error', {})

        return oaipmh_dict

    def _is_valid_oai_pmh_record(self, xmldict, metadata_prefix=''):
        site_url = config.get('ckan.site_url', '')

        if not metadata_prefix:
            metadata_prefix = 'oai_dc'
        try:
            xml_record = unparse(xmldict)

            oai_pmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], xml_record)

            # get the format
            metadata_format = MetadataFormats().get_metadata_formats(metadata_prefix)[0]
            metadata_schema = metadata_format.get_xsd_url()

            # local xsd for gcmd_dif (nasa hosted is not always available)
            if metadata_prefix == 'gcmd_dif':
                metadata_schema = metadata_schema.replace('http://gcmd.gsfc.nasa.gov/Aboutus/xml/dif/', site_url +
                                                          '/package_converter_xsd/')

            # modify xsd due to library bug
            fixed_xsd = '''<xs:schema xmlns="http://www.openarchives.org/OAI/2.0/"
                                  xmlns:xs="http://www.w3.org/2001/XMLSchema"
                                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                  xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" >
                           <xs:import namespace="http://www.openarchives.org/OAI/2.0/" schemaLocation="http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd" />
                           <xs:import namespace="{namespace}" schemaLocation="{schema}" />
                       </xs:schema>'''.format(namespace=metadata_format.get_namespace(), schema=metadata_schema)

            return oai_pmh_record.validate(custom_xsd=fixed_xsd)

        except Exception as e:
            print(e)
            log.error('Failed to validate OAI-PMH for format {0}'.format(metadata_prefix))
        except:
            log.error('Failed to validate OAI-PMH for format {0}'.format(metadata_prefix))
            return False

    def _is_error_oai_pmh_record(self, xmldict):
        return 'error' in xmldict.get('OAI-PMH', {}).keys()

    def __repr__(self):
        return str(self)

    def __str__(self):
        return str(self.__unicode__())

    def __unicode__(self):
        return (u'''OAIPMHRepository: granularity = {dateformat},
                                      prefix = {id_prefix}, id_field = {id_field},
                                      verb_handlers = {handlers},
                                      max_results = {max_results},
                                      ckan_solr_url = {ckan_solr_url}''').format(
            dateformat=self.dateformat,
            id_prefix=self.id_prefix,
            id_field=self.id_field,
            handlers=self.verb_handlers.keys(),
            max_results=self.max_results,
            ckan_solr_url=self.ckan_solr_url
        )

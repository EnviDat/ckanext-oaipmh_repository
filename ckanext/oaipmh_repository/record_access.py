import ckan.lib.search.common as ckan_search
from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import JSONRecord, XMLRecord
from ckanext.package_converter.logic import export_as_record
import ckan.plugins.toolkit as toolkit

from pylons import config
import urllib
import pytz

from datetime import datetime
import collections

import oaipmh_error
from doi_db_index import OAIPMHDOIIndex
from doi_solr_index import DoiSolrNode

import logging
log = logging.getLogger(__name__)

class RecordAccessService(object):

    def __init__(self, dateformat, id_prefix, id_field, regex, doi_solr_url, max_results = 1000, doi_index_params=[], local_tz='Europe/Berlin'):
        self.dateformat = dateformat
        self.id_prefix = id_prefix
        self.id_field = id_field
        self.regex = regex
        self.max_results = max_results
        self.doi_index = None
        if doi_index_params:
            self.doi_index = OAIPMHDOIIndex(doi_index_params[0], doi_index_params[1])
        self.doi_solr = DoiSolrNode(doi_solr_url, local_tz)

    def get_record(self, oai_identifier, format):
        log.debug('****** get_record ******')
        # Get record
        value = self._get_ckan_field_value(oai_identifier)

        log.debug('\t value: ' + value)
        log.debug('\t id_field: ' + self.id_field)

        results, size = self.doi_solr.query_by_field(self.id_field, value)
        log.debug('\t results: {result} type={type}'.format(result=results, type=type(results)))


        if not results:
            raise oaipmh_error.IdDoesNotExistError()
        else:
            result = results[0]

        log.debug('\t result: {result} type={type}'.format(result=result, type=type(result)))

        ckan_id = result['package_id']
        entity = result['entity']
        if entity == 'resource':
            ckan_id = result['resource_id']
        datestamp = result['datestamp']

        return(self._export_dataset(ckan_id, entity, oai_identifier, datestamp, format))

    def list_records(self, format, start_date=None, end_date=None, offset = 0):
        results, size = self._find_by_date(start_date, end_date,  offset = offset)

        if not results:
            raise oaipmh_error.NoRecordsMatchError()

        record_list = collections.OrderedDict()
        record_list['record'] = []

        for result in results:
            package_id = result.get('id')
            datestamp = result.get('datestamp')
            oai_identifier = self._get_oaipmh_id(result.get(self.id_field))
            record_list['record'] += [self._export_dataset(package_id, oai_identifier, datestamp, format)['record']]

        if size != len(results):
            token = self._get_ressumption_token(start_date, end_date, format, offset, len(results), size)
            log.debug(token)
            record_list['resumptionToken'] = token['resumptionToken']

        return record_list

    def list_identifiers(self, format, start_date=None, end_date=None, offset=0):
        results = self.list_records(format, start_date, end_date, offset=offset)

        if not results:
            raise oaipmh_error.NoRecordsMatchError()

        identifiers_list =  collections.OrderedDict()
        identifiers_list['header'] = []

        for result in results.get('record',[]):
            identifiers_list['header'] += [result['header']]

        if  results.get('resumptionToken', False):
            identifiers_list['resumptionToken'] = results.get('resumptionToken')

        return identifiers_list


    def _export_dataset(self, ckan_id, entity, oai_identifier, datestamp, format):
        # Convert record
        try:
            #log.debug(' Found package_id = {0}'.format(package_id))
            converted_record = export_as_record(ckan_id, format, type=entity)
            record = XMLRecord.from_record(converted_record)

        except Exception, e:
            log.exception(e)
            record = None
        if not record:
            raise oaipmh_error.CannotDisseminateFormatError()

        return (self._envelop_record(oai_identifier, datestamp, record.get_xml_dict()))

    def _get_oaipmh_id(self, id):
        return('{prefix}{id}'.format(prefix=self.id_prefix, id=id))

    def _get_ckan_field_value(self, oai_id):
        return (oai_id.split(self.id_prefix)[-1])


    def _find_by_date(self, start_date, end_date, offset=0):
        #TODO: Add link to DB behind firewall!!
        offset = int(offset)
        max_results = self.max_results
        results = []
        size = 0
        packages_found = []
        try:

            # compatibility ckan 2.5 and 2.6
            query_text = '{0}:{1}'.format(self.id_field, self.regex if self.regex else '*')

            if start_date or end_date:
                start_date_str = self._format_date(start_date, to_utc=True)
                end_date_str = self._format_date(end_date, to_utc=True)
                query_text += ' metadata_modified:[{0} TO {1}]'.format(start_date_str, end_date_str)

            field_query = 'state:active site_id:%s capacity:public' % config.get('ckan.site_id')
            fields = 'id, state, {0}, metadata_modified, {1}'.format('extras_' + self.id_field, self.id_field)

            results,size = self._solr_query(query_text, field_query, fields, offset)

            for result in results:
                package_id = result['id']
                if self.doi_index:
                    package_doi = result['extras_'+self.id_field]
                    if not self.doi_index.check_doi(package_doi, package_id):
                        continue
                metadata_modified = self._utc_to_local(result.get('metadata_modified'))
                value = result.get(self.id_field) if result.get(self.id_field) else result.get('extras_' + self.id_field)
                packages_found += [{'id':package_id, 'datestamp':metadata_modified, self.id_field:value}]

            #TODO: Search within resources
            log.debug('\nRESOURCES\n')
            resources_list = toolkit.get_action('resource_search')({}, {'query': 'doi:10.16904/'})
            log.debug(resources_list)

        except oaipmh_error.OAIPMHError, e:
            raise e
        except Exception, e:
            log.exception(e)
            return [],0

        return packages_found, size


    def _get_ressumption_token(self, start_date, end_date, format, offset, num_sent, size):
       offset = int(offset)
       token = collections.OrderedDict()

       token['@completeListSize'] = str(size)
       token['@cursor'] = str(offset)

       if (offset+num_sent)<size:
           params_list = []
           if start_date:
               params_list += ['from={0}'.format(self._format_date(start_date))]

           if not end_date:
               end_date = datetime.now().strftime(self.dateformat)
           params_list += ['until={0}'.format(self._format_date(end_date))]

           params_list += ['metadataPrefix={0}'.format(format)]
           params_list += ['offset={0}'.format(offset+num_sent)]
           params_list += ['size={0}'.format(size)]
           token['#text'] = urllib.quote(u'&'.join(params_list).encode('utf8'))

       return ({'resumptionToken':token})

    def _envelop_record(self, identifier, datestamp, content):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['record'] = self._envelop_header(identifier, datestamp)

        if not isinstance(content, dict):
            content = {'#text': str(content)}
        else:
            oaipmh_dict['record']['metadata'] = content

        return oaipmh_dict

    def _envelop_header(self, identifier, datestamp):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['header'] = collections.OrderedDict()
        oaipmh_dict['header']['identifier'] = identifier
        oaipmh_dict['header']['datestamp'] = datestamp.strftime(self.dateformat)

        return oaipmh_dict

#from ckan.logic.action import package_search
import ckan.lib.search.common as ckan_search
from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import JSONRecord, XMLRecord
from ckanext.package_converter.logic import package_export_as_record
import ckan.plugins.toolkit as toolkit

from pylons import config

from datetime import datetime
import collections

import oaipmh_error

import logging
log = logging.getLogger(__name__)

class RecordAccessService(object):
    def __init__(self, dateformat, id_prefix, id_field, regex):
        self.dateformat = dateformat
        self.id_prefix = id_prefix
        self.id_field = id_field
        self.regex = regex

    def get_record(self, oai_identifier, format):
        # Get record
        id = self._get_ckan_id(oai_identifier)

        result = self._find_by_field(id)
        log.debug(' get Record  result= '+ str(result))

        if not result:
            raise oaipmh_error.IdDoesNotExistError()

        package_id = result.get('id')
        datestamp = result.get('datestamp')

        return(self._export_package(package_id, oai_identifier, datestamp, format))

    def list_records(self, format, start_date=None, end_date=None):
        results = self._find_by_date(start_date, end_date)
        log.debug(' list_records results ' + str(results))
        if not results:
            raise oaipmh_error.NoRecordsMatchError()
        
        record_list = {'record':[]}
        
        for result in results:
            package_id = result.get('id')
            datestamp = result.get('datestamp')
            oai_identifier = self._get_oaipmh_id(result.get(self.id_field))
            record_list['record'] += [self._export_package(package_id, oai_identifier, datestamp, format)['record']]
        return record_list
    
    def list_identifiers(self, format, start_date=None, end_date=None):
        results = self.list_records(format, start_date, end_date)

        if not results:
            raise oaipmh_error.NoRecordsMatchError()
        
        identifiers_list = {'header':[]}
        
        for result in results.get('record',[]):
            identifiers_list['header'] += [result['header']]
        return identifiers_list
    
    def _export_package(self, package_id, oai_identifier, datestamp, format):
        # Convert record
        try:
            log.debug(' Found package_id = {0}'.format(package_id))
            converted_record = package_export_as_record(package_id, format)
            record = XMLRecord.from_record(converted_record)

        except Exception, e:
            log.exception(e)
            record = None
        if not record:
            raise oaipmh_error.CannotDisseminateFormatError()

        return (self._envelop_record(oai_identifier, datestamp, record.get_xml_dict()))

    def _get_oaipmh_id(self, id):
        return('{prefix}{id}'.format(prefix=self.id_prefix, id=id))

    def _get_ckan_id(self, oai_id):
        return (oai_id.split(self.id_prefix)[-1])

    def _find_by_field(self, id):
        #TODO: Replace with link to DB behind firewall!!
        field = self.id_field
        results = []
        try:
            conn = ckan_search.make_connection()
            # compatibility ckan 2.5 and 2.6
            if callable(getattr(conn, "query", None)):
                response = conn.query("{0}:{1}".format(field, id), fq='state:active', 
                                       fields='id, state, extras_doi, metadata_modified, {0}'.format(field), rows=1)
                results = response.results
            else:
                response = conn.search("{0}:{1}".format(field, id), fq='state:active', 
                                       fields='id, state, extras_doi, metadata_modified, {0}'.format(field), rows=1)
                results = response.docs
            #log.debug(results)
            package_id = results[0]['id']
            metadata_modified = results[0].get('metadata_modified')
        except Exception, e:
            log.exception(e)
            return {}
        #finally:
        #    if 'conn' in dir():
        #        conn.close()
        return {'id':package_id, 'datestamp':metadata_modified }
    
    def _format_date(self, date_input):
        if not date_input:
            return '*'
        try:
            return(datetime.strptime(date_input, self.dateformat).strftime(self.dateformat))
        except:
            try:
                return(datetime.strptime(date_input, "%Y-%m-%d").strftime(self.dateformat))
            except:
                raise oaipmh_error.BadArgumentError('Datestamp is expected one of the following formats: YYYY-MM-DDThh:mm:ssZ OR YYYY-MM-DD')

    def _find_by_date(self, start_date, end_date):
        #TODO: Add link to DB behind firewall!!
        results = []
        packages_found = []
        try:
            conn = ckan_search.make_connection()

            # compatibility ckan 2.5 and 2.6
            query_text = '{0}:{1}'.format(self.id_field, self.regex if self.regex else '*')
            
            if start_date or end_date:
                start_date_str = self._format_date(start_date)
                end_date_str = self._format_date(end_date)
                query_text += ' metadata_modified:[{0} TO {1}]'.format(start_date_str, end_date_str)

            log.debug(query_text)
            if callable(getattr(conn, "query", None)):
                response = conn.query(query_text, 
                                       fq='state:active site_id:%s' % config.get('ckan.site_id'), 
                                       fields='id, state, {0}, metadata_modified, {1}'.format('extras_' + self.id_field, self.id_field))
                results = response.results
            else:
                response = conn.search(query_text,
                                       fq='state:active site_id:%s' % config.get('ckan.site_id'), 
                                       fields='id, state, {0}, metadata_modified, {1}'.format('extras_' + self.id_field, self.id_field))
                results = response.docs
            
            for result in results:
                package_id = result['id']
                metadata_modified = result.get('metadata_modified')
                value = result.get(self.id_field) if result.get(self.id_field) else result.get('extras_' + self.id_field)
                packages_found += [{'id':package_id, 'datestamp':metadata_modified, self.id_field:value}]
        except oaipmh_error.OAIPMHError, e:
            raise e
        except Exception, e:
            log.exception(e)
            return []
        #finally:
        #    if 'conn' in dir():
        #        conn.close()
        return packages_found

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


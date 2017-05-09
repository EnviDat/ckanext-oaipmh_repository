#from ckan.logic.action import package_search
import ckan.lib.search.common as ckan_search
from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import JSONRecord, XMLRecord
from ckanext.package_converter.logic import package_export_as_record
import ckan.plugins.toolkit as toolkit

from datetime import datetime
import collections

import oaipmh_error

import logging
log = logging.getLogger(__name__)

class RecordAccessService(object):
    def __init__(self, dateformat, id_prefix, id_field):
        self.dateformat = dateformat
        self.id_prefix = id_prefix
        self.id_field = id_field

    def getRecord(self, identifier, format):
        # Get record
        id = self._get_ckan_id(identifier)

        result = self._find_by_field(self.id_field, id)
        log.debug(' get Record  result= '+ str(result))

        if not result:
            raise oaipmh_error.IdDoesNotExistError()

        package_id = result.get('id')
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

        datestamp = result.get('datestamp')
        return (self._envelop(identifier, datestamp, record.get_xml_dict()))

    def _get_oaipmh_id(self, id):
        return('{prefix}{id}'.format(prefix=self.id_prefix, id=id))

    def _get_ckan_id(self, oai_id):
        return (oai_id.split(self.id_prefix)[-1])

    def _find_by_field(self, field, id):
        #TODO: Replace with link to DB behind firewall!!
        try:
            conn = ckan_search.make_connection()

            results = []
            # compatibility ckan 2.5 and 2.6
            if callable(getattr(conn, "query", None)):
                response = conn.query("{0}:{1}".format(field, id), fq='state:active', fields='id, state, extras_doi, metadata_modified', rows=1)
                results = response.results
            else:
                response = conn.search("{0}:{1}".format(field, id), fq='state:active', fields='id, state, extras_doi, metadata_modified', rows=1)
                results = response.docs
            log.debug(results)
            package_id = results[0]['id']
            metadata_modified = results[0].get('metadata_modified')
        except Exception, e:
            log.exception(e)
            return {}
        #finally:
        #    if 'conn' in dir():
        #        conn.close()
        return {'id':package_id, 'datestamp':metadata_modified}

    def _envelop(self, identifier, datestamp, content):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['record'] = collections.OrderedDict()
        oaipmh_dict['record']['header'] = collections.OrderedDict()
        oaipmh_dict['record']['header']['identifier'] = identifier
        oaipmh_dict['record']['header']['datestamp'] = datestamp.strftime(self.dateformat)

        if not isinstance(content, dict):
            content = {'#text': str(content)}
        else:
            #fix metadata header
            root_key = content.keys()[0]
            oaipmh_dict['record']['metadata'] = collections.OrderedDict()
            oaipmh_dict['record']['metadata'][root_key] = content[root_key]
            for key in content[root_key]:
                if key.startswith('@xmlns'):
                    oaipmh_dict['record']['metadata'][key] =  content[root_key][key]
                    oaipmh_dict['record'][key] =  content[root_key][key]

        return oaipmh_dict



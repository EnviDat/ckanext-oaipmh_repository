#from ckan.logic.action import package_search
import ckan.lib.search.common as ckan_search
from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import JSONRecord, XMLRecord
from ckanext.package_converter.logic import package_export_as_record
import ckan.plugins.toolkit as toolkit

from datetime import datetime
import collections


import logging
log = logging.getLogger(__name__)

class RecordAccessService(object):
    def __init__(self, dateformat, id_prefix):
        self.dateformat = dateformat
        self.id_prefix = id_prefix
#<error code="idDoesNotExist">Identifier not found in this repository.</error>
#<error code="cannotDisseminateFormat">The metadata format identified by the value given for the metadataPrefix argument is not supported by the item or by the repository.</error>
    def getRecord(self, identifier, format):
        # Get record
        doi = self._get_DOI(identifier)
        log.debug(' * get by DOI ' + doi )
        result = self._find_by_DOI(doi)
        log.debug('  * result= '+ str(result))
        if not result:
            return 'ERROR idDoesNotExist'
        package_id = result.get('id')
        # Convert record
        #record = toolkit.get_action('package_export')({},{'id': package_id, 'format':format})
        try:
            record = XMLRecord.from_record(package_export_as_record(package_id, format))
        except:
            record = None
        if not record:
            return 'ERROR cannotDisseminateFormat'

        datestamp = result.get('datestamp')
        return (self._envelop(identifier, datestamp, record.get_xml_dict()))

    def _get_OAI_identifier(self, doi):
        return('{prefix}{doi}'.format(prefix=self.id_prefix, doi=doi))

    def _get_DOI(self, oai_id):
        return (oai_id.split(self.id_prefix)[-1])

    def _find_by_DOI(self, doi):
        #TODO: Replace with link to DB behind firewall!!
        try:
            conn = ckan_search.make_connection()
            
            results = []
            # compatibility ckan 2.5 and 2.6
            if callable(getattr(conn, "query", None)):
                results = conn.query("doi:{0}".format(doi), fq='state:active', fields='id, state, extras_doi, metadata_modified', rows=1)
            else:
                response = conn.search("doi:{0}".format(doi), fq='state:active', fields='id, state, extras_doi, metadata_modified', rows=1)
                results = response.docs
            
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
            oaipmh_dict['record']['metadata'] = content
            #fix metadata header
            root_key = content.keys()[0]
            for key in content[root_key]:
                if key.startswith('@xmlns'):
                    oaipmh_dict['record']['metadata'][key] =  content[root_key][key]

        return oaipmh_dict



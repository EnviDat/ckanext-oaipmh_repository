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

    def getRecord(self, identifier, format):
        # Get record
        doi = self._get_DOI(identifier)
        log.debug('********** getting DOI ' + doi + ' ************')
        result = self._find_by_DOI(doi)
        package_id = result.get('id')
        log.debug('  * package_id '+ str(package_id))
        # Convert record
        #record = toolkit.get_action('package_export')({},{'id': package_id, 'format':format})
        record = XMLRecord.from_record(package_export_as_record(package_id, format))

        datestamp = result.get('datestamp')
        log.debug(record)
        return (self._envelop(identifier, datestamp, record.get_xml_dict()))

    def _get_OAI_identifier(self, doi):
        return('{prefix}{doi}'.format(prefix=self.id_prefix, doi=doi))

    def _get_DOI(self, oai_id):
        return (oai_id.split(self.id_prefix)[-1])

    def _find_by_DOI(self, doi):
        #TODO: Replace with link to DB behind firewall!!
        try:
            conn = ckan_search.make_connection()
            response = conn.query("doi:{0}".format(doi), fq='state:active', fields='id, state, extras_doi, metadata_modified', rows=1)
            package_id = response.results[0]['id']
            metadata_modified = response.results[0].get('metadata_modified')
        except Exception, e:
            log.exception(e)
            return {}
        finally:
            if 'conn' in dir():
                conn.close()
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
            #fix header
            root_key = content.keys()[0]
            for key in content[root_key]:
                log.debug(key)
                if key.startswith('@'):
                    if key == '@xsi:schemaLocation':
                        pass
                        #content[root_key][key] = content[root_key][key].split(' ')[-1]
                    else:
                        #pass
                        content[root_key].pop(key, None)
            content[root_key]['@xmlns:dc']="http://purl.org/dc/elements/1.1"
            content[root_key]['@xmlns:oai_dc']="http://www.openarchives.org/OAI/2.0"
            content[root_key]['@xmlns:xsi']="http://www.w3.org/2001/XMLSchema-instance"
            content[root_key]['@xsi:schemaLocation']="http://www.openarchives.org/OAI/2.0/oai_dc.xsd"

        oaipmh_dict['record']['metadata'] = content

        return oaipmh_dict



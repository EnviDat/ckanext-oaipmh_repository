import collections
import logging
import urllib
from datetime import datetime
import urllib.parse
from ckan.logic import get_action


from ckanext.package_converter.logic import export_as_record
from ckanext.package_converter.model.record import XMLRecord
from ckanext.oaipmh_repository.oai_solr_index import OAISolrIndex
import ckanext.oaipmh_repository.oaipmh_error as oaipmh_error
import ckanext.oaipmh_repository.util as util

log = logging.getLogger(__name__)


class RecordAccessService(object):

    def __init__(self, dateformat, id_prefix, id_field, regex, oai_solr_url, max_results=1000,
                 local_tz='Europe/Berlin'):
        self.dateformat = dateformat
        self.local_tz = local_tz
        self.id_prefix = id_prefix
        self.id_field = id_field
        self.regex = regex
        self.oai_solr = OAISolrIndex(oai_solr_url, self.dateformat, self.local_tz)
        self.max_results = max_results
        self.ckan_dateformat = "%Y-%m-%dT%H:%M:%S.%f"

    def get_record(self, oai_identifier, format):
        # Get record
        value = self._get_ckan_field_value(oai_identifier)

        # results, size = self.doi_solr.query_by_field(self.id_field, value)
        dataset = get_action('package_show')({'ignore_auth': True, 'validate': False}, {'id': value})

        if not dataset:
            raise oaipmh_error.IdDoesNotExistError()

        ckan_id = dataset['id']
        entity = 'package'
        datestamp = dataset['metadata_modified']

        return (self._export_dataset(ckan_id, oai_identifier, datestamp, format))

    def list_records(self, format, start_date=None, end_date=None, offset=0):

        # results 
        results, size = self.oai_solr.query(self.id_field, self.regex, start_date, end_date, offset,
                                            max_rows=self.max_results)

        log.debug('list_records: got {0} out of {1} results'.format(len(results), size))

        if not results:
            raise oaipmh_error.NoRecordsMatchError()

        record_list = collections.OrderedDict()
        record_list['record'] = []

        for result in results:
            ckan_id = result['id']
            entity = 'package'
            # if entity == 'resource':
            #    ckan_id = result['resource_id']
            datestamp = result['metadata_modified']
            state = result['state']

            oai_identifier = self._get_oaipmh_id(result.get(self.id_field))

            record_list['record'] += [
                self._export_dataset(ckan_id, oai_identifier, datestamp, format, state, entity)['record']]

        if size != len(results):
            token = self._get_ressumption_token(start_date, end_date, format, offset, len(results), size)
            log.debug(token)
            record_list['resumptionToken'] = token['resumptionToken']

        return record_list

    def list_identifiers(self, format, start_date=None, end_date=None, offset=0):
        results = self.list_records(format, start_date, end_date, offset=offset)

        if not results:
            raise oaipmh_error.NoRecordsMatchError()

        identifiers_list = collections.OrderedDict()
        identifiers_list['header'] = []

        for result in results.get('record', []):
            identifiers_list['header'] += [result['header']]

        if results.get('resumptionToken', False):
            identifiers_list['resumptionToken'] = results.get('resumptionToken')

        return identifiers_list

    def _export_dataset(self, ckan_id, oai_identifier, datestamp, format, state='active', entity='package'):

        if state != 'active':
            return (self._envelop_record(oai_identifier, datestamp, {}, state))

        # Convert record
        try:
            converted_record = export_as_record(ckan_id, format, type=entity)
            record = XMLRecord.from_record(converted_record)

        except Exception as e:
            log.exception(e)
            record = None
        if not record:
            raise oaipmh_error.CannotDisseminateFormatError()

        return self._envelop_record(oai_identifier, datestamp, record.get_xml_dict(), state)

    def _get_oaipmh_id(self, id):
        return '{prefix}{id}'.format(prefix=self.id_prefix, id=id)

    def _get_ckan_field_value(self, oai_id):
        return oai_id.split(self.id_prefix)[-1]

    def _get_ressumption_token(self, start_date, end_date, format, offset, num_sent, size):
        offset = int(offset)
        token = collections.OrderedDict()

        token['@completeListSize'] = str(size)
        token['@cursor'] = str(offset)

        if (offset + num_sent) < size:
            params_list = []
            if start_date:
                params_list += ['from={0}'.format(util.format_date(start_date, self.dateformat, self.local_tz))]

            if not end_date:
                end_date = datetime.now().strftime(self.dateformat)

            params_list += ['until={0}'.format(util.format_date(end_date, self.dateformat, self.local_tz))]

            params_list += ['metadataPrefix={0}'.format(format)]
            params_list += ['offset={0}'.format(offset + num_sent)]
            params_list += ['size={0}'.format(size)]
            token['#text'] = urllib.parse.quote(u'&'.join(params_list).encode('utf8'))

        return {'resumptionToken': token}

    def _envelop_record(self, identifier, datestamp, content, state='active'):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['record'] = self._envelop_header(identifier, datestamp, state)

        if not isinstance(content, dict):
            content = {'#text': str(content)}
        else:
            if state == 'active':
                oaipmh_dict['record']['metadata'] = content

        return oaipmh_dict

    def _envelop_header(self, identifier, datestamp, state='active'):
        oaipmh_dict = collections.OrderedDict()

        # Header
        oaipmh_dict['header'] = collections.OrderedDict()
        oaipmh_dict['header']['identifier'] = identifier

        # Convert CKAN Dateformat 2018-12-07T12:53:12.093092 to OAIPMH
        datestamp_date = datestamp
        if type(datestamp_date) is not datetime:
            datestamp_date = datetime.strptime(datestamp, self.ckan_dateformat)

        oaipmh_dict['header']['datestamp'] = datestamp_date.strftime(self.dateformat)

        # deleted records
        if state != 'active':
            oaipmh_dict['header']['@status'] = 'deleted'

        return oaipmh_dict

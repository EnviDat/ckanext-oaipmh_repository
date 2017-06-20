from solr import SolrConnection
import pytz
from datetime import datetime
import oaipmh_error

import logging
log = logging.getLogger(__name__)

class DoiSolrNode(object):

    def __init__(self, url, local_tz='Europe/Berlin'):
        self.url = url
        self.local_tz=pytz.timezone(local_tz)

    def _make_connection(self):
        assert self.url is not None
        return SolrConnection(solr_url)

    def _solr_query(self, query_text, field_query, fields, max_rows, offset=0):
        results = []
        size = 0

        conn = self.make_connection()
        if callable(getattr(conn, "query", None)):
            # CKAN 2.5
            response = conn.query(query_text, fq=field_query, fields = fields,
                                  rows=max_rows, start=offset)
            results = response.results
            size = int(response.results.numFound)
        else:
            # CKAN 2.6
            response = conn.search(query_text, fq=field_query, fields = fields,
                                       rows=max_rows, start=offset)
            results = response.docs
            size = len(response.docs)

        return results,size

    def query_by_field(self, field, value, max_rows=1):
        return self.query (field, value, None, None, offset=0, max_rows=max_rows)

    def query (self, field, value, start_date, end_date, offset=0, max_rows=100):
        offset=int(offset)
        try:
            # search within packages
            query_text = "{0}:{1}".format(field, value if value else '*')

            if start_date or end_date:
                start_date_str = self._format_date(start_date, to_utc=True)
                end_date_str = self._format_date(end_date, to_utc=True)
                query_text += ' metadata_modified:[{0} TO {1}]'.format(start_date_str, end_date_str)

            field_query = 'state:active capacity:public'
            fields='package_id, resource_id, state, metadata_modified, entity, {0}, {1}'.format(field, 'extras_'+field)
            results,size = self._solr_query(query_text, field_query, fields, max_rows, offset)
            # format the date
            for result in results:
                if not result.get(field):
                    result[field] = result.get('extras_'+field)
                result['datestamp'] = self._utc_to_local(result.get('metadata_modified'))
            return results,size

        except oaipmh_error.OAIPMHError, e:
            raise e
        except Exception, e:
            log.exception(e)
            return [],0

    def _utc_to_local(self, date_utc):
        return date_utc.replace(tzinfo=pytz.utc).astimezone(self.local_tz)

    def _local_to_utc(self, date_local):
        try:
            date_local_tz = self.local_tz.localize(date_local)
            date_local_tz_norm = self.local_tz.normalize(date_local_tz)
            return date_local_tz_norm.astimezone(pytz.utc)
        except Exception as e:
            log.debug(e)
            raise

    def _format_date(self, date_input, offset=0, to_utc=False):
        if not date_input:
            return '*'

        try:
            local_dt = datetime.strptime(date_input, self.dateformat)
            if to_utc:
                return(self._local_to_utc(local_dt).strftime(self.dateformat))
            else:
                return(local_dt.strftime(self.dateformat))
        except:
            try:
                return(datetime.strptime(date_input, "%Y-%m-%d").strftime(self.dateformat))
            except:
                raise oaipmh_error.BadArgumentError('Datestamp is expected one of the following formats: YYYY-MM-DDThh:mm:ssZ OR YYYY-MM-DD')


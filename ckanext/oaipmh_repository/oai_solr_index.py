from datetime import datetime
import oaipmh_error
import util

import logging
log = logging.getLogger(__name__)

import pysolr
import simplejson
import re

class OAISolrIndex(object):

    def __init__(self, url, dateformat, local_tz='Europe/Berlin'):
        self.url = url
        self.dateformat = dateformat
        self.local_tz=local_tz

    def _make_connection(self):
        assert self.url is not None
        decoder = simplejson.JSONDecoder(object_hook=self.solr_datetime_decoder)
        return pysolr.Solr(self.url, decoder=decoder)

    def solr_datetime_decoder(self, d):
        for k, v in d.items():
            if isinstance(v, basestring):
                possible_datetime = re.search(pysolr.DATETIME_REGEX, v)
                if possible_datetime:
                    date_values = possible_datetime.groupdict()
                    for dk, dv in date_values.items():
                        date_values[dk] = int(dv)

                    d[k] = datetime( date_values['year'],
									 date_values['month'],
									 date_values['day'],
									 date_values['hour'],
									 date_values['minute'],
									 date_values['second'])
        return d

    def _solr_query(self, query_text, field_query, fields, max_rows, offset=0):
        results = []
        size = 0

        conn = self._make_connection()
        
        response = conn.search(query_text, fq=field_query, fields = fields,
                                       rows=max_rows, start=offset)
        results = response.docs
        size = int(response.hits)

        return results,size

    def query_by_field(self, field, value, max_rows=1):
        return self.query (field, value, None, None, offset=0, max_rows=max_rows)

    def query (self, field, value, start_date, end_date, offset=0, max_rows=100):
        offset=int(offset)
        try:
            # search within packages
            query_text = "{0}:{1}".format(field, value if value else '*')

            if start_date or end_date:
                start_date_str = util.format_date(start_date, self.dateformat, self.local_tz, to_utc=True)
                end_date_str = util.format_date(end_date, self.dateformat, self.local_tz, to_utc=True)
                query_text += ' metadata_modified:[{0} TO {1}]'.format(start_date_str, end_date_str)

            field_query = 'state:active capacity:public'
            fields='id, state, metadata_modified, {0}, {1}'.format(field, 'extras_'+field)
            results,size = self._solr_query(query_text, field_query, fields, max_rows, offset)
            # format the date
            for result in results:
                if not result.get(field):
                    result[field] = result.get('extras_'+field)
                result['datestamp'] = util.utc_to_local(result.get('metadata_modified'), self.local_tz)
            return results,size

        except oaipmh_error.OAIPMHError, e:
            raise e
        except Exception, e:
            log.exception(e)
            return [],0

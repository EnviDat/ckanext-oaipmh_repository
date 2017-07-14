from datetime import datetime
import pytz

import oaipmh_error

import logging
log = logging.getLogger(__name__)


def local_to_utc(date_local, local_tz):
    if isinstance(local_tz, basestring):
        local_tz=pytz.timezone(local_tz)
        
    try:
        date_local_tz = local_tz.localize(date_local)
        date_local_tz_norm = local_tz.normalize(date_local_tz)
        return date_local_tz_norm.astimezone(pytz.utc)
    except Exception as e:
        log.exception(e)
        raise

def utc_to_local(date_utc, local_tz):
    if isinstance(local_tz, basestring):
        local_tz=pytz.timezone(local_tz)
        
    return date_utc.replace(tzinfo=pytz.utc).astimezone(local_tz)

def format_date(date_input, dateformat, local_tz, offset=0, to_utc=False):
    local_tz=pytz.timezone(local_tz)
    
    if not date_input:
        return '*'

    try:
        local_dt = datetime.strptime(date_input, dateformat)
        if to_utc:
            return(local_to_utc(local_dt, local_tz).strftime(dateformat))
        else:
            return(local_dt.strftime(dateformat))
    except:
        try:
            return(datetime.strptime(date_input, "%Y-%m-%d").strftime(dateformat))
        except Exception as e:
            log.exception(e)
            raise oaipmh_error.BadArgumentError('Datestamp is expected one of the following formats: YYYY-MM-DDThh:mm:ssZ OR YYYY-MM-DD, got {0}'.format(date_input))


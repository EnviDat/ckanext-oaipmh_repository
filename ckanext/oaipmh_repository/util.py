from datetime import datetime
import oaipmh_error

import logging
log = logging.getLogger(__name__)


def local_to_utc(date_local, local_tz):
    try:
        date_local_tz = local_tz.localize(date_local)
        date_local_tz_norm = local_tz.normalize(date_local_tz)
        return date_local_tz_norm.astimezone(pytz.utc)
    except Exception as e:
        log.debug(e)
        raise

def format_date(date_input, dateformat, local_tz, offset=0, to_utc=False):
    log.debug(date_input)
    log.debug(to_utc)
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
            log.debug(e)
            raise oaipmh_error.BadArgumentError('Datestamp is expected one of the following formats: YYYY-MM-DDThh:mm:ssZ OR YYYY-MM-DD')


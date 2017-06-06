import sqlalchemy
import sys

import logging
log = logging.getLogger(__name__)

class OAIPMHDOIIndex(object):
    def __init__(self, url, site_id):
        self.url = url
        self.site_id = site_id
        self.con = sqlalchemy.create_engine(url, client_encoding='utf8')
        self.meta = sqlalchemy.MetaData(bind=self.con, reflect=True)

    def check_doi(self, doi, ckan_id, entity='package'):
        prefix = doi.split('/',1)[0]
        suffix = doi.split('/',1)[1]
        doi_realisation = self.meta.tables['doi_realisation']

        log.debug('check_doi doi = {0}, ckan_id = {1}, entity = {2}, site_id = "{3}"'.format(doi, ckan_id, entity, self.site_id))

        clause = sqlalchemy.select([doi_realisation.c.prefix,
                                    doi_realisation.c.suffix]
                                   ).where(doi_realisation.c.site_id == self.site_id
                                   ).where(doi_realisation.c.ckan_entity == entity
                                   ).where(doi_realisation.c.ckan_id == ckan_id)
        results = self.con.execute(clause).fetchall()
        log.debug(results)
        if not results:
           log.warn('CKAN ID not found ({entity}): {ckan_id}'.format(entity=entity, ckan_id=ckan_id))
           return(False)

        for row in results:
            db_prefix=str(row[0][0])
            db_suffix=str(row[1])
            if(prefix==db_prefix) and (suffix==db_suffix):
                log.debug('Check CKAN ID-DOI OK!! {entity}: {prefix}/{suffix}: {ckan_id}'.format(
                                                      entity=entity,
                                                      prefix=db_prefix,
                                                      suffix=db_suffix, ckan_id=ckan_id))
                return True
            log.error('CKAN ID linked to another DOI: {prefix}/{suffix}: {ckan_id}'.format(prefix=db_prefix,
                                                     suffix=db_suffix,
                                                     ckan_id=ckan_id))
        return(False)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __unicode__(self):
        return (u'OAIPMHDOIIndex({0}): \'{1}\' '.format(self.site_id, self.con))

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import urllib

from pylons import config

import logging
log = logging.getLogger(__name__)

class Oaipmh_RepositoryPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IPackageController, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'oaipmh_repository')

    # IRoutes
    def before_map(self, map):
        '''Map the controller to be used for OAI-PMH.
        '''
        controller = 'ckanext.oaipmh_repository.controller:OAIPMHController'
        map.connect('oai', '/oai', controller=controller, action='index')
	return map

    # IPackageController
    def before_index(self, pkg_dict):
        log.debug(' *** BEFORE INDEX ***')
        doi_solr_url = config.get('oaipmh_repository.solr_url')
        if doi_solr_url:
            url_delta = doi_solr_url + '/dataimport?command=delta-import&clean=false&wt=json'
            log.debug('Calling delta-import: {0} '.format(url_delta))
            fileobj = urllib.urlopen(url_delta)
            log.debug(fileobj.read())
        return pkg_dict

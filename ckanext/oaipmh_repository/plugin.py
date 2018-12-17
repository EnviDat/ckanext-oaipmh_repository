import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import urllib
import time

from multiprocessing import Process

from pylons import config

import logging
log = logging.getLogger(__name__)

class Oaipmh_RepositoryPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IRoutes, inherit=True)

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

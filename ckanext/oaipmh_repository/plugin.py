import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckanext.oaipmh_repository.blueprints as blueprints

import logging
log = logging.getLogger(__name__)


class Oaipmh_RepositoryPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IBlueprint, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')

    def get_blueprint(self):
        return blueprints.get_blueprints(self.name, self.__module__)

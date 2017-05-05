"""Tests for plugin.py."""
import ckanext.oaipmh_repository.plugin as plugin
from ckanext.oaipmh_repository.oaipmh_repository import OAIPMHRepository

from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import XMLRecord

import ckan.plugins
import ckan.model as model

from nose.tools import assert_equal, assert_true

from logging import getLogger
log = getLogger(__name__)

class TestPackageConverter(object):
    '''Tests for the ckanext.oaipmh_repository.plugin module.

    Specifically tests the OAI-PMH requests.
    '''
    def _get_test_app(self):

        # Return a test app with the custom config.
        app = ckan.config.middleware.make_app(config['global_conf'], **config)
        app = webtest.TestApp(app)

        ckan.plugins.load('oaipmh_repository')
        return app

    @classmethod
    def setup_class(cls):
        '''Nose runs this method once to setup our test class.'''
        # Test code should use CKAN's plugins.load() function to load plugins
        # to be tested.
        ckan.plugins.load('oaipmh_repository')

    def teardown(self):
        '''Nose runs this method after each test method in our test class.'''
        # Rebuild CKAN's database after each test method, so that each test
        # method runs with a clean slate.
        model.repo.rebuild_db()

    @classmethod
    def teardown_class(cls):
        '''Nose runs this method once after all the test methods in our class
        have been run.

        '''
        # We have to unload the plugin we loaded, so it doesn't affect any
        # tests that run after ours.
        ckan.plugins.unload('oaipmh_repository')

    def test_identify(self):
        identify_content = OAIPMHRepository().handle_request('Identify', {}, 'REQUEST_URL')
        log.info(identify_content)
        #Add metadata format oai-pmh(2.0),  http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd
        identify_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], identify_content)
        log.info(identify_record)
        # validate the XML
        assert_true(identify_record.validate())


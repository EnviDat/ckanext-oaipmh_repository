"""Tests for plugin.py."""
import ckanext.oaipmh_repository.plugin as plugin
from ckanext.oaipmh_repository.oaipmh_repository import OAIPMHRepository

from ckanext.package_converter.model.metadata_format import MetadataFormats
from ckanext.package_converter.model.record import XMLRecord, Record
from ckanext.package_converter.model.converter import Converters, BaseConverter

import ckan.plugins
import ckan.model as model
import ckan.tests.factories as factories
import ckan.lib.search as search

from xmltodict import unparse

import collections
 
from nose.tools import assert_equal, assert_true, assert_false

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
        
        model.repo.rebuild_db()
        search.index_for('Package').clear()
        search.rebuild()
        
        Converters().converters_dict = {}
        Converters().set_converter(TestOAIDCConverter())
                
    def teardown(self):
        '''Nose runs this method after each test method in our test class.'''
        # Rebuild CKAN's database after each test method, so that each test
        # method runs with a clean slate.
        model.repo.rebuild_db()
        search.index_for('Package').clear()
        search.rebuild()
        
    @classmethod
    def teardown_class(cls):
        '''Nose runs this method once after all the test methods in our class
        have been run.

        '''
        # We have to unload the plugin we loaded, so it doesn't affect any
        # tests that run after ours.
        ckan.plugins.unload('oaipmh_repository')

    def test_bad_request(self):
        request_content = OAIPMHRepository().handle_request('badverb', {}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)
        # validate the XML
        assert_true(oaipmh_record.validate())
        assert_true(OAIPMHRepository()._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))

    def test_identify(self):
        request_content = OAIPMHRepository().handle_request('Identify', {}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)
        # validate the XML
        assert_true(oaipmh_record.validate())
        assert_false(OAIPMHRepository()._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))

    def test_list_metadata_formats(self):
        request_content = OAIPMHRepository().handle_request('ListMetadataFormats', {}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)
        # validate the XML
        assert_true(oaipmh_record.validate())
        assert_false(OAIPMHRepository()._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))

    def test_get_record(self):
        dataset = factories.Dataset(name='dataset_test_api_export', author='Test Plugin')
        repository = OAIPMHRepository()
        oaipmh_identifier = repository.record_access._get_oaipmh_id(dataset.get(repository.id_field))

        request_content = repository.handle_request('GetRecord', {'identifier':oaipmh_identifier, 
                                                                        'metadataPrefix':'oai_dc'}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)

        # validate the XML
        assert_true(repository._is_valid_oai_pmh_record(oaipmh_record.get_xml_dict()))
        assert_false(repository._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))
        
    def test_list_records(self):
        dataset = factories.Dataset(name='dataset_test_api_export_01', author='Test Plugin')
        dataset = factories.Dataset(name='bad_dataset_test_api_export', author='Test Plugin')
        dataset = factories.Dataset(name='dataset_test_api_export_02', author='Test Plugin')
        repository = OAIPMHRepository()

        request_content = repository.handle_request('ListRecords', {'metadataPrefix':'oai_dc'}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)

        # validate the XML
        assert_true(repository._is_valid_oai_pmh_record(oaipmh_record.get_xml_dict()))
        assert_false(repository._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))
        
    def test_list_identifiers(self):
        dataset = factories.Dataset(name='dataset_test_api_export_01', author='Test Plugin')
        dataset = factories.Dataset(name='bad_dataset_test_api_export', author='Test Plugin')
        dataset = factories.Dataset(name='dataset_test_api_export_02', author='Test Plugin')
        repository = OAIPMHRepository()

        request_content = repository.handle_request('ListIdentifiers', {'metadataPrefix':'oai_dc'}, 'REQUEST_URL')
        oaipmh_record = XMLRecord(MetadataFormats().get_metadata_formats('oai_pmh')[0], request_content)

        # validate the XML
        assert_true(repository._is_valid_oai_pmh_record(oaipmh_record.get_xml_dict()))
        assert_false(repository._is_error_oai_pmh_record(oaipmh_record.get_xml_dict()))
        

class TestOAIDCConverter(BaseConverter):

    def __init__(self):
        oaidc_output_format = MetadataFormats().get_metadata_formats('oai_dc')[0]
        BaseConverter.__init__(self, oaidc_output_format)

    def convert(self, record):
        if self.can_convert(record):
            dataset_dict = record.get_json_dict()
            oai_dc_dict = collections.OrderedDict()
            oai_dc_dict['oai_dc:dc'] = collections.OrderedDict()
            oai_dc_dict['oai_dc:dc']['@xmlns:oai_dc']='http://www.openarchives.org/OAI/2.0/oai_dc/'
            oai_dc_dict['oai_dc:dc']['@xmlns:dc']='http://purl.org/dc/elements/1.1/'
            oai_dc_dict['oai_dc:dc']['@xmlns:xsi']='http://www.w3.org/2001/XMLSchema-instance'
            oai_dc_dict['oai_dc:dc']['@xsi:schemaLocation'] = 'http://www.openarchives.org/OAI/2.0/oai_dc/  http://www.openarchives.org/OAI/2.0/oai_dc.xsd'

            oai_dc_dict['oai_dc:dc']['dc:identifier']= dataset_dict.get('id','')
            oai_dc_dict['oai_dc:dc']['dc:identifier']= dataset_dict.get('name','')
            oai_dc_dict['oai_dc:dc']['dc:creator']= dataset_dict.get('author','')
            oai_dc_dict['oai_dc:dc']['dc:date']= dataset_dict.get('metadata_modified','2017').split('-')[0]
            oai_dc_dict['oai_dc:dc']['dc:title']= dataset_dict.get('title','')
            oai_dc_dict['oai_dc:dc']['dc:type']= 'Dataset'

            converted_record = Record(self.output_format, unparse(oai_dc_dict, pretty=True))
            return XMLRecord.from_record(converted_record)

            return converted_record
        else:
            raise TypeError(('Converter is not compatible with the record format {record_format}({record_version}). ' +
                             'Accepted format is CKAN {input_format}.').format(
                                 record_format=record.get_metadata_format().get_format_name(), record_version=record.get_metadata_format().get_version(),
                                 input_format=self.get_input_format().get_format_name()))

    def __unicode__(self):
        return super(TestOAIDCConverter, self).__unicode__() + u' Test OAI-DC Converter. '



import unittest
import lodcc

class LodccTestCase( unittest.TestCase ):

    def test_download_prepare( self ):

        # n_triples
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )
        url, format_ = lodcc.download_prepare( ['id', 'name', 'https://example.org/dataset.nt', 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.nt', 'application_n_triples' ) )

        # rdf_xml
        url, format_ = lodcc.download_prepare( ['id', 'name', None, 'https://example.org/dataset.rdf'] )
        self.assertEqual( ( url, format_ ), ( 'https://example.org/dataset.rdf', 'application_rdf_xml' ) )

    def test_download_prepare__None( self ):

        self.assertIsNotNone( lodcc.download_prepare( None ) )
        self.assertIsNone( lodcc.download_prepare( None )[0] )
        self.assertIsNone( lodcc.download_prepare( ['id', 'name', None, None] )[0] )

    def test_download_data( self ):

        # no filename in url, suppose filename is taken from dataset name
        self.assertEqual( 'dump_foo-lod.rdf', lodcc.download_data( [None,'foo-lod'], 'http://www.gesis.org/missy/metadata/MZ/2012', 'application_rdf_xml' ) )
        
    def test_download_data__fails( self ):

        # url wrong
        self.assertIsNone( lodcc.download_data( [None,'foo-lod'], 'http://www.gesis.org/missy/metadata/MZ/2020', 'application_rdf_xml' ) )
        # url correct but file not there
        self.assertIsNone( lodcc.download_data( [None,'foo-lod'], 'http://lak.linkededucation.org/lak/LAK-DATASET-DUMP.nt.zip', 'application_n_triples' ) )
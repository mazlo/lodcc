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

    def test_download_prepare_None( self ):

        self.assertIsNotNone( lodcc.download_prepare( None ) )
        self.assertIsNone( lodcc.download_prepare( None )[0] )
        self.assertIsNone( lodcc.download_prepare( ['id', 'name', None, None] )[0] )
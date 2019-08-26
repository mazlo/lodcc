import unittest
import unitgraphs
import graph.metrics.fernandez_et_al.object_in_degrees as oid

class MetricsTestCase( unittest.TestCase ):
    """"""

    def setUp( self ):
        """"""
        self.G = unitgraphs.basic_graph()
        self.stats = dict()

    def test_in_degree( self ):
        """"""
        oid.in_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_in_degree'], 3 )
        self.assertEqual( round( self.stats['mean_in_degree'], 2 ), 1.40 )

    def test_partial_in_degree( self ):
        """"""
        oid.partial_in_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_partial_in_degree'], 2 )
        self.assertEqual( round( self.stats['mean_partial_in_degree'], 2 ), 1.17 )

    def test_labelled_in_degree( self ):
        """"""
        oid.labelled_in_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_labelled_in_degree'], 2 )
        self.assertEqual( round( self.stats['mean_labelled_in_degree'], 2 ), 1.20 )

    def test_direct_in_degree( self ):
        """"""
        oid.direct_in_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_direct_in_degree'], 3 )
        self.assertEqual( round( self.stats['mean_direct_in_degree'], 2 ), 1.40 )

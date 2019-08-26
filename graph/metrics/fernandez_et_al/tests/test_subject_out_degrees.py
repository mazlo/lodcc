import unittest
import unitgraphs
import graph.metrics.fernandez_et_al.subject_out_degrees as sod

class MetricsTestCase( unittest.TestCase ):
    """"""

    def setUp( self ):
        """"""
        self.G = unitgraphs.basic_graph()
        self.stats = dict()

    def test_out_degree( self ):
        """"""
        sod.out_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_out_degree'], 4 )
        self.assertEqual( round( self.stats['mean_out_degree'], 2 ), 1.75 )

    def test_partial_out_degree( self ):
        """"""
        sod.partial_out_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_partial_out_degree'], 2 )
        self.assertEqual( round( self.stats['mean_partial_out_degree'], 2 ), 1.17 )

    def test_labelled_out_degree( self ):
        """"""
        sod.labelled_out_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_labelled_out_degree'], 3 )
        self.assertEqual( round( self.stats['mean_labelled_out_degree'], 2 ), 1.50 )

    def test_direct_out_degree( self ):
        """"""
        sod.direct_out_degree( self.G, None, self.stats )

        self.assertEqual( self.stats['max_direct_out_degree'], 4 )
        self.assertEqual( round( self.stats['mean_direct_out_degree'], 2 ), 1.75 )

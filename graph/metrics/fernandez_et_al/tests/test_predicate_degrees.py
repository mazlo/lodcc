import unittest
import unitgraphs
import graph.metrics.fernandez_et_al.predicate_degrees as pd

class MetricsTestCase( unittest.TestCase ):
    """"""

    def setUp( self ):
        """"""
        self.G = unitgraphs.basic_graph()
        self.stats = dict()

    def test_predicate_degree( self ):
        """"""
        pd.predicate_degree( self.G, None, self.stats )
        self.assertEqual( round( self.stats['max_predicate_degree'], 2 ), 2 )
        self.assertEqual( round( self.stats['mean_predicate_degree'], 2), 1.40 )

    def test_predicate_in_degree( self ):
        """"""
        pd.predicate_in_degree( self.G, None, self.stats )
        self.assertEqual( round( self.stats['max_predicate_in_degree'], 2 ), 2 )
        self.assertEqual( round( self.stats['mean_predicate_in_degree'], 2), 1.17 )

    def test_predicate_out_degree( self ):
        """"""
        pd.predicate_out_degree( self.G, None, self.stats )
        self.assertEqual( round( self.stats['max_predicate_out_degree'], 2 ), 2 )
        self.assertEqual( round( self.stats['mean_predicate_out_degree'], 2), 1.17 )

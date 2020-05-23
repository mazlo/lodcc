import unittest
import unitgraphs
import graph.measures.fernandez_et_al.predicate_lists as pl

class MetricsTestCase( unittest.TestCase ):
    """"""

    def setUp( self ):
        """"""
        self.G = unitgraphs.basic_graph()
        self.stats = dict()

    def test_repeated_predicate_lists( self ):
        """"""
        pl.repeated_predicate_lists( self.G, None, self.stats )
        self.assertEqual( round( self.stats['repeated_predicate_lists'], 2 ), 0.25 )
        self.assertEqual( round( self.stats['max_predicate_list_degree'], 2 ), 2 )
        self.assertEqual( round( self.stats['mean_predicate_list_degree'], 2 ), 1.33 )

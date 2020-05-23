import unittest
import unitgraphs
import graph.measures.fernandez_et_al.common_ratios as cr

class MetricsTestCase( unittest.TestCase ):
    """"""

    def setUp( self ):
        """"""
        self.G = unitgraphs.basic_graph()
        self.stats = dict()

    def test_subject_object_ratio( self ):
        """"""
        cr.subject_object_ratio( self.G, None, self.stats )
        self.assertEqual( round( self.stats['subject_object_ratio'], 2 ), 0.12 )

import graph.metrics.fernandez_et_al.subject_out_degrees as subject_out_degrees
from graph.metrics.fernandez_et_al.subject_out_degrees import *
import graph.metrics.fernandez_et_al.object_in_degrees as object_in_degrees
import graph.metrics.fernandez_et_al.predicate_degrees as predicate_degrees
import graph.metrics.fernandez_et_al.common_ratios as ratios
import graph.metrics.fernandez_et_al.predicate_lists as predicate_lists
from graph.metrics.fernandez_et_al.predicate_lists import *
import graph.metrics.fernandez_et_al.typed_subjects_objects as typed_so

all    = subject_out_degrees.METRICS + object_in_degrees.METRICS + predicate_degrees.METRICS + ratios.METRICS + predicate_lists.METRICS + typed_so.METRICS
LABELS = subject_out_degrees.LABELS + object_in_degrees.LABELS + predicate_degrees.LABELS + ratios.LABELS + predicate_lists.LABELS + typed_so.LABELS
SETS   = {}
SETS.update( subject_out_degrees.METRICS_SET )
SETS.update( object_in_degrees.METRICS_SET )
SETS.update( predicate_degrees.METRICS_SET )
SETS.update( predicate_lists.METRICS_SET )

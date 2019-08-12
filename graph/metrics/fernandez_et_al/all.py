import graph.metrics.fernandez_et_al.subject_out_degrees as subj_out_deg
import graph.metrics.fernandez_et_al.object_in_degrees as obj_in_deg
import graph.metrics.fernandez_et_al.predicate_degrees as pred_deg
import graph.metrics.fernandez_et_al.common_ratios as ratios
import graph.metrics.fernandez_et_al.predicate_lists as pred_lists
from graph.metrics.fernandez_et_al.predicate_lists import *
import graph.metrics.fernandez_et_al.typed_subjects_objects as typed_so

all    = subj_out_deg.METRICS + obj_in_deg.METRICS + pred_deg.METRICS + ratios.METRICS + pred_lists.METRICS + typed_so.METRICS
LABELS = subj_out_deg.LABELS + obj_in_deg.LABELS + pred_deg.LABELS + ratios.LABELS + pred_lists.LABELS + typed_so.LABELS
SETS   = {}
SETS.update( subj_out_deg.METRICS_SET )
SETS.update( obj_in_deg.METRICS_SET )
SETS.update( pred_deg.METRICS_SET )
SETS.update( pred_lists.METRICS_SET )

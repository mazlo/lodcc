import graph.metrics.fernandez_et_al.subject_out_degrees as subj_out_deg
import graph.metrics.fernandez_et_al.object_in_degrees as obj_in_deg
import graph.metrics.fernandez_et_al.predicate_degrees as pred_deg
import graph.metrics.fernandez_et_al.common_ratios as ratios
import graph.metrics.fernandez_et_al.predicate_lists as pred_lists

all    = subj_out_deg.METRICS + obj_in_deg.METRICS + pred_deg.METRICS + ratios.METRICS + pred_lists.METRICS
LABELS = subj_out_deg.LABELS + obj_in_deg.LABELS + pred_deg.LABELS + ratios.LABELS + pred_lists.LABELS
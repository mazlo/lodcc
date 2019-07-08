import graph.metrics.fernandez_et_al.subject_out_degrees as subj_out_deg
import graph.metrics.fernandez_et_al.object_in_degrees as obj_in_deg
import graph.metrics.fernandez_et_al.predicate_degrees as pred_deg
import graph.metrics.fernandez_et_al.common_ratios as ratios
import graph.metrics.fernandez_et_al.predicate_lists as pred_lists

all = subj_out_deg.all + obj_in_deg.all + pred_deg.all + ratios.all + pred_lists.all

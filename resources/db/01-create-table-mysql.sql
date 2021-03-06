CREATE TABLE stats_2017_08 ( 
    id int AUTO_INCREMENT NOT NULL PRIMARY KEY, 
    name VARCHAR(128), 
    title VARCHAR(255), 
    domain VARCHAR(255), 
    url VARCHAR(255) );

CREATE TABLE stats_graph_2017_08
(
   id int PRIMARY KEY NOT NULL,
   name varchar(128),
   n bigint,
   m bigint,
   mean_degree float(19),
   mean_degree_centrality float(19),
   mean_in_degree_centrality float(19),
   mean_out_degree_centrality float(19),
   max_degree bigint,
   max_in_degree bigint,
   max_out_degree bigint,
   h_index_d int,
   h_index_u int,
   fill float(19),
   reciprocity float(19),
   local_clustering float(19),
   global_clustering float(19),
   max_degree_vertex text,
   max_pagerank_vertex text,
   max_eigenvector_vertex text,
   pseudo_diameter real,
   pseudo_diameter_src_vertex text,
   pseudo_diameter_trg_vertex text,
   fill_overall float(19),
   parallel_edges bigint,
   m_unique bigint,
   lines_match_edgelist bool,
   max_pagerank float(19),
   max_in_degree_centrality float(19),
   max_out_degree_centrality float(19),
   max_degree_centrality float(19),
   powerlaw_exponent_degree text,
   powerlaw_exponent_degree_dmin text,
   powerlaw_exponent_in_degree text,
   powerlaw_exponent_in_degree_dmin text,
   max_in_degree_vertex text,
   max_out_degree_vertex text,
   stddev_in_degree float(19),
   stddev_out_degree float(19),
   coefficient_variation_in_degree real,
   coefficient_variation_out_degree real,
   path_edgelist text,
   path_graph_gt text,
   var_in_degree float(19),
   var_out_degree float(19),
   centralization_in_degree float(19),
   centralization_out_degree float(19),
   max_degree_vertex_uri text,
   max_pagerank_vertex_uri text,
   max_in_degree_vertex_uri text,
   max_out_degree_vertex_uri text,
   pseudo_diameter_src_vertex_uri text,
   pseudo_diameter_trg_vertex_uri text,
   centralization_degree float(19),
   domain varchar(64) );
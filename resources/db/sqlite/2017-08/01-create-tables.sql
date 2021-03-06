CREATE TABLE stats_2017_08 ( 
   id int AUTO_INCREMENT NOT NULL PRIMARY KEY, 
   name VARCHAR(128), 
   title VARCHAR(255), 
   domain VARCHAR(255), 
   url VARCHAR(255),
   application_n_triples VARCHAR(255),
   application_rdf_xml VARCHAR(255),
   text_turtle VARCHAR(255),
   text_n3 VARCHAR(255),
   application_n_quads VARCHAR(255),
   unknown VARCHAR(255) );

CREATE TABLE stats_graph_2017_08 (
   id int PRIMARY KEY NOT NULL,
   name varchar(128),
   domain varchar(64),
   n bigint,
   m bigint,
   max_degree bigint,
   mean_degree float(19),
   max_in_degree bigint,
   max_out_degree bigint,
   h_index_d int,
   h_index_u int,
   fill float(19),
   fill_overall float(19),
   m_unique bigint,
   parallel_edges bigint,
   reciprocity float(19),
   pseudo_diameter real,
   powerlaw_exponent_degree text,
   powerlaw_exponent_degree_dmin text,
   powerlaw_exponent_in_degree text,
   powerlaw_exponent_in_degree_dmin text,
   stddev_in_degree float(19),
   stddev_out_degree float(19),
   var_in_degree float(19),
   var_out_degree float(19),
   coefficient_variation_in_degree real,
   coefficient_variation_out_degree real,
   centralization_degree float(19),
   centralization_in_degree float(19),
   centralization_out_degree float(19),
   max_pagerank float(19),
   max_degree_centrality float(19),
   mean_degree_centrality float(19),
   max_in_degree_centrality float(19),
   mean_in_degree_centrality float(19),
   max_out_degree_centrality float(19),
   mean_out_degree_centrality float(19),
   local_clustering float(19),
   global_clustering float(19),
   max_degree_vertex text,
   max_in_degree_vertex text,
   max_out_degree_vertex text,
   max_pagerank_vertex text,
   max_eigenvector_vertex text,
   pseudo_diameter_src_vertex text,
   pseudo_diameter_trg_vertex text,
   max_degree_vertex_uri text,
   max_in_degree_vertex_uri text,
   max_out_degree_vertex_uri text,
   max_pagerank_vertex_uri text,
   pseudo_diameter_src_vertex_uri text,
   pseudo_diameter_trg_vertex_uri text,
   path_edgelist text,
   path_graph_gt text );
APPLICATION_N_QUADS = 'application_n_quads'
APPLICATION_N_TRIPLES = 'application_n_triples'
APPLICATION_RDF_XML = 'application_rdf_xml'
APPLICATION_UNKNOWN = 'unknown'
TEXT_TURTLE = 'text_turtle'
TEXT_N3 = 'text_n3'

SHORT_FORMAT_MAP = {
    'nquads': APPLICATION_N_QUADS,
    'ntriples': APPLICATION_N_TRIPLES,
    'rdfxml': APPLICATION_RDF_XML,
    'turtle': TEXT_TURTLE,
    'n3': TEXT_N3
}

MEDIATYPES = { 
    APPLICATION_RDF_XML: { 
        'cmd_to_ntriples': './bin/to_ntriples.sh rdfxml %s %s %s', 
        'cmd_merge_edgelists': './bin/merge_edgelists.sh %s %s',
        'extension': '.rdf' 
    },
    APPLICATION_N_QUADS: {
        'cmd_to_ntriples': './bin/to_ntriples.sh nquads %s %s %s',
        'cmd_merge_edgelists': './bin/merge_edgelists.sh %s %s',
        'extension': '.nq'
    },
    APPLICATION_N_TRIPLES: {
        'cmd_to_ntriples': './bin/to_ntriples.sh ntriples %s %s %s',
        'cmd_merge_edgelists': './bin/merge_edgelists.sh %s %s',
        'extension': '.nt'
    },
    TEXT_TURTLE: {
        'cmd_to_ntriples': './bin/to_ntriples.sh turtle %s %s %s', 
        'cmd_merge_edgelists': './bin/merge_edgelists.sh %s %s',
        'extension': '.ttl'
    },
    TEXT_N3: {
        'cmd_to_ntriples': './bin/to_ntriples.sh turtle %s %s %s', 
        'cmd_merge_edgelists': './bin/merge_edgelists.sh %s %s',
        'extension': '.n3'
    }
}
MEDIATYPES_COMPRESSED = [ 'tar.gz', 'tar.xz', 'tgz', 'gz', 'zip', 'bz2', 'tar' ]    # do not add 'xy.z' types at the end, they have privilege

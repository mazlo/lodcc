APPLICATION_N_QUADS = 'application_n_quads'
APPLICATION_N_TRIPLES = 'application_n_triples'
APPLICATION_RDF_XML = 'application_rdf_xml'
APPLICATION_UNKNOWN = 'unknown'
TEXT_TURTLE = 'text_turtle'
TEXT_N3 = 'text_n3'

MEDIATYPES = { 
    APPLICATION_RDF_XML: { 
        'cmd_to_ntriples': './to_ntriples.sh rdfxml %s %s', 
        'cmd_to_csv': './to_csv.sh %s %s', 
        'cmd_to_one-liner': './to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.rdf' 
    },
    APPLICATION_N_QUADS: {
        'cmd_to_ntriples': './to_ntriples.sh nquads %s %s',
        'cmd_to_csv': './to_csv.sh %s %s', 
        'cmd_to_one-liner': './to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.nq'
    },
    APPLICATION_N_TRIPLES: {
        'cmd_to_ntriples': None,    # does not need to be transformed 
        'cmd_to_csv': './to_csv.sh %s %s', 
        'cmd_to_one-liner': './to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.nt'
    },
    TEXT_TURTLE: {
        'cmd_to_ntriples': './to_ntriples.sh turtle %s %s', 
        'cmd_to_csv': './to_csv.sh %s %s', 
        'cmd_to_one-liner': './to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.ttl'
    },
    TEXT_N3: {
        'cmd_to_ntriples': './to_ntriples.sh turtle %s %s', 
        'cmd_to_csv': './to_csv.sh %s %s', 
        'cmd_to_one-liner': './to_one-liner.sh %s %s %s', # e.g. /to_one-liner.sh dumps/foo-dataset bar.nt.tgz .tgz
        'extension': '.n3'
    }
}
MEDIATYPES_COMPRESSED = [ 'tar.gz', 'tar.xz', 'tgz', 'gz', 'zip', 'bz2', 'tar' ]    # do not add 'xy.z' types at the end, they have privilege

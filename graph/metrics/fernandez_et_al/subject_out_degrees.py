import numpy as np

# SUBJECT OUT-DEGREES

def partial_out_degree( D, stats ):
    """"""

    # the number of triples of G, in which s occurs as subject and p as predicate
    ## e.g. l = ['/John_foaf:mbox', '/John_foaf:mbox', '/John_rdf:type', '/John_ex:birthPlace', '/Rome_foaf:name', '/Giacomo_ex:areaOfWork', '/Piero_ex:areaOfWork']
    l = [ (D.vp.name[s],D.ep.c0[p]) for s in D.vertices() if D.vp.subject[s] for p in s.out_edges() ]
    _, l = np.unique( l, return_counts=True, axis=0 )
    print( "(4) partial out-degree. max: %s, mean: %s" % ( np.max( l ), np.mean( l ) ) )

def labelled_out_degree( D, stats ):
    """"""

    # the number of different predicates (labels) of G with which s is related as a subject
    ## e.g. l = [ ['foaf:mbox', 'foaf:mbox', 'rdf:type', 'ex:birthPlace'], ['foaf:name'], ['ex:areaOfWork'], ['ex:areaOfWork'] ]
    l = np.array( [ len( { D.ep.c0[p] for p in s.out_edges() } ) 
                   for s in D.vertices() 
                   if D.vp.subject[s] ] )
    print( "(5) labelled out-degree. max: %s, mean: %s" % ( l.max(), l.mean() ) )

def direct_out_degree( D, stats ):
    """"""

    # the number of different objects of G with which s is related as a subject
    ## e.g. l = [ ['john@example.org', 'john@doe.org', '/Researcher', '/Rome'], ['"Roma"@it'], ['/Rome'], ['/Rome'] ]
    l = np.array( [ len( { D.vp.name[p.target()] for p in s.out_edges() } ) 
                   for s in D.vertices() 
                   if D.vp.subject[s] ] )
    print( "(6) direct out-degree. max: %s, mean: %s" % ( l.max(), l.mean() ) )

all = [ partial_out_degree, labelled_out_degree, direct_out_degree ]
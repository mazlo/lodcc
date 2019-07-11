from graph_tool import Graph

def basic_graph():
    """"""

    G = Graph()
        
    v0 = G.add_vertex()
    v1 = G.add_vertex()
    v2 = G.add_vertex()
    v3 = G.add_vertex()
    v4 = G.add_vertex()
    v5 = G.add_vertex()
    v6 = G.add_vertex()
    v7 = G.add_vertex()

    e0 = G.add_edge( v0, v1 )
    e1 = G.add_edge( v0, v2 )
    e2 = G.add_edge( v0, v3 )
    e3 = G.add_edge( v0, v4 )
    e4 = G.add_edge( v5, v4 )
    e5 = G.add_edge( v6, v4 )
    e6 = G.add_edge( v4, v7 )

    prop_v = G.new_vertex_property( 'string' )
    prop_e = G.new_edge_property( 'string' )

    G.vertex_properties['name'] = prop_v
    G.edge_properties['c0'] = prop_e

    prop_v[v0] = '/John'
    prop_v[v1] = 'john@example.org'
    prop_v[v2] = 'john@doe.org'
    prop_v[v3] = '/Researcher'
    prop_v[v4] = '/Rome'
    prop_v[v5] = '/Giacomo'
    prop_v[v6] = '/Piero'
    prop_v[v7] = '"Roma"@it'

    prop_e[e0] = 'foaf:mbox'
    prop_e[e1] = 'foaf:mbox'
    prop_e[e2] = 'rdf:type'
    prop_e[e3] = 'ex:birthPlace'
    prop_e[e4] = 'ex:areaOfWork'
    prop_e[e5] = 'ex:areaOfWork'
    prop_e[e6] = 'foaf:name'

    return G
from graph_tool import Graph

def query_graph_1():
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    
    G.add_edge(v0,v1) # e0
    G.add_edge(v0,v2) # e1
    G.add_edge(v2,v3) # e2
    
    return G

def query_graph_2():
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    
    G.add_edge(v2,v3) # e0, wsdbm:likes
    G.add_edge(v2,v1) # e1, sorg:nationality
    G.add_edge(v0,v1) # e2, gn:parentCountry, switched directions
    
    return G

def query_graph_3():
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, wsdbm:likes
    G.add_edge(v0,v2) # e1, wsdbm:subscribes
    
    return G

def query_graph_4():
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, og:tag
    G.add_edge(v0,v2) # e1, sorg:caption
    
    return G

def query_graph_5():
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, sorg:jobTitle
    G.add_edge(v0,v3) # e1, sorg:nationality
    G.add_edge(v2,v3) # e2, gn:parentCountry
    
    return G

def query_graph_6():
    """query s1"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    v7=G.add_vertex()
    v8=G.add_vertex()
    v9=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, gr:includes
    G.add_edge(v2,v0) # e1, gr:offers
    G.add_edge(v0,v3) # e2, gr:price
    G.add_edge(v0,v4) # e3, gr:serial_number
    G.add_edge(v0,v5) # e4, gr:validFrom
    G.add_edge(v0,v6) # e5, gr:validThrough
    G.add_edge(v0,v7) # e6, sorg:eligible_Region
    G.add_edge(v0,v8) # e7, sorg:eligible_Region
    G.add_edge(v0,v9) # e8, gr:priceValidUntil
    
    return G

def query_graph_7():
    """query s2"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    v4=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, dc:Location
    G.add_edge(v0,v2) # e1, sorg:nationality
    G.add_edge(v0,v3) # e2, wsdbm:gender
    G.add_edge(v0,v4) # e3, rdf:type
    
    return G

def query_graph_8():
    """query s3"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    v4=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, rdf:type
    G.add_edge(v0,v2) # e1, sorg:caption
    G.add_edge(v0,v3) # e2, wsdbm:hasGenre
    G.add_edge(v0,v4) # e3, sorg:publisher
    
    return G

def query_graph_9():
    """query s4"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    v4=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, foaf:age
    G.add_edge(v0,v2) # e1, foaf:familyName
    G.add_edge(v3,v0) # e2, mo:artist
    G.add_edge(v0,v4) # e3, sorg:nationality
    
    return G

def query_graph_10():
    """query s5"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    v4=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, dc:Location
    G.add_edge(v0,v2) # e1, sorg:nationality
    G.add_edge(v0,v3) # e2, wsdbm:gender
    G.add_edge(v0,v4) # e3, rdf:type
    
    return G

def query_graph_11():
    """query s6"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    
    G.add_edge(v0,v1) # e0, mo:conductor
    G.add_edge(v0,v2) # e1, rdf:type
    G.add_edge(v0,v3) # e2, wsdbm:hasGenre
    
    return G

def query_graph_12():
    """query s7"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()    
    
    G.add_edge(v0,v1) # e0, rdf:type
    G.add_edge(v0,v2) # e1, sorg:text
    G.add_edge(v3,v0) # e2, wsdbm:likes
    
    return G

def query_graph_13():
    """query f1"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, og:tag
    G.add_edge(v0,v2) # e1, rdf:type
    G.add_edge(v3,v0) # e2, wsdbm:hasGenre
    G.add_edge(v3,v4) # e3, sorg:trailer
    G.add_edge(v3,v5) # e4, rdf:type
    G.add_edge(v3,v6) # e5, sorg:keywords
    
    return G

def query_graph_14():
    """query f2"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    v7=G.add_vertex()
    v8=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, foaf:homepage
    G.add_edge(v0,v2) # e1, og:title
    G.add_edge(v0,v3) # e2, rdf:type
    G.add_edge(v0,v4) # e3, sorg:caption
    G.add_edge(v0,v5) # e4, sorg:description
    G.add_edge(v1,v6) # e5, sorg:url
    G.add_edge(v1,v7) # e6, wsdbm:hits
    G.add_edge(v0,v8) # e7, wsdbm:hasGenre
    
    return G

def query_graph_15():
    """query f3"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, sorg:contentRating
    G.add_edge(v0,v2) # e1, sorg:contentSize
    G.add_edge(v0,v3) # e2, wsdbm:HasGenre
    G.add_edge(v5,v0) # e3, wsdbm:purchaseFor
    G.add_edge(v4,v5) # e4, wsdbm:makesPurchase
    G.add_edge(v5,v6) # e5, wsdbm:purchaseDate
    
    return G

def query_graph_16():
    """query f4"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    v7=G.add_vertex()
    v8=G.add_vertex()
    v9=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, foaf:homepage
    G.add_edge(v2,v0) # e1, gr:includes
    G.add_edge(v0,v3) # e2, og:title
    G.add_edge(v0,v4) # e3, sorg:description
    G.add_edge(v0,v8) # e4, sorg:contentSize
    G.add_edge(v1,v5) # e5, sorg:url
    G.add_edge(v1,v6) # e6, wsdbm:hits
    G.add_edge(v7,v1) # e7, wsdbm:likes
    G.add_edge(v1,v9) # e8, sorg:language
    
    return G
    
def query_graph_17():
    """query f5"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, gr:includes
    G.add_edge(v2,v0) # e1, gr:offers
    G.add_edge(v0,v3) # e2, gr:price
    G.add_edge(v0,v4) # e3, gr:validThrough
    G.add_edge(v1,v5) # e4, og:title
    G.add_edge(v1,v6) # e5, rdf:type
    
    return G

def query_graph_18():
    """query c1"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    v7=G.add_vertex()
    v8=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, sorg:caption
    G.add_edge(v0,v2) # e1, sorg:text
    G.add_edge(v0,v3) # e2, sorg:contentRating
    G.add_edge(v0,v4) # e3, rev:hasReview
    G.add_edge(v4,v5) # e4, rev:title
    G.add_edge(v4,v6) # e5, rev:reviewer
    G.add_edge(v7,v6) # e6, sorg:actor
    G.add_edge(v7,v8) # e7, sorg:language
    
    return G

def query_graph_19():
    """query c2"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    v7=G.add_vertex()
    v8=G.add_vertex()
    v9=G.add_vertex()
    v10=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, sorg:legalName
    G.add_edge(v0,v2) # e1, gr:offers
    G.add_edge(v2,v3) # e2, gr:includes
    G.add_edge(v2,v5) # e3, sorg:eligibleRegion
    G.add_edge(v3,v8) # e4, sorg:actor
    G.add_edge(v8,v9) # e5, rev:totalVotes
    G.add_edge(v7,v3) # e6, wsdbm:purchaseFor
    G.add_edge(v4,v7) # e7, wsdbm:makesPurchase
    G.add_edge(v4,v10) # e8, sorg:jobTitle
    G.add_edge(v4,v6) # e9, foaf:homepage
    
    return G

def query_graph_20():
    """query c3"""
    G=Graph( directed=True )
    
    v0=G.add_vertex()
    v1=G.add_vertex()
    v2=G.add_vertex()
    v3=G.add_vertex()
    v4=G.add_vertex()
    v5=G.add_vertex()
    v6=G.add_vertex()
    
    G.add_edge(v0,v1) # e0, wsdbm:likes
    G.add_edge(v0,v2) # e1, wsdbm:friendOf
    G.add_edge(v0,v3) # e2, dc:Location
    G.add_edge(v0,v4) # e3, foaf:age
    G.add_edge(v0,v5) # e4, wsdbm:gender
    G.add_edge(v0,v6) # e5, foaf:givenName
    
    return G
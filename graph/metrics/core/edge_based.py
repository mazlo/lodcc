
def f_reciprocity( D, stats ):
    """"""

    if 'reciprocity' in args['features']:
        stats['reciprocity']=edge_reciprocity(D)
        log.debug( 'done reciprocity' )

def f_pseudo_diameter( D, stats ):
    """"""

    LC = label_largest_component(D)
    LCD = GraphView( D, vfilt=LC )

    if 'diameter' in args['features']:
        if LCD.num_vertices() == 0 or LCD.num_vertices() == 1:
            # if largest component does practically not exist, use the whole graph
            dist, ends = pseudo_diameter(D)
        else:
            dist, ends = pseudo_diameter(LCD)

        stats['pseudo_diameter']=dist
        # D may be used in both cases
        stats['pseudo_diameter_src_vertex']=D.vertex_properties['name'][ends[0]]
        stats['pseudo_diameter_trg_vertex']=D.vertex_properties['name'][ends[1]]
        log.debug( 'done pseudo_diameter' )

import numpy as np

def gini( array ):
    """Calculate the Gini coefficient of a numpy array."""
    # based on bottom eq: http://www.statsdirect.com/help/content/image/stat0206_wmf.gif
    # from: http://www.statsdirect.com/help/default.htm#nonparametric_methods/gini.htm
    array = array.flatten() #all values are treated equally, arrays must be 1d
    
    if np.amin( array ) < 0:
        #values cannot be negative
        array -= np.amin( array )
        
    if np.amin( array ) == 0:
        #values cannot be 0
        array = array.astype( float )
        array += 0.0000001
        
    array = np.sort( array )  #values must be sorted
    
    index = np.arange( 1, array.shape[0]+1 )   #index per array element
    n = array.shape[0]  #number of array elements
    
    return ( float( np.sum((2 * index - n  - 1) * array) ) / ( n * np.sum(array) ) ) # Gini coefficient

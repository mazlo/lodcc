import argparse
import logging as log
import os
import re


if __name__ == '__main__':

    parser = argparse.ArgumentParser( description = 'lodcc - merge edgelists in directory' )
    parser.add_argument( '--paths', '-p', nargs='*', required = True, help = '' )
    parser.add_argument( '--rm-edgelists', '-re', action = "store_true", help = 'If given, the programm will remove single edgelist files after they have been appended to data.edgelist.csv' )

    log.basicConfig(
            level = log.INFO, 
            format = '[%(asctime)s] - %(levelname)-8s : %(message)s', )

    args = vars( parser.parse_args() )
    paths = args['paths']

    merge_edgelists( paths, args['rm_edgelists'], log )

    log.info( 'done' )

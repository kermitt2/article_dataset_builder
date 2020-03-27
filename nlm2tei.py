import argparse
import os
import shutil
import subprocess
import S3

class Nlm2tei(object):
    """
    Convert existing NLM/JATS files (PMC) in a data repository into TEI XML format similar as Grobid output.
    This is using Pub2TEI (https://github.com/kermitt2/Pub2TEI) and it is done in batch to have good runtime. 
    """
    

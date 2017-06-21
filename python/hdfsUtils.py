import os
import sys
import subprocess
import argparse


def strip_hdfs(directory):
    return '/'.join([x for x in directory.split('/') if x not in ['hdfs']])

def hdfs_ls_directory(storeDir):
    '''Utility for ls'ing /hdfs at UW'''
    storeDir = strip_hdfs(storeDir)
    command = 'gfal-ls srm://cmssrm2.hep.wisc.edu:8443/srm/v2/server?SFN=/hdfs/{0}'.format(storeDir)
    out = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).communicate()[0]
    if 'gfal-ls' in out:
        log.error(out)
        return []
    return out.split()

def get_hdfs_root_files(topDir,lastDir=''):
    '''Utility for getting all root files in a directory (and subdirectories)'''
    lsDir = strip_hdfs('{0}/{1}'.format(topDir,lastDir)) if lastDir else strip_hdfs(topDir)
    nextLevel = hdfs_ls_directory(lsDir)
    out = []
    for nl in nextLevel:
        if nl=='failed': # dont include
            continue
        elif nl[-4:]=='root': # its a root file
            out += ['{0}/{1}'.format(lsDir,nl)]
        else: # keep going down
            out += get_hdfs_root_files(lsDir,nl)
    return out

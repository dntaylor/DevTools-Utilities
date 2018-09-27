#!/usr/bin/env python

import os
import sys
import glob
import argparse
import logging

import ROOT

def getVar(rtrow,lep,var):
    return getattr(rtrow,'{0}_{1}'.format(lep,var))

def print_detailed_dy(rtrow):
    print '{0}:{1}:{2}'.format(rtrow.run, rtrow.lumi, rtrow.event)
    print '    channel: {0}'.format(rtrow.channel)
    leps = ['z1','z2']
    for lep in leps:
         print '    {0}: pt {1}, eta {2}, phi {3}, pass: {4}'.format(lep, getVar(rtrow,lep,'pt'), getVar(rtrow,lep,'eta'), getVar(rtrow,lep,'phi'), getVar(rtrow,lep,'passMedium'))
    print '    z_mass: {0}, z_pt: {1}'.format(rtrow.z_mass,rtrow.z_pt)

def print_detailed_wz(rtrow):
    print '{0}:{1}:{2}'.format(rtrow.run, rtrow.lumi, rtrow.event)
    leps = ['z1','z2','w1']
    for lep in leps:
         print '    {0}: pt {1}, eta {2}, phi {3}, pass: {4} passTight: {5}'.format(lep, getVar(rtrow,lep,'pt'), getVar(rtrow,lep,'eta'), getVar(rtrow,lep,'phi'), getVar(rtrow,lep,'passMedium'), getVar(rtrow,lep,'passTight'))
    print '    z_mass: {0}, met {1}, bveto {2}, 3l_mass {3}, wmll1 {4}, wmll2 {5}'.format(rtrow.z_mass, rtrow.met_pt, rtrow.numBjetsTight30, getattr(rtrow,'3l_mass'), rtrow.w1_z1_mass, rtrow.w1_z2_mass)

def print_detailed_hpp3l(rtrow):
    print '{0}:{1}:{2}'.format(rtrow.run, rtrow.lumi, rtrow.event)
    print rtrow.channel, rtrow.genChannel
    print rtrow.hpp1_pt, rtrow.hpp2_pt, rtrow.hm1_pt
    print rtrow.hpp_mass, rtrow.hm_mt

def print_detailed_mmtt(row,header=False):
    varsToPrint = [
        'am1_pt', 'am1_eta', 'am1_phi', 'am1_isolation',
        'am2_pt', 'am2_eta', 'am2_phi', 'am2_isolation',
        'atm_pt', 'atm_eta', 'atm_phi', 'atm_isolation',
        'ath_pt', 'ath_eta', 'ath_phi', 'ath_byIsolationMVArun2v1DBoldDMwLTraw', 'ath_decayMode',
        'amm_mass', 'amm_pt', 'amm_eta', 'amm_phi', 'amm_deltaR',
        'att_mass', 'att_pt', 'att_eta', 'att_phi', 'att_deltaR',
        'h_mass', 'h_pt', 'h_eta', 'h_phi', 'h_massKinFit',
    ]
    if header:
        print 'run:lumi:event:'+':'.join(varsToPrint)
    else:
        print ':'.join([str(row.run), str(row.lumi), str(row.event)]+['{:.3f}'.format(getattr(row,v)) for v in varsToPrint])

def print_detailed_mini(rtrow):
    print '{0}:{1}:{2}'.format(rtrow.run, rtrow.lumi, rtrow.event)
    for obj in ['electrons','muons','taus','jets']:
        nobj = getattr(rtrow,obj+'_count')
        print '    {0}: {1}'.format(obj,nobj)
        for o in range(nobj):
            print '        {0}: pt {1} eta {2} phi {3} charge {4}'.format(
                 o,
                 getattr(rtrow,obj+'_pt')[o],
                 getattr(rtrow,obj+'_eta')[o],
                 getattr(rtrow,obj+'_phi')[o],
                 getattr(rtrow,obj+'_charge')[o],
            )
            if obj=='muons':
                print '         : isMediumMuonICHEP {0} relPFIsoDeltaBetaR04 {1} relTrackIso {2} dz {3} dxy {4}'.format(
                    getattr(rtrow,obj+'_isMediumMuonICHEP')[o],
                    getattr(rtrow,obj+'_relPFIsoDeltaBetaR04')[o],
                    getattr(rtrow,obj+'_trackIso')[o]/getattr(rtrow,obj+'_pt')[o],
                    getattr(rtrow,obj+'_dz')[o],
                    getattr(rtrow,obj+'_dB2D')[o],
                )
            if obj=='electrons':
                print '         : wwLoose {0} cutBasedMedium {1}'.format(
                    getattr(rtrow,obj+'_wwLoose')[o],
                    getattr(rtrow,obj+'_cutBasedMedium')[o],
                )

def parse_command_line(argv):
    parser = argparse.ArgumentParser(description="Print events from ntuple")

    parser.add_argument('files', nargs='+', help='File names w/ UNIX wildcards')
    parser.add_argument('-t','--tree',type=str,default='MiniTree',help='Tree name')
    parser.add_argument('-c','--cut',nargs='?',type=str,default='',help='Cut to be applied to tree')
    parser.add_argument('-e','--events',nargs='*',type=str,default=[],help='Events to print (form: run:lumi:event, space delimited)')
    parser.add_argument('-ef','--eventsFile',nargs='?',type=str,default='',help='Events file to print (form: run:lumi:event, one per line)')
    parser.add_argument('-d','--detailed',action='store_true',help='Print detailed event information')
    parser.add_argument('--log',nargs='?',type=str,const='INFO',default='INFO',choices=['INFO','DEBUG','WARNING','ERROR','CRITICAL'],help='Log level for logger')
    args = parser.parse_args(argv)

    return args



def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    loglevel = getattr(logging,args.log)
    logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s', level=loglevel, datefmt='%Y-%m-%d %H:%M:%S', stream=sys.stderr)

    files = [filename for string in args.files for filename in glob.glob(string)]

    tchain = ROOT.TChain(args.tree)
    for f in files:
        tchain.Add(f)

    selectedEvents = tchain.CopyTree(args.cut) if args.cut else tchain

    if args.eventsFile:
       with open(args.eventsFile,'r') as f:
           for line in f.readlines():
               args.events += [line.strip()]

    rtrow = selectedEvents
    if args.detailed:
        if args.tree=='MuMuTauTauTree': print_detailed_mmtt(rtrow,header=True)
    for r in xrange(rtrow.GetEntries()):
        rtrow.GetEntry(r)
        eventkey = '{0}:{1}:{2}'.format(rtrow.run, rtrow.lumi, rtrow.event)
        if args.events and eventkey not in args.events: continue
        if args.detailed:
            if args.tree=='miniTree/MiniTree': print_detailed_mini(rtrow)
            if args.tree=='DYTree': print_detailed_dy(rtrow)
            if args.tree=='WZTree': print_detailed_wz(rtrow)
            if args.tree=='Hpp3lTree': print_detailed_hpp3l(rtrow)
            if args.tree=='MuMuTauTauTree': print_detailed_mmtt(rtrow)
        else:
            print eventkey

if __name__ == "__main__":
    main()

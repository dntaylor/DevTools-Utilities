#!/usr/bin/env python

'''
submit_job.py

Script to submit jobs to crab or condor.
'''

import argparse
import logging
import os
import math
import sys
import glob
import subprocess
import fnmatch
import json
from socket import gethostname

log = logging.getLogger("submit_job")
logging.basicConfig(level=logging.INFO, stream=sys.stderr)

try:
    from CRABClient.ClientExceptions import ClientException
    from CRABClient.ClientUtilities import initLoggers
    from httplib import HTTPException
    import CRABClient.Commands.submit as crabClientSubmit
    import CRABClient.Commands.status as crabClientStatus
    import CRABClient.Commands.resubmit as crabClientResubmit
    crabLoaded = True
except:
    crabLoaded = False

from DevTools.Utilities.utilities import getJson
from DevTools.Utilities.hdfsUtils import strip_hdfs, hdfs_ls_directory, get_hdfs_root_files, get_hdfs_directory_size
    
UNAME = os.environ['USER']

def get_scratch_area():
    '''Return a scratch area'''
    if 'uwlogin' in gethostname():
        scratchDir = '/data/{0}'.format(UNAME)
    elif 'lpc' in gethostname():
        scratchDir = os.path.expanduser('~/nobackup')
    else:
        scratchDir = '/nfs_scratch/{0}'.format(UNAME) # default, wisconsin
    return scratchDir

#######################
### Crab submission ###
#######################

def get_crab_workArea(args):
    '''Get the job working area'''
    scratchDir = get_scratch_area()
    return '{0}/crab_projects/{1}'.format(scratchDir,args.jobName)

def get_config(args):
    '''Get a crab config file based on the arguments of crabSubmit'''
    from CRABClient.UserUtilities import config

    config = config()

    config.General.workArea         = get_crab_workArea(args)
    config.General.transferOutputs  = True

    config.JobType.pluginName       = 'Analysis'
    if args.scriptExe:
        config.JobType.psetName     = '{0}/src/DevTools/Utilities/test/PSet.py'.format(os.environ['CMSSW_BASE'])
        config.JobType.scriptExe    = args.cfg
        config.JobType.scriptArgs   = args.cmsRunArgs #+ ['outputFile=crab.root']
        config.JobType.outputFiles  = ['crab.root']
    else:
        config.JobType.psetName     = args.cfg
        config.JobType.pyCfgArgs    = args.cmsRunArgs
    config.JobType.sendPythonFolder = True

    config.Data.inputDBS            = args.inputDBS
    config.Data.splitting           = 'FileBased'
    config.Data.unitsPerJob         = args.filesPerJob
    #config.Data.splitting           = 'LumiBased'
    #config.Data.unitsPerJob         = 10
    #config.Data.splitting           = 'EventAwareLumiBased'
    #config.Data.unitsPerJob         = 100000
    config.Data.outLFNDirBase       = '/store/user/{0}/{1}/'.format(args.user,args.jobName)
    config.Data.publication         = args.publish
    config.Data.outputDatasetTag    = args.jobName
    if args.applyLumiMask:
        config.Data.lumiMask        = getJson(args.applyLumiMask)
        #config.Data.splitting       = 'LumiBased'
        #config.Data.unitsPerJob     = args.lumisPerJob
    if args.allowNonValid:
        config.Data.allowNonValidInputDataset = True

    config.Site.storageSite         = args.site
    if args.scriptExe:
        config.Site.whitelist = ['T2_US_Wisconsin']

    return config

def submit_das_crab(args):
    '''Submit samples using DAS'''
    tblogger, logger, memhandler = initLoggers()
    tblogger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    memhandler.setLevel(logging.INFO)

    # crab config
    config = get_config(args)

    # get samples
    sampleList = []
    if args.samples:
        sampleList += args.samples
    elif os.path.isfile(args.sampleList):
        with open(args.sampleList,'r') as f:
            sampleList = [line.strip() for line in f]
    else:
        log.error('Sample input list {0} does not exist.'.format(args.sampleList))

    submitMap = {}
    # iterate over samples
    for sample in sampleList:
        _, primaryDataset, datasetTag, dataFormat = sample.split('/')
        config.General.requestName = '{0}'.format(primaryDataset)
        maxDatasetTagSize = 97-len(primaryDataset)
        config.General.requestName += '_' + datasetTag[-maxDatasetTagSize:]
        # make it only 100 characters
        config.General.requestName = config.General.requestName[:99] # Warning: may not be unique now
        config.Data.inputDataset   = sample
        # submit the job
        submitArgs = ['--config',config]
        if args.dryrun: submitArgs += ['--dryrun']
        try:
            log.info("Submitting for input dataset {0}".format(sample))
            submitMap[sample] = crabClientSubmit.submit(logger,submitArgs)()
        except HTTPException as hte:
            log.info("Submission for input dataset {0} failed: {1}".format(sample, hte.headers))
        except ClientException as cle:
            log.info("Submission for input dataset {0} failed: {1}".format(sample, cle))

def submit_untracked_crab(args):
    '''Submit jobs from an inputDirectory'''
    tblogger, logger, memhandler = initLoggers()
    tblogger.setLevel(logging.INFO)
    logger.setLevel(logging.INFO)
    memhandler.setLevel(logging.INFO)

    # crab config
    config = get_config(args)
    config.Site.whitelist = [args.site] # whitelist site, run on same site as files located


    # get samples
    sampleList = hdfs_ls_directory(args.inputDirectory)

    submitMap = {}
    # iterate over samples
    for sample in sampleList:
        if hasattr(args,'sampleFilter'):
            submitSample = False
            for sampleFilter in args.sampleFilter:
                if fnmatch.fnmatch(sample,sampleFilter): submitSample = True
            if not submitSample: continue
        primaryDataset = sample
        config.General.requestName = '{0}'.format(primaryDataset)
        # make it only 100 characters
        config.General.requestName = config.General.requestName[:99] # Warning: may not be unique now
        config.Data.outputPrimaryDataset = primaryDataset
        # get file list
        inputFiles = get_hdfs_root_files(args.inputDirectory,sample)
        config.Data.userInputFiles = inputFiles
        totalFiles = len(inputFiles)
        if totalFiles==0:
            logging.warning('{0} {1} has no files.'.format(inputDirectory,sample))
            continue
        filesPerJob = args.filesPerJob
        if args.gigabytesPerJob:
            totalSize = get_hdfs_directory_size(os.path.join(args.inputDirectory,sample))
            if totalSize:
                averageSize = totalSize/totalFiles
                GB = 1024.*1024.*1024.
                filesPerJob = int(math.ceil(args.gigabytesPerJob*GB/averageSize))
        if hasattr(args,'jsonFilesPerJob') and args.jsonFilesPerJob:
            if os.path.isfile(args.jsonFilesPerJob):
                with open(args.jsonFilesPerJob) as f:
                    data = json.load(f)
                if sample in data:
                    filesPerJob = data[sample]
            else:
                logging.error('JSON map {0} for jobs does not exist'.format(args.jsonFilesPerJob))
                return
        config.Data.unitsPerJob = filesPerJob
        # submit the job
        submitArgs = ['--config',config]
        if args.dryrun: submitArgs += ['--dryrun']
        try:
            log.info("Submitting for input dataset {0}".format(sample))
            submitMap[sample] = crabClientSubmit.submit(logger,submitArgs)()
        except HTTPException as hte:
            log.info("Submission for input dataset {0} failed: {1}".format(sample, hte.headers))
        except ClientException as cle:
            log.info("Submission for input dataset {0} failed: {1}".format(sample, cle))


def submit_crab(args):
    '''Create submission script for crab'''
    if not crabLoaded:
        logging.error('You must source a crab environment to submit to crab.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')
        return
    if args.sampleList or args.samples:
        submit_das_crab(args)
    elif args.inputDirectory:
        submit_untracked_crab(args)
    else:
        log.warning('Unrecognized submit configuration: include --inputDirectory, --samples, or --sampleList.')


def status_crab(args):
    '''Check jobs'''
    if not crabLoaded:
        logging.error('You must source a crab environment to submit to crab.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')
        return
    crab_dirs = []
    if args.jobName:
        workArea = get_crab_workArea(args)
        crab_dirs += sorted(glob.glob('{0}/*'.format(workArea)))
    elif args.crabDirectories:
        for d in args.crabDirectories:
            crab_dirs += glob.glob(d)
    else:
        log.error("Shouldn't be possible to get here")

    tblogger, logger, memhandler = initLoggers()
    tblogger.setLevel(logging.WARNING)
    logger.setLevel(logging.WARNING)
    memhandler.setLevel(logging.WARNING)

    statusMap = {}
    for d in crab_dirs:
        if os.path.exists(d):
            statusArgs = ['--dir',d]
            #if args.verbose: statusArgs += ['--long']
            try:
                log.info('Retrieving status of {0}'.format(d))
                statusMap[d] = crabClientStatus.status(logger,statusArgs)()
                if args.verbose: print_single_status(args,statusMap[d])
            except HTTPException as hte:
                log.warning("Status for input directory {0} failed: {1}".format(d, hte.headers))
            except ClientException as cle:
                log.warning("Status for input directory {0} failed: {1}".format(d, cle))

    parse_crab_status(args,statusMap)


allowedStates = ['idle','running','transferring','finished','failed','unsubmitted','cooloff','killing','held']
allowedStatuses = ['COMPLETED','UPLOADED','SUBMITTED','FAILED','QUEUED','SUBMITFAILED','KILLED','KILLFAILED','RESUBMITFAILED','NEW','RESUBMIT','KILL','UNKNOWN']

def print_single_status(args,summary):
    status = summary['status']
    log.info('Status: {0}'.format(status))
    if 'jobs' in summary:
        singleStateSummary = {}
        for state in allowedStates: singleStateSummary[state] = 0
        for j,job in summary['jobs'].iteritems():
            singleStateSummary[job['State']] += 1
        for s in allowedStates:
            if singleStateSummary[s]:
                log.info('        {0:12} : {1}'.format(s,singleStateSummary[s]))

def parse_crab_status(args,statusMap):
    '''Parse the output of a crab status call'''
    statusSummary = {}
    for status in allowedStatuses: statusSummary[status] = []
    singleStateSummary = {}
    stateSummary = {}
    for state in allowedStates: stateSummary[state] = 0
    for d,summary in statusMap.iteritems():
        status = summary['status']
        statusSummary[status] += [d]
        if 'jobs' in summary:
            singleStateSummary[d] = {}
            for state in allowedStates: singleStateSummary[d][state] = 0
            for j,job in summary['jobs'].iteritems():
                singleStateSummary[d][job['State']] += 1
                stateSummary[job['State']] += 1
    log.info('Summary')
    for s in allowedStatuses:
        if statusSummary[s]:
            log.info('Status: {0}'.format(s))
            for d in sorted(statusSummary[s]):
                log.info('    {0}'.format(d))
                if args.verbose:
                    for s in allowedStates:
                        if singleStateSummary[d][s]:
                            log.info('        {0:12} : {1}'.format(s,singleStateSummary[d][s]))
    for s in allowedStates:
        if stateSummary[s]:
            log.info('{0:12} : {1}'.format(s,stateSummary[s]))

def resubmit_crab(args):
    '''Resubmit jobs'''
    if not crabLoaded:
        logging.error('You must source a crab environment to submit to crab.\nsource /cvmfs/cms.cern.ch/crab3/crab.sh')
        return
    crab_dirs = []
    if args.jobName:
        workArea = get_crab_workArea(args)
        crab_dirs += sorted(glob.glob('{0}/*'.format(workArea)))
    elif args.crabDirectories:
        for d in args.crabDirectories:
            crab_dirs += glob.glob(d)
    else:
        log.error("Shouldn't be possible to get here")

    tblogger, logger, memhandler = initLoggers()
    tblogger.setLevel(logging.WARNING)
    logger.setLevel(logging.WARNING)
    memhandler.setLevel(logging.WARNING)

    resubmitMap = {}
    for d in crab_dirs:
        if os.path.exists(d):
            statusArgs = ['--dir',d]
            resubmitArgs = ['--dir',d]
            try:
                summary = crabClientStatus.status(logger,statusArgs)()
                resubmit = False
                total = 0
                failed = 0
                allJobStatus = {}
                if 'jobs' in summary:
                    for j,job in summary['jobs'].iteritems():
                        total += 1
                        if job['State'] not in allJobStatus: allJobStatus[job['State']] = 0
                        allJobStatus[job['State']] += 1
                        if job['State'] in ['failed']:
                            failed += 1
                            resubmit = True
                if resubmit:
                    log.info('Resubmitting {0}'.format(d))
                    log.info('{0} of {1} jobs failed'.format(failed,total))
                    log.info(' '.join(['{0}: {1}'.format(state,allJobStatus[state]) for state in allowedStates if state in allJobStatus]))
                    resubmitMap[d] = crabClientResubmit.resubmit(logger,resubmitArgs)()
            except HTTPException as hte:
                log.warning("Submission for input directory {0} failed: {1}".format(d, hte.headers))
            except ClientException as cle:
                log.warning("Submission for input directory {0} failed: {1}".format(d, cle))

    for d,statMap in resubmitMap.iteritems():
        if statMap['status'] != 'SUCCESS':
            log.info('Status: {0} - {1}'.format(statMap['status'],d))

#########################
### Condor submission ###
#########################

def get_condor_workArea(args):
    '''Get the job working area'''
    scratchDir = get_scratch_area()
    return '{0}/condor_projects/{1}'.format(scratchDir,args.jobName)


def submit_untracked_condor(args):
    '''Submit to condor using an input directory'''
    # get samples
    for inputDirectories in args.inputDirectory:
        for inputDirectory in glob.glob(inputDirectories):
            sampleList = hdfs_ls_directory(inputDirectory)

            workArea = get_condor_workArea(args)
            os.system('mkdir -p {0}'.format(workArea))

            submitMap = {}
            # iterate over samples
            for sample in sampleList:
                if hasattr(args,'sampleFilter'):
                    submitSample = False
                    for sampleFilter in args.sampleFilter:
                        if fnmatch.fnmatch(sample,sampleFilter): submitSample = True
                    if not submitSample: continue
                # farmout config
                command = 'farmoutAnalysisJobs --infer-cmssw-path --input-basenames-not-unique'
                if hasattr(args,'scriptExe') and args.scriptExe:
                    command += ' --fwklite'
                # submit dir
                submitDir = '{0}/{1}'.format(workArea, sample)
                command += ' --submit-dir={0}'.format(submitDir)
                if os.path.exists(submitDir) and not args.resubmit:
                    logging.warning('Submit directory exists {0}'.format(submitDir))
                    continue
                # input files
                inputFiles = get_hdfs_root_files(inputDirectory,sample)
                totalFiles = len(inputFiles)
                if totalFiles==0:
                    logging.warning('{0} {1} has no files.'.format(inputDirectory,sample))
                    continue
                fileList = '{0}_inputs.txt'.format(submitDir)
                with open(fileList,'w') as f:
                    if args.jobsPerFile>1:
                        jobStrings = []
                        for job in range(args.jobsPerFile):
                            for inputFile in inputFiles:
                                jobStrings += ['{0}/{1}/{2}'.format(inputFile,args.jobsPerFile,job)]
                        f.write('\n'.join(jobStrings))
                    else:
                        f.write('\n'.join(inputFiles))
                filesPerJob = args.filesPerJob
                if args.jobsPerFile>1: filesPerJob = len(inputFiles)
                if args.gigabytesPerJob:
                    totalSize = hdfs_directory_size(os.path.join(inputDirectory,sample))
                    averageSize = totalSize/totalFiles
                    GB = 1024.*1024.*1024.
                    filesPerJob = int(math.ceil(args.gigabytesPerJob*GB/averageSize))
                if hasattr(args,'jsonFilesPerJob') and args.jsonFilesPerJob:
                    if os.path.isfile(args.jsonFilesPerJob):
                        with open(args.jsonFilesPerJob) as f:
                            data = json.load(f)
                        if sample in data:
                            filesPerJob = data[sample]
                    else:
                        logging.error('JSON map {0} for jobs does not exist'.format(args.jsonFilesPerJob))
                        return
                command += ' --input-file-list={0} --assume-input-files-exist --input-files-per-job={1}'.format(fileList,filesPerJob)
                if args.vsize:
                    command += ' --vsize-limit={0}'.format(args.vsize)
                if args.useAFS:
                    command += ' --shared-fs'
                # output directory
                outputDir = 'srm://cmssrm2.hep.wisc.edu:8443/srm/v2/server?SFN=/hdfs/store/user/{0}/{1}/{2}'.format(args.user,args.jobName,sample)
                command += ' --output-dir={0}'.format(outputDir)
                if args.useHDFS: command += ' --use-hdfs'
                if args.resubmit: command += ' --resubmit-failed-jobs'
                if hasattr(args,'cfg'):
                    command += ' {0} {1} {2}'.format(args.jobName, args.cfg, ' '.join(args.cmsRunArgs))
                else: # its a merge
                    command += ' --merge {0}'.format(args.jobName)
                if args.dryrun:
                    logging.info(command)
                else:
                    os.system(command)
        

def submit_condor(args):
    '''Create submission script for condor'''
    if args.inputDirectory:
        submit_untracked_condor(args)
    else:
        log.warning('Unrecognized submit configuration: include --inputDirectory.')

def status_condor(args):
    '''Check jobs on condor'''
    condor_dirs = []
    if args.jobName:
        workArea = get_condor_workArea(args)
        condor_dirs += sorted(glob.glob('{0}/*'.format(workArea)))
    elif args.condorDirectories:
        for d in args.condorDirectories:
            condor_dirs += glob.glob(d)
    else:
        log.error("Shouldn't be possible to get here")

    allowedStatuses = ['SUBMITTED','RUNNING','ERROR','EVICTED','ABORTED','SUSPENDED','HELD','FINISHED','UNKNOWN','FAILED']

    logstatuses = { # TODO: lookup possible states
        0 : 'SUBMITTED',
        1 : 'RUNNING',
        2 : 'ERROR',
        4 : 'EVICTED',
        5 : 'FINISHED',
        9 : 'ABORTED',
        10: 'SUSPENDED',
        11: 'RUNNING', #'UNSUSPENDED',
        12: 'HELD',
        13: 'RUNNING', #'RELEASED',
    }
    results = {}
    for d in sorted(condor_dirs):
        if os.path.isdir(d):
            results[d] = {}
            # get list of jobs
            jobDirs = [j for j in glob.glob('{0}/*'.format(d)) if os.path.isdir(j)]
            for j in jobDirs:
                results[d][j] = {}
                # completed jobs have a report.log in the submission directory
                if os.path.exists(os.path.join(j,'report.log')):
                    # parse report.log
                    with open(os.path.join(j,'report.log')) as f:
                        try:
                            statusString = f.readlines()[-1].strip().replace('params : ','').replace("'",'"')
                            status = json.loads(statusString)
                            if 'JobExitCode' in status:
                                exitCode = int(status['JobExitCode'])
                                if exitCode:
                                    results[d][j]['status'] = 'FAILED'
                                else:
                                    results[d][j]['status'] = 'FINISHED'
                            else:
                                results[d][j]['status'] = 'RUNNING'
                        except:
                            logging.error('Failed to parse {0}'.format(j))
                            results[d][j]['status'] = 'UNKNOWN'
                else:
                    # load log file
                    logfile = '{0}/{1}.log'.format(j,os.path.basename(j))
                    if os.path.exists(logfile):
                        laststatus = ''
                        with open(logfile,'r') as f:
                            for line in f.readlines():
                                if 'TriggerEventTypeNumber' in line:
                                    code = int(line.split()[-1])
                                    if code in logstatuses: 
                                        laststatus = logstatuses[code]
                        results[d][j]['status'] = laststatus
                    else:
                        results[d][j]['status'] = 'UNKNOWN'

    # print out the summary
    total = {}
    for s in allowedStatuses: total[s] = 0
    for d in sorted(results):
        if args.verbose: log.info(d)
        for s in allowedStatuses:
            jobs = [key for key,val in results[d].iteritems() if val['status']==s]
            total[s] += len(jobs)
            if len(jobs) and args.verbose:
                log.info('    {0:20}: {1}'.format(s,len(jobs)))
                if s=='FAILED':
                    for j in jobs:
                        log.info('      {0}'.format(j))
    for s in allowedStatuses:
        if total[s]: log.info('{0:20}: {1}'.format(s,total[s]))

############################
### Command line options ###
############################

def add_common_submit(parser):
    parser.add_argument('jobName', type=str, help='Job Name for submission')
    parser.add_argument('cfg', type=str, help='cmsRun config file or user script')
    parser.add_argument('cmsRunArgs', nargs='*', help='Arguments passed to cmsRun/script')
    parser.add_argument('--scriptExe', action='store_true', help='This is a script, not a cmsRun config')

def add_common_inputs(parser):
    # job inputs
    parser_inputs = parser.add_mutually_exclusive_group(required=True)
    parser_inputs.add_argument('--samples', type=str, nargs='*',
        help='Space delimited list of DAS samples to submit'
    )
    parser_inputs.add_argument('--sampleList', type=str,
        help='Text file list of DAS samples to submit, one per line'
    )
    parser_inputs.add_argument('--inputDirectory', type=str,
        help='Top level directory to submit. Each subdirectory will create one crab job.'
    )
    parser.add_argument('--sampleFilter', type=str, nargs='*', default=['*'],
        help='Only submit selected samples, unix wild cards allowed'
    )

    parser.add_argument('--applyLumiMask',type=str, default=None,
        choices=['Collisions15','ICHEP2016','Collisions16','Collisions17'],
        help='Apply the latest golden json run lumimask to data'
    )

    parser.add_argument('--inputDBS', type=str, default='global',
        choices=['global','phys01','phys02','phys03'], 
        help='DAS instance to search for input files'
    )

    parser.add_argument('--allowNonValid', action='store_true', help='Allow non valid datasets from DAS')


def add_common_splitting(parser):
    # job splitting
    parser_jobs = parser.add_mutually_exclusive_group()
    parser_jobs.add_argument('--filesPerJob', type=int, default=1,
        help='Number of files per job'
    )

    parser_jobs.add_argument('--lumisPerJob', type=int, default=30,
        help='Number of lumis per job'
    )

    parser_jobs.add_argument('--gigabytesPerJob', type=float, default=0,
        help='Average jobs to process a given number of gigabytes'
    )

    #parser_jobs.add_argument('--jobsPerFile', type=int, default=1,
    #    help='Number of jobs per file. File list will be of the form "fname/njobs/job"'
    #)

    parser_jobs.add_argument('--jsonFilesPerJob', type=str, default='',
        help='Number of files per job in form of a json file with "sample":num pairs'
    )

def add_common_resubmit(parser):
    parser_directories = parser.add_mutually_exclusive_group(required=True)
    parser_directories.add_argument('--jobName', type=str, help='Job name from submission')
    parser_directories.add_argument('--directories', type=str, nargs="*",
        help='Space separated list of submission directories. Unix wild-cards allowed.',
    )
    parser.add_argument('--verbose', action='store_true', help='Verbose status summary')


def add_common_condor(parser):
    parser.add_argument('--vsize', type=int, default=0, help='Override default vsize for condor')
    parser.add_argument('--useAFS', action='store_true', help='Read from AFS rather than creating a usercode')
    parser.add_argument('--resubmit', action='store_true', help='Resubmit failed jobs')
    parser.add_argument('--useHDFS', action='store_true', help='Use HDFS to read files')
    parser.add_argument('--dryrun', action='store_true', help='Do not submit jobs')
    parser.add_argument('--user', type=str, default=UNAME, help='Username for grid storage. i.e. /store/user/[username]/')


def add_common_crab(parser):
    parser.add_argument('--publish', action='store_true', help='Publish output to DBS')
    parser.add_argument('--site', type=str, default='T2_US_Wisconsin',
        help='Site to write output files. Can check write pemissions with `crab checkwrite --site=<SITE>`.'
    )
    parser.add_argument('--dryrun', action='store_true', help='Do not submit jobs')
    parser.add_argument('--user', type=str, default=UNAME, help='Username for grid storage. i.e. /store/user/[username]/')


def parse_command_line(argv):
    parser = argparse.ArgumentParser(description='Submit jobs to grid')

    # submission type
    subparsers = parser.add_subparsers(help='Submission mode')

    # crabSubmit
    parser_crabSubmit = subparsers.add_parser('crabSubmit', help='Submit jobs via crab')
    add_common_submit(parser_crabSubmit)
    add_common_inputs(parser_crabSubmit)
    add_common_splitting(parser_crabSubmit)
    add_common_crab(parser_crabSubmit)
    parser_crabSubmit.set_defaults(submit=submit_crab)

    # crabStatus
    parser_crabStatus = subparsers.add_parser('crabStatus', help='Check job status via crab')
    add_common_resubmit(parser_crabStatus)
    parser_crabStatus.set_defaults(submit=status_crab)

    # crabResubmit
    parser_crabResubmit = subparsers.add_parser('crabResubmit', help='Resubmit crab jobs')
    add_common_resubmit(parser_crabResubmit)
    parser_crabResubmit.set_defaults(submit=resubmit_crab)

    # condorSubmit
    parser_condorSubmit = subparsers.add_parser('condorSubmit', help='Submit jobs via condor')
    add_common_submit(parser_condorSubmit)
    add_common_inputs(parser_condorSubmit)
    add_common_splitting(parser_condorSubmit)
    add_common_condor(parser_condorSubmit)
    parser_condorSubmit.set_defaults(submit=submit_condor)

    # condorStatus
    parser_condorStatus = subparsers.add_parser('condorStatus', help='Check job status via condor')
    add_common_resubmit(parser_condorStatus)
    parser_condorStatus.set_defaults(submit=status_condor)

    # condorMerge
    parser_condorMerge = subparsers.add_parser('condorMerge', help='Submit merge job via condor')
    parser_condorMerge.add_argument('jobName', type=str, help='Job Name for submission')
    add_common_inputs(parser_condorMerge)
    add_common_condor(parser_condorMerge)
    parser_condorMerge.set_defaults(submit=submit_condor)

    return parser.parse_args(argv)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    args = parse_command_line(argv)

    submit_string = args.submit(args)

if __name__ == "__main__":
    status = main()
    sys.exit(status)

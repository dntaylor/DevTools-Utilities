import FWCore.ParameterSet.Config as cms

from FWCore.ParameterSet.VarParsing import VarParsing
options = VarParsing('analysis')

options.outputFile = 'events.root'
options.inputFiles = '/store/data/Run2016G/DoubleMuon/MINIAOD/23Sep2016-v1/100000/0A30F7A9-ED8F-E611-91F1-008CFA1C6564.root' # ReReco
options.maxEvents = 1

process = cms.Process("PickEvent")

process.source = cms.Source ("PoolSource",
          fileNames = cms.untracked.vstring(options.inputFiles)
)

process.maxEvents = cms.untracked.PSet(
            input = cms.untracked.int32 (options.maxEvents)

)

process.Out = cms.OutputModule("PoolOutputModule",
         fileName = cms.untracked.string (options.outputFile)
)

process.end = cms.EndPath(process.Out)

#
#
from omero import scripts
import omero.model
from omero.rtypes import rstring, rlong
from datetime import datetime
from math import ceil
from itertools import izip

import sys, os
basedir = os.getenv('HOME') + '/work/omero-pychrm'
for p in ['/utils', '/pychrm-lib']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
import pychrm.FeatureSet





def createWeights(tcIn, tcF, tcW, tcL, project, featureThreshold):
    # Build the classifier (basically a set of weights)
    message = ''
    trainFts = pychrm.FeatureSet.FeatureSet_Discrete()

    classId = 0
    for ds in project.listChildren():
        message += 'Processing dataset id:%d\n' % ds.getId()
        message += addToFeatureSet(tcIn, ds, trainFts, classId)
        classId += 1

    tmp = trainFts.ContiguousDataMatrix()
    weights = pychrm.FeatureSet.FisherFeatureWeights.NewFromFeatureSet(trainFts)

    if featureThreshold < 1.0:
        nFeatures = ceil(len(weights.names) * featureThreshold)
        message += 'Selecting top %d features\n' % nFeatures
        weights = weights.Threshold(nFeatures)
        trainFts = reduceFeatures(trainFts, weights)


    # Save the features, weights and classes to tables
    # TODO:Delete existing tables
    #if getProjectTableFile(tcOutF, tcF.tableName, proj):
    FeatureHandler.createClassifierTables(tcF, tcW, tcL, weights.names)
    message += 'Created classifier tables\n'

    # We've (ab)used imagenames_list to hold the image ids
    ids = [long(a) for b in trainFts.imagenames_list for a in b]
    classIds = [a for b in [[i] * len(z) for i, z in izip(xrange(
                    len(trainFts.imagenames_list)), trainFts.imagenames_list)]
                for a in b]
    featureMatrix = trainFts.data_matrix
    featureNames = weights.names
    featureWeights = weights.values
    classNames = trainFts.classnames_list

    FeatureHandler.saveClassifierTables(
        tcF, tcW, tcL, ids, classIds, featureMatrix,
        featureNames, featureWeights, classNames)

    FeatureHandler.addFileAnnotationTo(tcF, tcF.table, project)
    FeatureHandler.addFileAnnotationTo(tcW, tcW.table, project)
    FeatureHandler.addFileAnnotationTo(tcL, tcL.table, project)

    message += 'Saved classifier\n'

    ns = FeatureHandler.createClassifierTagSet(
        tcL, FeatureHandler.PYCHRM_NAMESPACE, project.getName(), classNames,
        project)
    message += 'Created tagset: %s\n' % ns
    return trainFts, weights, message


def reduceFeatures(fts, weights):
    if fts.source_path is None:
        fts.source_path = ''
    ftsr = fts.FeatureReduce(weights.names)
    return ftsr


def addToFeatureSet(tcIn, ds, fts, classId):
    message = ''

    tid = FeatureHandler.getAttachedTableFile(tcIn, tcIn.tableName, ds)
    if tid:
        if not FeatureHandler.openTable(tcIn, tableId=tid):
            return message + '\nERROR: Table not opened'
        message += 'Opened table id:%d\n' % tid
    else:
        message += 'ERROR: Table not found for Dataset id:%d' % ds.getId()
        return message

    #fts = pychrm.FeatureSet.FeatureSet_Discrete({'num_images': 0})
    for image in ds.listChildren():
        imId = image.getId()
        message += '\tProcessing features for image id:%d\n' % imId

        sig = pychrm.FeatureSet.Signatures()
        (sig.names, sig.values) = FeatureHandler.loadFeatures(tcIn, imId)
        sig.source_file = str(imId)
        fts.AddSignature(sig, classId)

    fts.classnames_list[classId] = ds.getName()
    return message


def trainClassifier(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    projectId = scriptParams['IDs']
    contextName = scriptParams['Context_Name']
    featureThreshold = scriptParams['Features_threshold'] / 100.0

    if len(projectId) != 1:
        raise Exception('A single project must be provided')

    projectId = projectId[0]

    tableNameIn = '/Pychrm/' + contextName + FeatureHandler.SMALLFEATURES_TABLE
    tableNameOutF = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_FEATURES_TABLE
    tableNameOutW = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_WEIGHTS_TABLE
    tableNameOutL = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_LABELS_TABLE
    message += 'tableNameIn:' + tableNameIn + '\n'
    message += 'tableNameOutF:' + tableNameOutF + '\n'
    message += 'tableNameOutW:' + tableNameOutW + '\n'
    message += 'tableNameOutL:' + tableNameOutL + '\n'

    tcIn = FeatureHandler.connFeatureTable(client, tableNameIn)
    tcOutF = FeatureHandler.connClassifierTable(client, tableNameOutF)
    tcOutW = FeatureHandler.connClassifierTable(client, tableNameOutW)
    tcOutL = FeatureHandler.connClassifierTable(client, tableNameOutL)

    try:
        # Training
        message += 'Training classifier\n'
        trainProject = tcIn.conn.getObject(dataType, projectId)
        trainFts, weights, msg = createWeights(
            tcIn, tcOutF, tcOutW, tcOutL, trainProject, featureThreshold)
        message += msg

    except:
        print message
        raise
    finally:
        tcIn.closeTable()
        tcOutF.closeTable()
        tcOutW.closeTable()
        tcOutL.closeTable()

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'Pycharm_Build_Classifier.py',
        'Build a classifier from features calculated over two or more ' +
        'datasets in a project, each dataset represents a different class',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The training source.',
                       values=[rstring('Project')], default='Project'),

        scripts.List(
            'IDs', optional=False, grouping='1',
            description='Project ID').ofType(rlong(0)),

        scripts.String(
            'Context_Name', optional=False, grouping='1',
            description='The name of the classification context.',
            default='Example'),

        scripts.Long(
            'Features_threshold', optional=False, grouping='2',
            description='The proportion of features to keep (%)\n' + \
                '(Should be a Double but doesn\'t seem to work)',
            default=100),

        version = '0.0.1',
        authors = ['Simon Li', 'OME Team'],
        institutions = ['University of Dundee'],
        contact = 'ome-users@lists.openmicroscopy.org.uk',
    )

    try:
        startTime = datetime.now()
        session = client.getSession()
        client.enableKeepAlive(60)
        scriptParams = {}

        # process the list of args above.
        for key in client.getInputKeys():
            if client.getInput(key):
                scriptParams[key] = client.getInput(key, unwrap=True)
        message = str(scriptParams) + '\n'

        # Run the script
        message += trainClassifier(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


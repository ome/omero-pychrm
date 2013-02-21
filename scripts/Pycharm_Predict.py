#
#
from omero import scripts
import omero.model
from omero.rtypes import rstring, rlong
from datetime import datetime
import numpy

import sys, os
basedir = os.getenv('HOME') + '/work/omero-pychrm'
for p in ['/utils', '/pychrm-lib']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
import pychrm.FeatureSet



def loadClassifier(tcF, tcW, tcL, project):
    tidF = FeatureHandler.getAttachedTableFile(tcF, tcF.tableName, project)
    tidW = FeatureHandler.getAttachedTableFile(tcW, tcW.tableName, project)
    tidL = FeatureHandler.getAttachedTableFile(tcL, tcL.tableName, project)

    if tidF is None or tidW is None or tidL is None:
        raise Exception('Incomplete set of classifier tables: %s' %
                        (tidF, tidW, tidL))

    FeatureHandler.openTable(tcF, tableId=tidF)
    FeatureHandler.openTable(tcW, tableId=tidW)
    FeatureHandler.openTable(tcL, tableId=tidL)

    cls = FeatureHandler.loadClassifierTables(tcF, tcW, tcL)
    #ids,trainClassIds,featureMatrix,featureNames,weights,classIds,classNames

    trainFts = pychrm.FeatureSet.FeatureSet_Discrete()

    #if cls['classIds'] != sorted(cls['classIds']):
    if cls['classIds'] != range(len(cls['classIds'])):
        raise Exception('Incorrectly ordered class IDs')
    trainFts.classnames_list = cls['classNames']

    # Objects should be in order of increasing class ID, this makes rebuilding
    # data_list much faster
    nclasses = len(cls['classNames'])
    classCounts = [0] * nclasses
    cprev = -1
    for c in cls['trainClassIds']:
        if c < cprev:
            raise Exception('Incorrectly ordered training class feature data')
        cprev = c
        classCounts[c] += 1
    trainFts.classsizes_list = classCounts

    classFts = [[] for n in xrange(len(cls['classNames']))]
    p = 0
    for i in xrange(nclasses):
        classFts[i] = numpy.array(cls['featureMatrix'][p:(p + classCounts[i])])
        p += classCounts[i]
    trainFts.data_list = classFts
    trainFts.num_images = sum(classCounts)
    trainFts.num_features = len(cls['featureNames'])
    trainFts.num_classes = nclasses

    trainFts.featurenames_list = cls['featureNames']
    trainFts.imagenames_list = cls['ids']
    tmp = trainFts.ContiguousDataMatrix()

    weights = pychrm.FeatureSet.FisherFeatureWeights(
        data_dict={'names': cls['featureNames'], 'values': cls['weights']})
    return (trainFts, weights)



def predictDataset(tcIn, trainFts, predDs, weights):
    message = ''
    predictFts = pychrm.FeatureSet.FeatureSet_Discrete()
    classId = 0
    message += addToFeatureSet(tcIn, predDs, predictFts, classId)
    tmp = predictFts.ContiguousDataMatrix()

    predictFts = reduceFeatures(predictFts, weights)

    pred = pychrm.FeatureSet.DiscreteBatchClassificationResult.New(
        trainFts, predictFts, weights)
    return pred, message


def formatPredResult(r):
    return 'ID:%s Prediction:%s Probabilities:[%s]' % \
        (r.source_file, r.predicted_class_name,
         ' '.join(['%.3e' % p for p in r.marginal_probabilities]))


def addPredictionsToImages(tc, prediction, dsId, commentImages, tagSet):
    """
    Add a comment to the dataset containing the prediction results.
    @param commentImages If true add comment to individual images as well
    as the dataset
    @param tagSet If provided then tag images with the predicted label
    """
    dsComment = ''

    tagMap = {}
    if tagSet:
        for tag in tagSet.listTagsInTagset():
            tagMap[tag.getValue()] = tag

    for r in prediction.individual_results:
        c = formatPredResult(r)
        imId = long(r.source_file)

        if commentImages:
            FeatureHandler.addCommentTo(tc, c, 'Image', imId)
        im = tc.conn.getObject('Image', imId)
        dsComment += im.getName() + ' ' + c + '\n'

        if tagMap:
            tag = tagMap[r.predicted_class_name]._obj
            FeatureHandler.addTagTo(tc, tag, 'Image', imId)

    FeatureHandler.addCommentTo(tc, dsComment, 'Dataset', dsId)


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
        #message += extractFeatures(tc, d, im = image) + '\n'

        sig = pychrm.FeatureSet.Signatures()
        (sig.names, sig.values) = FeatureHandler.loadFeatures(tcIn, imId)
        #sig.source_file = image.getName()
        sig.source_file = str(imId)
        fts.AddSignature(sig, classId)

    fts.classnames_list[classId] = ds.getName()
    return message


def predict(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    projectId = scriptParams['Training_Project_ID']
    dataType = scriptParams['Data_Type']
    predictIds = scriptParams['IDs']
    commentImages = scriptParams['Comment_Images']
    tagImages = scriptParams['Tag_Images']

    contextName = scriptParams['Context_Name']

    tableNameIn = '/Pychrm/' + contextName + FeatureHandler.SMALLFEATURES_TABLE
    tableNameF = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_FEATURES_TABLE
    tableNameW = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_WEIGHTS_TABLE
    tableNameL = '/Pychrm/' + contextName + \
        FeatureHandler.CLASS_LABELS_TABLE
    message += 'tableNameIn:' + tableNameIn + '\n'
    message += 'tableNameF:' + tableNameF + '\n'
    message += 'tableNameW:' + tableNameW + '\n'
    message += 'tableNameL:' + tableNameL + '\n'

    tcIn = FeatureHandler.connFeatureTable(client, tableNameIn)
    tcF = FeatureHandler.connClassifierTable(client, tableNameF)
    tcW = FeatureHandler.connClassifierTable(client, tableNameW)
    tcL = FeatureHandler.connClassifierTable(client, tableNameL)

    try:
        message += 'Loading classifier\n'
        trainProject = tcIn.conn.getObject('Project', projectId)
        trainFts, weights = loadClassifier(tcF, tcW, tcL, trainProject)
        classifierName = FeatureHandler.CLASSIFIER_PYCHRM_NAMESPACE
        tagSet = FeatureHandler.getClassifierTagSet(
            tcF, classifierName, trainProject.getName(), trainProject)

        # Predict
        message += 'Predicting\n'
        predDatasets = tcIn.conn.getObjects(dataType, predictIds)

        for ds in predDatasets:
            message += 'Predicting dataset id:%d\n' % ds.getId()
            pred, msg = predictDataset(tcIn, trainFts, ds, weights)
            message += msg
            addPredictionsToImages(tcIn, pred, ds.getId(),
                                   commentImages, tagSet)

    except:
        print message
        raise
    finally:
        tcIn.closeTable()
        tcF.closeTable()
        tcW.closeTable()
        tcL.closeTable()

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'Pycharm_Build_Classifier.py',
        'Build a classifier from features calculated over two or more ' +
        'datasets, each dataset represents a different class',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The source data to be predicted.',
                       values=[rstring('Project'), rstring('Dataset'), rstring('Image')], default='Dataset'),

        scripts.List(
            'IDs', optional=False, grouping='1',
            description='List of Dataset IDs to be predicted').ofType(rlong(0)),

        scripts.Long(
            'Training_Project_ID', optional=False, grouping='1',
            description='Project ID used for training'),

        scripts.String(
            'Context_Name', optional=False, grouping='1',
            description='The name of the classification context.',
            default='Example'),

        scripts.Bool(
            'Comment_Images', optional=False, grouping='1',
            description='Add predictions as image comments', default=False),

        scripts.Bool(
            'Tag_Images', optional=False, grouping='1',
            description='Tag images with predictions', default=False),

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
        message += predict(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


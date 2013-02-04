#
#
from omero import scripts
import omero.model
from omero.rtypes import rstring, rlong
from omero.gateway import DatasetWrapper, FileAnnotationWrapper, ImageWrapper
from datetime import datetime


import sys, os
basedir = os.getenv('HOME') + '/work/omero-pychrm'
for p in ['/utils', '/pychrm-lib']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
import pychrm.FeatureSet



def addFileAnnotationToDataset(tc, table, d):
    """
    Attach the annotation to the dataset if not already attached
    """
    namespace = FeatureHandler.NAMESPACE

    tfile = table.getOriginalFile()

    d = tc.conn.getObject('Dataset', d.getId())
    for a in d.listAnnotations(namespace):
        if isinstance(a, FileAnnotationWrapper):
            if tfile.getId() == a._obj.getFile().getId():
                return 'Already attached'

    fa = omero.model.FileAnnotationI()
    fa.setFile(tfile)
    fa.setNs(rstring(namespace))
    fa.setDescription(rstring(namespace + ':' + tfile.getName().val))

    annLink = omero.model.DatasetAnnotationLinkI()
    annLink.link(omero.model.DatasetI(d.getId(), False), fa)

    annLink = tc.conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached file id:%d to dataset id:%d\n' % \
        (tfile.getId().getValue(), d.getId())


def addCommentTo(tc, comment, objType, objId):
    namespace = FeatureHandler.NAMESPACE

    ca = omero.model.CommentAnnotationI()
    ca.setNs(rstring(namespace))
    ca.setTextValue(rstring(comment))

    if objType == "Dataset":
        annLink = omero.model.DatasetAnnotationLinkI()
        annLink.link(omero.model.DatasetI(objId, False), ca)
    else:
        annLink = omero.model.ImageAnnotationLinkI()
        annLink.link(omero.model.ImageI(objId, False), ca)

    annLink = tc.conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached comment to %s id:%d\n' % (objType, objId)


def getDatasetTableFile(tc, tableName, d):
    """
    See if the dataset has a table file annotation
    """
    namespace = FeatureHandler.NAMESPACE

    # Refresh the dataset, as the cached view might not show the latest
    # annotations
    d = tc.conn.getObject('Dataset', d.getId())

    for a in d.listAnnotations(namespace):
        if isinstance(a, FileAnnotationWrapper):
            if tableName == a.getFileName():
                return a._obj.getFile().getId().getValue()
                #return a._obj.getFile()

    return None


def createWeights(tcIn, tcOut, datasets):
    # Build the classifier (basically a set of weights)
    message = ''
    trainFts = pychrm.FeatureSet.FeatureSet_Discrete()

    classId = 0
    for ds in datasets:
        message += 'Processing dataset id:%d\n' % ds.getId()
        message += addToFeatureSet(tcIn, ds, trainFts, classId)
        classId += 1

    tmp = trainFts.ContiguousDataMatrix()
    weights = pychrm.FeatureSet.FisherFeatureWeights.NewFromFeatureSet(trainFts)

    #if not FeatureHandler.openTable(tcOut, tableName=tcOut.tableName):
    if not FeatureHandler.openTable(tcOut):
        FeatureHandler.createTable(tcOut, weights.names)
        message += 'Created new table\n'
        #message += addFileAnnotationToDataset(tc, tc.table, ds)

    FeatureHandler.saveFeatures(tcOut, 0, weights)
    return trainFts, weights, message + 'Saved classifier weights\n'


def predictDataset(tcIn, trainFts, predDs, weights):
    message = ''
    predictFts = pychrm.FeatureSet.FeatureSet_Discrete()
    classId = 0
    message += addToFeatureSet(tcIn, predDs, predictFts, classId)
    tmp = trainFts.ContiguousDataMatrix()

    pred = pychrm.FeatureSet.DiscreteBatchClassificationResult.New(
        trainFts, predictFts, weights)
    return pred, message


    #message = FeatureHandler.saveFeatures(tcOut, 0, weights)
    #return message + 'Saved classifier weights\n'


def formatPredResult(r):
    return 'ID:%s Prediction:%s Probabilities:[%s]' % \
        (r.source_file, r.predicted_class_name,
         ' '.join(['%.3e' % p for p in r.marginal_probabilities]))


def addPredictionsAsComments(tc, prediction, dsId, commentImages):
    """
    Add a comment to the dataset containing the prediction results.
    @param commentImages If true add comment to individual images as well
    as the dataset
    """
    dsComment = ''

    for r in prediction.individual_results:
        c = formatPredResult(r)
        imId = long(r.source_file)

        if commentImages:
            addCommentTo(tc, c, 'Image', imId)
        im = tc.conn.getObject('Image', imId)
        dsComment += im.getName() + ' ' + c + '\n'

    addCommentTo(tc, dsComment, 'Dataset', dsId)


def addToFeatureSet(tcIn, ds, fts, classId):
    message = ''

    tid = getDatasetTableFile(tcIn, tcIn.tableName, ds)
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


def trainAndPredict(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    trainIds = scriptParams['Training_IDs']
    predictIds = scriptParams['Predict_IDs']
    commentImages = scriptParams['Comment_images']

    contextName = scriptParams['Context_Name']

    tableNameIn = '/Pychrm/' + contextName + '/SmallFeatureSet.h5'
    tableNameOut = '/Pychrm/' + contextName + '/Weights.h5'
    message += 'tableNameIn:' + tableNameIn + '\n'
    message += 'tableNameOut:' + tableNameOut + '\n'
    tcIn = FeatureHandler.connect(client, tableNameIn)
    tcOut = FeatureHandler.connect(client, tableNameOut)

    try:
        # Training
        message += 'Training classifier\n'
        trainDatasets = tcIn.conn.getObjects(dataType, trainIds)
        trainFts, weights, msg = createWeights(tcIn, tcOut, trainDatasets)
        message += msg

        # Predict
        message += 'Predicting\n'
        predDatasets = tcIn.conn.getObjects(dataType, predictIds)

        for ds in predDatasets:
            message += 'Predicting dataset id:%d\n' % ds.getId()
            pred, msg = predictDataset(tcIn, trainFts, ds, weights)
            message += msg
            addPredictionsAsComments(tcOut, pred, ds.getId(), commentImages)

    finally:
        tcIn.closeTable()
        tcOut.closeTable()

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'PycharmBuildClassifier.py',
        'Build a classifier from features calculated over two or more ' +
        'datasets, each dataset represents a different class',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The data you want to work with.',
                       values=[rstring('Dataset')], default='Dataset'),

        scripts.List(
            'Training_IDs', optional=False, grouping='1',
            description='List of training Dataset IDs').ofType(rlong(0)),

        scripts.List(
            'Predict_IDs', optional=False, grouping='1',
            description='List of Dataset IDs to be predicted').ofType(rlong(0)),

        scripts.Bool(
            'Comment_images', optional=False, grouping='1',
            description='Add predictions as image comments'),

        scripts.String(
            'Context_Name', optional=False, grouping='1',
            description='The name of the classification context.',
            default='Example'),

        version = '0.0.1',
        authors = ['Simon Li', 'OME Team'],
        institutions = ['University of Dundee'],
        contact = 'ome-users@lists.openmicroscopy.org.uk',
    )

    try:
        startTime = datetime.now()
        session = client.getSession()
        scriptParams = {}

        # process the list of args above.
        for key in client.getInputKeys():
            if client.getInput(key):
                scriptParams[key] = client.getInput(key, unwrap=True)
        message = str(scriptParams) + '\n'

        # Run the script
        message += trainAndPredict(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


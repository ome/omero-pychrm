# Handle saving and loading of features and classes between Pychrm and
# OMERO.tables

from itertools import izip
from TableConnection import FeatureTableConnection, TableConnectionError
from TableConnection import TableConnection
import omero
from omero.rtypes import wrap


######################################################################
# Constants for OMERO
######################################################################
CLASSIFIER_PARENT_NAMESPACE = '/classifier'
CLASSIFIER_LABEL_NAMESPACE = '/label'

PYCHRM_NAMESPACE = '/testing/pychrm'
CLASSIFIER_PYCHRM_NAMESPACE = CLASSIFIER_PARENT_NAMESPACE + PYCHRM_NAMESPACE

SMALLFEATURES_TABLE = '/SmallFeatureSet.h5'

CLASS_FEATURES_TABLE = '/ClassFeatures.h5'
CLASS_WEIGHTS_TABLE = '/Weights.h5'
CLASS_LABELS_TABLE = '/ClassLabels.h5'


######################################################################
# Feature handling
######################################################################

# Maximum number of rows to read/write in one go
CHUNK_SIZE = 100

def parseFeatureName(name):
    """
    Convert a single value feature name in the form
    'ABC ... [NN]'
    to a feature group name and size in the form
    'ABC ...', NN
    """
    ft, idx = name.split(' [')
    idx = int(idx[:-1])
    return (ft, idx)


def createFeatureName(ft, idx):
    """
    The inverse of parseFeatureName
    """
    name = '%s [%d]' % (ft, idx)
    return name


def featureSizes(names):
    """
    Convert a list of single value feature names to a dictionary of
    feature group names and sizes
    """
    featSizes = {}
    for name in names:
        ft, idx = parseFeatureName(name)
        if ft in featSizes:
            featSizes[ft] = max(featSizes[ft], idx)
        else:
            featSizes[ft] = idx

    # Indexing starts at 0, so add one to sizes
    for ft in featSizes:
        featSizes[ft] += 1
    return featSizes


def connFeatureTable(client, tableName):
    tc = FeatureTableConnection(client=client, tableName=tableName)
    return tc


def connClassifierTable(client, tableName):
    tc = TableConnection(client=client, tableName=tableName)
    return tc


def createTable(tc, featureNames):
    """
    Initialise an OMERO.table for storing features
    @param featureNames Either a mapping of feature names to feature sizes, or a
    list of single value feature names which can be parsed using
    parseFeatureName
    """
    # Unparsed list or dict?
    if hasattr(featureNames, 'keys'):
        features = featureNames
    else:
        features = featureSizes(featureNames)

    colNames = sorted(features.keys())
    desc = [(name, features[name]) for name in colNames]
    tc.createNewTable('id', desc)
    return tc


def openTable(tc, tableName = None, tableId = None):
    try:
        tc.openTable(tableName=tableName, tableId=tableId)
        return True
    except TableConnectionError as e:
        print "No table found: %s" % e
        return False


def isTableCompatible(tc, features):
    """
    Check whether an existing table is compatible with this set of features,
    that is whether suitable columsn exist
    @return true if this set of features can be stored in this table
    """
    cols = tc.getHeaders()
    colMap = dict([(c.name, c) for c in cols])
    featSizes = featureSizes(features.names)

    for (ft, sz) in featSizes.iteritems():
        if (ft not in colMap or colMap[ft].size != sz):
            print '%s [%d] is incompatible' %  (ft, sz)
            return False
    return True


def tableContainsId(tc, id):
    """
    Check whether this ID is already present in the table
    """
    return tc.getRowId(id) is not None


def saveFeatures(tc, id, features):
    """
    Save the features to a table
    @param features an object with field names holding a list of single value
    feature names, and values holding a list of doubles corresponding to names
    """
    cols = tc.getHeaders()
    colMap = dict([(c.name, c) for c in cols])
    cols[0].values = [id]

    for (name, value) in izip(features.names, features.values):
        ft, idx = parseFeatureName(name)
        col = colMap[ft]
        if not col.values:
            col.values = [[float('nan')] * col.size]
        col.values[0][idx] = value

    tc.addData(cols)


def loadFeatures(tc, id):
    """
    Load features for an object from a table
    @return a (names, values) tuple where names is a list of single value
    features and value are the corresponding feature values
    """
    r = tc.getRowId(id)
    # Skip the first id column
    colNumbers = range(1, len(tc.getHeaders()))
    cols = tc.readArray(colNumbers, r, r + 1)
    names = []
    values = []
    for col in cols:
        names.extend([createFeatureName(col.name, x) for x in xrange(col.size)])
        values.extend(col.values[0])

    return (names, values)



######################################################################
# Save a classifier
######################################################################


def createClassifierTables(tc1, tc2, tc3, featureNames):
    """
    Create a set of OMERO.tables for storing the state of a trained image
    classifier. The first table stores the training samples with reduced
    features and classes, the second stores a list of weights and feature
    names, and the third stores the class IDs and class names
    """
    schema1 = [
        omero.grid.LongColumn('id'),
        omero.grid.LongColumn('label'),
        omero.grid.DoubleArrayColumn('features', '', len(featureNames)),
        ]
    tc1.newTable(schema1)

    schema2 = [
        omero.grid.StringColumn('featurename', '', 1024),
        omero.grid.DoubleColumn('weight'),
        ]
    tc2.newTable(schema2)

    schema3 = [
        omero.grid.LongColumn('classID'),
        omero.grid.StringColumn('className', '', 1024),
        ]
    tc3.newTable(schema3)


def saveClassifierTables(tc1, tc2, tc3,
                         ids, classIds, featureMatrix,
                         featureNames, weights, classNames):
    """
    Save the classifier state (reduced features, labels and weights)
    """
    cols1 = tc1.getHeaders()
    cols1[0].values = ids
    cols1[1].values = classIds
    cols1[2].values = featureMatrix
    tc1.chunkedAddData(cols1, CHUNK_SIZE)

    cols2 = tc2.getHeaders()
    cols2[0].values = featureNames
    cols2[1].values = weights
    tc2.chunkedAddData(cols2, CHUNK_SIZE)

    cols3 = tc3.getHeaders()
    cols3[0].values = range(len(classNames))
    cols3[1].values = classNames
    tc3.chunkedAddData(cols3, CHUNK_SIZE)


def loadClassifierTables(tc1, tc2, tc3):
    """
    Load the classifier state (reduced features, labels and weights)
    """
    d1 = tc1.chunkedRead(
        range(len(tc1.getHeaders())), 0, tc1.getNumberOfRows(), CHUNK_SIZE)
    cols1 = d1.columns
    ids = cols1[0].values
    trainClassIds = cols1[1].values
    featureMatrix = cols1[2].values

    d2 = tc2.chunkedRead(
        range(len(tc2.getHeaders())), 0, tc2.getNumberOfRows(), CHUNK_SIZE)
    cols2 = d2.columns
    featureNames = cols2[0].values
    weights = cols2[1].values

    d3 = tc3.chunkedRead(
        range(len(tc3.getHeaders())), 0, tc3.getNumberOfRows(), CHUNK_SIZE)
    cols3 = d3.columns
    classIds = cols3[0].values
    classNames = cols3[1].values

    return {'ids': ids, 'trainClassIds': trainClassIds,
            'featureMatrix': featureMatrix,
            'featureNames': featureNames, 'weights': weights,
            'classIds': classIds, 'classNames': classNames}


######################################################################
# Annotations
######################################################################


def addFileAnnotationTo(tc, table, obj):
    """
    Attach the annotation to an object (dataset/project) if not already attached
    """
    tfile = table.getOriginalFile()
    oclass = obj.OMERO_CLASS

    obj = tc.conn.getObject(oclass, obj.getId())
    for a in obj.listAnnotations(PYCHRM_NAMESPACE):
        if isinstance(a, omero.gateway.FileAnnotationWrapper):
            if tfile.getId() == a._obj.getFile().getId():
                return 'Already attached'

    fa = omero.model.FileAnnotationI()
    fa.setFile(tfile)
    fa.setNs(wrap(PYCHRM_NAMESPACE))
    fa.setDescription(wrap(PYCHRM_NAMESPACE + ':' + tfile.getName().val))

    if oclass == 'Dataset':
        annLink = omero.model.DatasetAnnotationLinkI()
        annLink.link(omero.model.DatasetI(obj.getId(), False), fa)
    elif oclass == 'Project':
        annLink = omero.model.ProjectAnnotationLinkI()
        annLink.link(omero.model.ProjectI(obj.getId(), False), fa)
    else:
        raise Exception('Unexpected object type: %s' % oclass)

    annLink = tc.conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached file id:%d to %s id:%d\n' % \
        (tfile.getId().getValue(), oclass, obj.getId())


def getAttachedTableFile(tc, tableName, obj):
    """
    See if this object (dataset/project) has a table file annotation
    """
    # Refresh the dataset, as the cached view might not show the latest
    # annotations
    obj = tc.conn.getObject(obj.OMERO_CLASS, obj.getId())

    for a in obj.listAnnotations(PYCHRM_NAMESPACE):
        if isinstance(a, omero.gateway.FileAnnotationWrapper):
            if tableName == a.getFileName():
                return a._obj.getFile().getId().getValue()

    return None


def addCommentTo(tc, comment, objType, objId):
    """
    Add a comment to an object (dataset/project/image)
    """
    ca = omero.model.CommentAnnotationI()
    ca.setNs(wrap(PYCHRM_NAMESPACE))
    ca.setTextValue(wrap(comment))

    if objType == "Dataset":
        annLink = omero.model.DatasetAnnotationLinkI()
        annLink.link(omero.model.DatasetI(objId, False), ca)
    elif objType == "Project":
        annLink = omero.model.ProjectAnnotationLinkI()
        annLink.link(omero.model.ProjectI(objId, False), ca)
    elif objType == "Image":
        annLink = omero.model.ImageAnnotationLinkI()
        annLink.link(omero.model.ImageI(objId, False), ca)
    else:
        raise Exception('Unexpected object type: %s' % oclass)

    annLink = tc.conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached comment to %s id:%d\n' % (objType, objId)


def createClassifierTagSet(tc, classifierName, instanceName, labels,
                           project = None):
    """
    Create a tagset and labels associated with an instance of a classifier
    """
    us = tc.conn.getUpdateService()

    tagSet = omero.model.TagAnnotationI()
    instanceNs = classifierName + '/' + instanceName
    tagSet.setTextValue(wrap(instanceNs));
    tagSet.setNs(wrap(omero.constants.metadata.NSINSIGHTTAGSET));
    tagSet.setDescription(wrap('Classification labels for ' + instanceNs))
    tagSetR = us.saveAndReturnObject(tagSet);
    tagSetR.unload()

    for lb in labels:
        tag = omero.model.TagAnnotationI()
        tag.setTextValue(wrap(lb));
        tag.setNs(wrap(instanceNs));
        tagR = us.saveAndReturnObject(tag);

        link = omero.model.AnnotationAnnotationLinkI()
        link.setChild(tagR)
        link.setParent(tagSetR)
        linkR = us.saveAndReturnObject(link)
        assert(linkR)

    if project:
        annLink = omero.model.ProjectAnnotationLinkI()
        annLink.link(omero.model.ProjectI(project.getId(), False), tagSetR)
        us.saveAndReturnObject(annLink);

    return instanceNs


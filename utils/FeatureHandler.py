# Handle saving and loading of features and classes between Pychrm and
# OMERO.tables

from itertools import izip
from TableConnection import FeatureTableConnection, TableConnectionError
from TableConnection import TableConnection
from omero.grid import LongColumn, DoubleColumn, DoubleArrayColumn, StringColumn


######################################################################
# Constants for OMERO
######################################################################
NAMESPACE = '/testing/pychrm'
SMALLFEATURES_TABLE = '/SmallFeatureSet.h5'

CLASS_FEATURES_TABLE = '/ClassFeatures.h5'
CLASS_WEIGHTS_TABLE = '/Weights.h5'
CLASS_LABELS_TABLE = '/ClassLabels.h5'


######################################################################
# Feature handling
######################################################################


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


def connect(client = None, tableName = None):
    user = 'test1'
    passwd = 'test1'
    if not tableName:
        tableName = '/test.h5'
    host = 'localhost'

    if client:
        tc = FeatureTableConnection(client=client, tableName=tableName)
    else:
        tc = FeatureTableConnection(user, passwd, host, tableName=tableName)
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
        LongColumn('id'),
        LongColumn('label'),
        DoubleArrayColumn('features', '', len(featureNames)),
        ]
    tc1.newTable(schema1)

    schema2 = [
        StringColumn('featurename', '', 1024),
        DoubleColumn('weight'),
        ]
    tc2.newTable(schema2)

    schema3 = [
        LongColumn('classID'),
        StringColumn('className', '', 1024),
        ]
    tc3.newTable(schema3)


def saveClassifierTables(tc1, tc2, tc3,
                         ids, classIds, featureMatrix,
                         featureNames, weights, classNames):
    """
    Save the classifier state (reduced features, labels and weights)
    """
    t1 = tc1.table
    cols1 = t1.getHeaders()
    cols1[0].values = ids
    cols1[1].values = classIds
    cols1[2].values = featureMatrix
    t1.addData(cols1)

    t2 = tc2.table
    cols2 = t2.getHeaders()
    cols2[0].values = featureNames
    cols2[1].values = weights
    t2.addData(cols2)

    t3 = tc3.table
    cols3 = t3.getHeaders()
    cols3[0].values = range(len(classNames))
    cols3[1].values = classNames
    t3.addData(cols3)


def loadClassifierTables(tc1, tc2, tc3):
    """
    Load the classifier state (reduced features, labels and weights)
    """
    t1 = tc1.table
    d1 = tc1.chunkedRead(
        range(len(t1.getHeaders())), 0, t1.getNumberOfRows(), 100)
    cols1 = d1.columns
    ids = cols1[0].values
    trainClassIds = cols1[1].values
    featureMatrix = cols1[2].values

    t2 = tc2.table
    d2 = tc2.chunkedRead(
        range(len(t2.getHeaders())), 0, t2.getNumberOfRows(), 100)
    cols2 = d2.columns
    featureNames = cols2[0].values
    weights = cols2[1].values

    t3 = tc3.table
    d3 = tc3.chunkedRead(
        range(len(t3.getHeaders())), 0, t3.getNumberOfRows(), 100)
    cols3 = d3.columns
    classIds = cols3[0].values
    classNames = cols3[1].values

    return {'ids': ids, 'trainClassIds': trainClassIds,
            'featureMatrix': featureMatrix,
            'featureNames': featureNames, 'weights': weights,
            'classIds': classIds, 'classNames': classNames}


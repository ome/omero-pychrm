# Handle saving and loading of features and classes between Pychrm and
# OMERO.tables

from itertools import izip
from TableConnection import FeatureTableConnection, TableConnectionError
from TableConnection import TableConnection


######################################################################
# Constants for OMERO
######################################################################
NAMESPACE = '/testing/pychrm'


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
    @param features Either a mapping of feature names to feature sizes, or a
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
    assert(len(r) == 1)
    r = r[0]
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
# Image class handling
######################################################################


def createImageClassTable():
    """
    Initialise an OMERO.table for storing image classification labels
    and related data
    """
    user = 'test1'
    passwd = 'test1'
    tableName = '/test-imageClass.h5'
    host = 'localhost'

    tc = TableConnection(user, passwd, host, tableName)

    schema = [
        LongColumn('id'),
        LongColumn('training label'),
        LongColumn('predicted label'),
        ]
    tc.newTable(schema)
    return tc





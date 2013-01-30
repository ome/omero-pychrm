#
#
from omero import scripts
from omero.util import script_utils
import omero.model
from omero.rtypes import rstring, rlong
from omero.gateway import FileAnnotationWrapper, ImageWrapper
from datetime import datetime


import sys
basedir = '/Users/simon/work'
for p in ['/omero-pychrm/utils']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler



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


def countCompleted(tc, ds):
    message = ''

    imIds = [im.getId() for im in ds.listChildren()]
    tid = getDatasetTableFile(tc, tc.tableName, ds)
    if tid is None:
        message += 'Image feature status PRESENT:%d ABSENT:%d\n' % \
            (0, len(imIds))
        return message

    if not FeatureHandler.openTable(tc, tableId=tid):
        message += 'ERROR: Table not opened\n'
        message += 'Image feature status UNKNOWN:%d\n' % len(imIds)
        return message

    message += 'Opened table id:%d\n' % tid
    d = tc.chunkedRead([0], 0, tc.table.getNumberOfRows(), 100)
    ftImIds = d.columns[0].values
    matchedIds = set(imIds).intersection(ftImIds)
    message += 'Image feature status PRESENT:%d ABSENT:%d\n' % \
        (len(matchedIds), len(imIds) - len(matchedIds))
    return message


def processImages(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    ids = scriptParams['IDs']
    contextName = scriptParams['Context_Name']

    tableName = '/Pychrm/' + contextName + '/SmallFeatureSet.h5'
    message += 'tableName:' + tableName + '\n'
    tc = FeatureHandler.connect(client, tableName)

    try:
        # Get the datasets
        objects, logMessage = script_utils.getObjects(tc.conn, scriptParams)
        message += logMessage

        if not objects:
            return message

        datasets = tc.conn.getObjects(dataType, ids)
        for ds in datasets:
            message += 'Processing dataset id:%d\n' % ds.getId()
            msg = countCompleted(tc, ds)
            message += msg

    finally:
        tc.closeTable()

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'PycharmFeatureExtraction.py',
        'Extract the small Pychrm feature set from images',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The data you want to work with.',
                       values=[rstring('Dataset')], default='Dataset'),

        scripts.List(
            'IDs', optional=False, grouping='1',
            description='List of Dataset IDs or Image IDs').ofType(rlong(0)),

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
        message += processImages(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


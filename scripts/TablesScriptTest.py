#
#
from omero import scripts
from omero.util import script_utils
import omero.model
from omero.rtypes import rstring, rlong
from omero.gateway import FileAnnotationWrapper, ImageWrapper
from datetime import datetime
from tempfile import NamedTemporaryFile
from random import random

import sys
basedir = '/Users/simon/work'
for p in ['/omero-pychrm/utils',
          '/wndchrm/pychrm/trunk/build/lib.macosx-10.8-x86_64-2.7/']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
from pychrm.FeatureSet import Signatures

NAMESPACE = 'TablesScriptTest'


def addFileAnnotationToDataset(tc, table, d):
    """
    Attach the annotation to the dataset if not already attached
    """
    namespace = NAMESPACE

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


def getDatasetTableFile(tc, tableName, d):
    """
    See if the dataset has a table file annotation
    """
    namespace = NAMESPACE

    # Refresh the dataset, as the cached view might not show the latest
    # annotations
    d = tc.conn.getObject('Dataset', d.getId())

    for a in d.listAnnotations(namespace):
        if isinstance(a, FileAnnotationWrapper):
            if tableName == a.getFileName():
                return a._obj.getFile().getId().getValue()
                #return a._obj.getFile()

    return None


def addSimulatedData(tc, ds, xid):
    message = ''

    tid = getDatasetTableFile(tc, tc.tableName, ds)
    if tid:
        if not FeatureHandler.openTable(tc, tableId=tid):
            return message + '\nERROR: Table not opened\n'
        message += 'Opened table id:%d\n' % tid

    ftSizes = {}
    ft = Signatures()
    for n in xrange(26):
        ftSizes[chr(n + ord('a')) * 100] = 90 + n
    for k, v in ftSizes.iteritems():
        ft.names.extend(['%s [%d]' % (k, w) for w in xrange(v)])
        ft.values.extend(random() for w in xrange(v))

    # Save the features to a table
    if not tid:
        FeatureHandler.createTable(tc, ft.names)
        message += 'Created new table\n'
        message += addFileAnnotationToDataset(tc, tc.table, ds)

    FeatureHandler.saveFeatures(tc, xid, ft)
    return message + 'Extracted features from id:%d\n' % xid


def process(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    ids = scriptParams['IDs']
    number = scriptParams['Number']
    closeTable = scriptParams['Close_Table']

    tableName = '/test.h5'
    message += 'tableName:' + tableName + '\n'
    tc = FeatureHandler.connect(client, tableName)

    try:
        nimages = 0

        # Get the datasets
        objects, logMessage = script_utils.getObjects(tc.conn, scriptParams)
        message += logMessage

        if not objects:
            return message

        datasets = tc.conn.getObjects(dataType, ids)
        for d in datasets:
            message += 'Processing dataset id:%d\n' % d.getId()
            for n in xrange(number):
                message += 'Processing %d\n' % n
                msg = addSimulatedData(tc, d, n)
                message += msg + '\n'
                if closeTable:
                    tc.closeTable()

    finally:
        tc.closeTable()

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'TablesScriptTest.py',
        'Compare the performance of the table code',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The data you want to work with.',
                       values=[rstring('Dataset')], default='Dataset'),

        scripts.List(
            'IDs', optional=False, grouping='1',
            description='List of Dataset IDs or Image IDs').ofType(rlong(0)),

        scripts.Long(
            'Number', optional=False, grouping='2',
            description='Number of rows to add to the table.',
            default=True),

        scripts.Bool(
            'Close_Table', optional=False, grouping='2',
            description='Should the table be closed after every write?',
            default=True),

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
        message += process(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


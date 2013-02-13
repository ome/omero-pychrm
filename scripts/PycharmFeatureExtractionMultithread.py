#
#
from omero import scripts
from omero.util import script_utils
import omero.model
from omero.rtypes import rstring, rlong
from omero.gateway import FileAnnotationWrapper, ImageWrapper
from datetime import datetime
from tempfile import NamedTemporaryFile
#import threading
import multiprocessing


import sys, os
basedir = os.getenv('HOME') + '/work/omero-pychrm'
for p in ['/utils', '/pychrm-lib']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
from pychrm.FeatureSet import Signatures




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


def extractFeatures(tc, ds, newOnly, imageId = None, im = None):
    message = ''

    # dataset must be explicitly provided because an image can be linked to
    # multiple datasets in which case im.getDataset() doesn't work
    if not im:
        if not imageId:
            raise Exception('No input image')

        im = tc.conn.getObject('Image', imageId)
        if not im:
            return 'Image id:%d not found\n' % imageId
    else:
        imageId = im.getId()

    tid = getDatasetTableFile(tc, tc.tableName, ds)
    if tid:
        if not FeatureHandler.openTable(tc, tableId=tid):
            return message + '\nERROR: Table not opened\n'
        message += 'Opened table id:%d\n' % tid

        if newOnly and FeatureHandler.tableContainsId(tc, imageId):
            return message + 'Image id:%d features already in table' % imageId

    # Pychrm only takes a tiff, so write an OME-TIFF to a temporary file
    with NamedTemporaryFile(suffix='.tif', delete=False) as tmpf:
        tmpf.write(im.exportOmeTiff())

    # Calculate features for an image, override the temporary filename
    ft = Signatures.SmallFeatureSet(tmpf.name)
    ft.source_path = im.getName()
    tmpf.unlink(tmpf.name)

    # Save the features to a table
    if not tid:
        FeatureHandler.createTable(tc, ft.names)
        message += 'Created new table\n'
        message += addFileAnnotationToDataset(tc, tc.table, ds)

    FeatureHandler.saveFeatures(tc, imageId, ft)
    return message + 'Extracted features from Image id:%d\n' % imageId


def processImages(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    ids = scriptParams['IDs']
    contextName = scriptParams['Context_Name']
    newOnly = scriptParams['New_Images_Only']

    tableName = '/Pychrm/' + contextName + '/SmallFeatureSet.h5'
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
            for image in d.listChildren():
                message += 'Processing image id:%d\n' % image.getId()
                msg = extractFeatures(tc, d, newOnly, im=image)
                message += msg + '\n'

    except:
        print message
        raise
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
            'Context_Name', optional=False, grouping='2',
            description='The name of the classification context.',
            default='Example'),

        scripts.Bool(
            'New_Images_Only', optional=False, grouping='3',
            description='If features already exist for an image do not recalculate.',
            default=True),

        scripts.Long(
            'Number_Of_Processes', optional=False, grouping='3',
            description='Number of parallel feature calculation processes.',
            default=multiprocessing.cpu_count()),

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
        message += processImages(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


#
#
from omero import scripts
from omero.util import script_utils
import omero.model
from omero.rtypes import rstring, rlong
from omero.gateway import FileAnnotationWrapper, ImageWrapper
from datetime import datetime
from itertools import izip
from tempfile import NamedTemporaryFile


import sys, os
basedir = os.getenv('HOME') + '/work/omero-pychrm'
for p in ['/utils', '/pychrm-lib']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler
from pychrm.FeatureSet import Signatures

try:
    from PIL import Image, ImageDraw, ImageFont     # see ticket:2597
except: #pragma: nocover
    try:
        import Image, ImageDraw, ImageFont          # see ticket:2597
    except:
        raise omero.ServerError('No PIL installed')




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


def getTifs(im):
    sz = (im.getSizeX(), im.getSizeY())
    nch = im.getSizeC()
    zctlist = [(0, ch, 0) for ch in xrange(im.getSizeC())]
    planes = im.getPrimaryPixels().getPlanes(zctlist)
    tifs = [Image.fromarray(p) for p in planes]

    tmpfs = []
    for tif in tifs:
        with NamedTemporaryFile(suffix='.tif', delete=False) as tmpf:
            tif.save(tmpf.name)
        tmpfs.append(tmpf)

    return tmpfs


def extractFeatures(tc, ds, newOnly, chNames, imageId = None, im = None):
    message = ''

    # dataset must be explicitly provided because an image can be linked to
    # multiple datasets in which case im.getDataset() doesn't work
    if not im:
        if not imageId:
            #raise Exception('No input image')
            raise omero.ServerError('No input image')

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

    # Pychrm only takes tifs
    tmpfs = getTifs(im)

    # Calculate features for an image channel
    # Override the temporary filename
    # Prepend the channel label to each feature name and combine
    ftall = None
    for tmpf, ch in izip(tmpfs, chNames):
        ft = Signatures.SmallFeatureSet(tmpf.name)
        ft.names = ['[%s] %s' % (ch, n) for n in ft.names]
        ft.source_path = im.getName()
        tmpf.unlink(tmpf.name)
        if not ftall:
            ftall = ft
        else:
            ftall.names += ft.names
            ftall.values += ft.values

    # Save the features to a table
    if not tid:
        FeatureHandler.createTable(tc, ftall.names)
        message += 'Created new table\n'
        message += addFileAnnotationToDataset(tc, tc.table, ds)

    FeatureHandler.saveFeatures(tc, imageId, ftall)
    return message + 'Extracted features from Image id:%d\n' % imageId


def checkChannels(datasets):
    message = ''
    channels = None
    imref = None
    good = False

    for d in datasets:
        message += 'Checking channels in dataset id:%d\n' % d.getId()
        for image in d.listChildren():
            ch = [c.getLabel() for c in image.getChannels()]
            if imref is None:
                channels = ch
                imref = image.getId()
                message += 'Got channels %s, image id:%d\n' % (channels, imref)
                good = True
            elif channels != ch:
                message += 'Expected channels %s, image id:%d has %s\n' % \
                    (channels, image.getId(), ch)
                good = False

    return good, channels, message


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

        datasets = list(tc.conn.getObjects(dataType, ids))

        good, chNames, msg = checkChannels(datasets)
        message += msg
        if not good:
            raise omero.ServerError(
                'Channel check failed, ' +
                'all images must have the same channels: %s' % message)

        for d in datasets:
            message += 'Processing dataset id:%d\n' % d.getId()
            for image in d.listChildren():
                message += 'Processing image id:%d\n' % image.getId()
                msg = extractFeatures(tc, d, newOnly, chNames, im=image)
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
        'Pycharm_Feature_Extraction_Multichannel.py',
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


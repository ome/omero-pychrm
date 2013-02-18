#
#
from omero import scripts
from omero.util import script_utils
import omero.model
from omero.gateway import BlitzGateway
from omero.rtypes import rstring, rlong
from datetime import datetime



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

    # Get the datasets
    conn = BlitzGateway(client_obj = client)
    objects, logMessage = script_utils.getObjects(conn, scriptParams)
    message += logMessage

    if not objects:
        return message

    datasets = conn.getObjects(dataType, ids)

    good, chNames, msg = checkChannels(datasets)
    message += msg
    if not good:
        raise omero.ServerError(
            'Channel check failed, ' +
            'all images must have the same channels: %s' % message)

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'Pycharm_Multichannel_Check.py',
        'Check all images in selected datasetes have the same channel names',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The data you want to work with.',
                       values=[rstring('Dataset')], default='Dataset'),

        scripts.List(
            'IDs', optional=False, grouping='1',
            description='List of Dataset IDs or Image IDs').ofType(rlong(0)),

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


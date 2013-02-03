#
#
from omero import scripts
from omero.util import script_utils
from omero.rtypes import rstring, rlong
from omero.gateway import BlitzGateway
from datetime import datetime

import sys, os
basedir = os.getenv('HOME') + '/work'
for p in ['/omero-pychrm/utils']:
    if basedir + p not in sys.path:
        sys.path.append(basedir + p)
import FeatureHandler


def removeAnnotations(obj):
    namespace = FeatureHandler.NAMESPACE
    obj.removeAnnotations(namespace)
    return 'Removed annotations from %s id:%d\n' % \
        (obj.OMERO_CLASS, obj.getId())


def processObjects(client, scriptParams):
    message = ''

    # for params with default values, we can get the value directly
    dataType = scriptParams['Data_Type']
    ids = scriptParams['IDs']

    # Get the images or datasets
    conn = BlitzGateway(client_obj=client)
    objects, logMessage = script_utils.getObjects(conn, scriptParams)
    message += logMessage
    if not objects:
        return None, message

    for o in objects:
        message += removeAnnotations(o)
        if dataType == 'Dataset':
            for im in o.listChildren():
                message += removeAnnotations(im)

    return message


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    dataTypes = [rstring('Dataset'),rstring('Image')]
    client = scripts.client(
        'PycharmRemoveAnnotations.py',
        'Remove Pychrm annotations from Datasets and/or Images',

        scripts.String('Data_Type', optional=False, grouping='1',
                       description='The data you want to work with.',
                       values=dataTypes, default='Image'),

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
        scriptParams = {}

        # process the list of args above.
        for key in client.getInputKeys():
            if client.getInput(key):
                scriptParams[key] = client.getInput(key, unwrap=True)
        message = str(scriptParams) + '\n'

        # Run the script
        message += processObjects(client, scriptParams) + '\n'

        stopTime = datetime.now()
        message += 'Duration: %s' % str(stopTime - startTime)

        print message
        client.setOutput('Message', rstring(message))

    finally:
        client.closeSession()

if __name__ == '__main__':
    runScript()


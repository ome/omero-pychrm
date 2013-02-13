#
#
from omero import scripts
from omero.rtypes import rstring
from datetime import datetime
from tempfile import NamedTemporaryFile
from time import sleep


def process(client, scriptParams):
    # for params with default values, we can get the value directly
    prefix = scriptParams['File_Prefix']
    interval = scriptParams['Log_Interval']
    numMsgs = scriptParams['Number_Of_Messages']

    valid = frozenset(
        'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-')
    for p in prefix:
        if p not in valid:
            raise Exception('Invalid character in prefix')
    dstr = datetime.strftime(datetime.utcnow(),'%Y%m%dT%H%M%SZ')
    prefix += '-' + dstr + '-'

    n = 0
    with NamedTemporaryFile(
        prefix=prefix, suffix='.txt', delete=False, bufsize=1) as tmpf:
        while n < numMsgs or numMsgs == 0:
            dstr = datetime.strftime(datetime.utcnow(),'%Y-%m-%d %H:%M:%S Z')
            tmpf.write('%d: %s\n' % (n, dstr))
            sleep(interval)
            n += 1

    return 'Output file: %s' % tmpf.name


def runScript():
    """
    The main entry point of the script, as called by the client via the scripting service, passing the required parameters. 
    """

    client = scripts.client(
        'Long_Running_Script_Test.py',
        'Writes messages at intervals to a test file ' + \
            '/<tempdir>/<File_Prefix>-<Date_Time>-<XXXX>.txt',

        scripts.String('File_Prefix', optional=False, grouping='1',
                       description='The filename prefix, [A-Za-z0-9_-].',
                       default='Test'),

        scripts.Long(
            'Log_Interval', optional=False, grouping='2', min=0,
            description='Sleep for this period between messages (seconds).',
            default=60),

        scripts.Long(
            'Number_Of_Messages', optional=False, grouping='2', min=0,
            description='Number of messages, 0 for infinite.',
            default=10),

        version = '0.0.1',
        authors = ['Simon Li', 'OME Team'],
        institutions = ['University of Dundee'],
        contact = 'ome-users@lists.openmicroscopy.org.uk',
    )

    try:
        startTime = datetime.now()
        session = client.getSession()
        #client.enableKeepAlive(60)
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


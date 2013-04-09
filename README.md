omero-pychrm
============

Scripts for using Pychrm in `OMERO http://www.openmicroscopy.org/`.

This requires `PyCHRM http://code.google.com/p/wnd-charm/wiki/InstallingPyChrm`,
the `WND-CHRM https://code.google.com/p/wnd-charm/` Python wrapper, to be
installed, for example:

    pip install svn+http://wnd-charm.googlecode.com/svn/pychrm/trunk/@675#egg=pychrm
    pip install git+git://github.com/manics/omero-pychrm.git@cleanup_10583#egg=OmeroPychrm

Note: in theory running just the second command should automatically pull in
Pychrm as a dependency, however at present this installs Pychrm in editable
mode. Running the first command on its own doesn't have this problem.

Note: The above version of pychrm doesn't work with OmeroPychrm due to recent
changes.


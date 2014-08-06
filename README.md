omero-pychrm
============

Scripts for using Pychrm in `OMERO http://www.openmicroscopy.org/`.

This requires `PyCHRM https://github.com/wnd-charm/wnd-charm/` (part of the
WND-CHRM package) to be installed. You may wish to create and enter a
virtualenv before doing so. At present it is not possible to install Pychrm
using pip, so you should build from source:

    git clone https://github.com/wnd-charm/wnd-charm.git
    cd wnd-charm
    python setup.py build
    python setup.py install

Note due to a `bug https://github.com/wnd-charm/wnd-charm/issues/11` setup.py
must be run twice.

Although OmeroPychrm can be installed using pip:

    pip install git+git://github.com/ome/omero-wndcharm.git

The scripts for using WND-CHRM in OMERO are also in the same Github source
repository so you still need to clone it:

    git clone https://github.com/ome/omero-wndcharm.git
    cd omero-wndcharm
    python setup.py install

Copy the scripts into the OMERO.server script directory:

    cp -r scripts OMERO_SERVER/lib/scripts/wnd-charm

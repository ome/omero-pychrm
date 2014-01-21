omero-pychrm
============

Scripts for using Pychrm in `OMERO http://www.openmicroscopy.org/`.

This requires `PyCHRM https://github.com/wnd-charm/wnd-charm/` (part of the
WND-CHRM package) to be installed. You may wish to create and enter a
virtualenv before doing so. At present it is not possible to install Pychrm
using pip, so you should build from source:

    git clone https://github.com/wnd-charm/wnd-charm.git
    cd wnd-charm
    python setup.py install

Although OmeroPychrm can be installed using pip:

    pip install git+git://github.com/manics/omero-pychrm.git

The scripts for using WND-CHRM in OMERO are also in the same Github source
repository so you still need to clone it:

    git clone https://github.com/manics/omero-pychrm.git
    cd omero-pychrm
    python setup.py install

Copy the scripts into the OMERO.server script directory:

    cp -r scripts OMERO_SERVER/lib/scripts/wnd-charm

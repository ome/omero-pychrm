omero-wndcharm
==============

Scripts for using [WND-CHARM](https://github.com/wnd-charm/wnd-charm/) in
[OMERO](http://www.openmicroscopy.org/).

This requires the WND-CHARM Python API to be installed. You may wish to
create and enter a virtualenv before doing so. WND-CHARM has several
dependencies as described in the [WND-CHARM README]
(https://github.com/wnd-charm/wnd-charm/blob/master/README.md#supported-platforms).
Once these requirements are satisfied WND-CHARM can be installed from
[source](https://github.com/wnd-charm/wnd-charm/), or using pip:

    pip install git+https://github.com/wnd-charm/wnd-charm.git

Although [OmeroWndcharm](https://github.com/ome/omero-wndcharm/) can also be
installed using pip:

    pip install git+https://github.com/ome/omero-wndcharm.git

The scripts for using WND-CHARM in OMERO are also in the same Github source
repository so you still need to clone it:

    git clone https://github.com/ome/omero-wndcharm.git
    cd omero-wndcharm
    # Only run this if you didn't install using pip:
    python setup.py install

Copy the scripts into the OMERO.server script directory:

    cp -r scripts OMERO.server/lib/scripts/wnd-charm

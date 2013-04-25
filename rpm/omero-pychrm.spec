
Summary: Scripts for using PyCHRM in OMERO
Name: omero-pychrm
Version: 0.1.0
Release: 2
Source0: %{name}-%{version}.tar.gz
License: LICENSE.txt
Group: Development/Libraries
BuildArch: noarch
Vendor: Simon Li <spli@dundee.ac.uk>
Url: https://github.com/manics/omero-pychrm


BuildRequires: python-setuptools >= 0.6
Requires: pychrm = 0.1.0

%global omerodir /opt/omero


%description
Scripts for using PyCHRM in `OMERO http://www.openmicroscopy.org/`.
This requires PyCHRM, the WND-CHRM https://code.google.com/p/wnd-charm/
Python wrapper, to be installed.


%package scripts
Summary: OMERO server scripts

Requires: omero-pychrm = %{version}-%{release}
Requires: omero-server >= 4.4.6

%description scripts
Scripts for running PyCHRM on OMERO.server.


%prep
%setup -n %{name}-%{version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=%{buildroot} --record=INSTALLED_FILES

mkdir -p %{buildroot}%{omerodir}/server/lib/scripts
cp -a scripts %{buildroot}%{omerodir}/server/lib/scripts/pychrm

%clean
rm -rf %{buildroot}



%files -f INSTALLED_FILES
%defattr(-,root,root)


%files scripts
%defattr(-,root,root)
%{omerodir}/server/lib/scripts/pychrm


%changelog

* Wed Apr 24 2013 Simon Li <spli@dundee.ac.uk> - 0.1.0-2
- Changed the OMERO server directory

* Wed Apr 10 2013 Simon Li <spli@dundee.ac.uk> - 0.1.0-1
- Initial rpm


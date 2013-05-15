
Summary: Scripts for using PyCHRM in OMERO
Name: omero-pychrm
Version: 0.1.0
Release: 2%{?dist}
Source0: %{name}-%{version}.tar.gz
License: LICENSE.txt
Group: Development/Libraries
BuildArch: noarch
Vendor: Simon Li <spli@dundee.ac.uk>
Url: https://github.com/manics/omero-pychrm


BuildRequires: python-setuptools >= 0.6
Requires: pychrm = 0.1.0
Requires: omero-server >= 4.4.7

%global omerodir /opt/omero


%description
Scripts for using PyCHRM in `OMERO http://www.openmicroscopy.org/`.
This requires PyCHRM, the WND-CHRM https://code.google.com/p/wnd-charm/
Python wrapper, to be installed.


%prep
%setup -n %{name}-%{version}

%build
python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=%{buildroot}

mkdir -p %{buildroot}%{omerodir}/server/lib/scripts
cp -a scripts %{buildroot}%{omerodir}/server/lib/scripts/pychrm

%clean
rm -rf %{buildroot}



%files
%defattr(-,root,root)
%{python_sitelib}/OmeroPychrm*
%{omerodir}/server/lib/scripts/pychrm


%changelog

* Thu Apr 25 2013 Simon Li <spli@dundee.ac.uk> - 0.1.0-2
- Remove separate scripts package

* Wed Apr 10 2013 Simon Li <spli@dundee.ac.uk> - 0.1.0-1
- Initial rpm


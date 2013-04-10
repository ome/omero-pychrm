
Summary: Python bindings for wnd-charm
Name: pychrm
Version: 0.1.0
Release: 1
Source0: %{name}-%{version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
Vendor: Chris Coletta, Ilya Goldberg <UNKNOWN>
Url: http://code.google.com/p/wnd-charm

Patch1: pychrm-r558-demo_feb_2013.patch

BuildRequires: fftw-devel >= 3.2.0
BuildRequires: libtiff-devel >= 3.9.0


%description
WND-CHARM is a multi-purpose image classifier that can be applied to a wide variety of image classification tasks without modifications or fine-tuning, and yet provides classification accuracy comparable to state-of-the-art task-specific image classifiers. Wndchrm can extract up to ~3,000 generic image descriptors (features) including polynomial decompositions, high contrast features, pixel statistics, and textures. These features are derived from the raw image, transforms of the image, and compound transforms of the image (transforms of transforms). The features are filtered and weighted depending on their effectiveness in discriminating between a set of pre-defined image classes (the training set). These features are then used to classify test images based on their similarity to the training classes. This classifier was tested on a wide variety of imaging problems including biological and medical image classification using several imaging modalities, face recognition, and other pattern recognition tasks. WND-CHARM is an acronym that stands for weighted neighbor distance using compound hierarchy of algorithms representing morphology.

%prep
%setup
%patch -p1 -P 1

%build
env CFLAGS="$RPM_OPT_FLAGS" python setup.py build

%install
python setup.py install --single-version-externally-managed -O1 --root=%{buildroot} --record=INSTALLED_FILES

%clean
rm -rf %{buildroot}


%files -f INSTALLED_FILES
%defattr(-,root,root)


%changelog

* Wed Apr 10 2013 Simon Li <spli@dundee.ac.uk> - 0.1.0-1
- Initial rpm

#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Copyright (C) 2013 University of Dundee & Open Microscopy Environment.
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

# Handle saving and loading of features and classes between Pychrm and
# OMERO.tables
#
# This has now expanded to do a lot more, and should be split up/renamed

from itertools import izip, chain
from TableConnection import FeatureTableConnection, TableConnectionError
from TableConnection import TableConnection, Connection
import omero
from omero.rtypes import wrap, unwrap


######################################################################
# Constants for OMERO
######################################################################
CLASSIFIER_PARENT_NAMESPACE = '/classifier'
CLASSIFIER_LABEL_NAMESPACE = '/label'

PYCHRM_NAMESPACE = '/testing/pychrm'
CLASSIFIER_PYCHRM_NAMESPACE = CLASSIFIER_PARENT_NAMESPACE + PYCHRM_NAMESPACE
PYCHRM_VERSION_NAMESPACE = PYCHRM_NAMESPACE + '/version'

SMALLFEATURES_TABLE = '/SmallFeatureSet.h5'

CLASS_FEATURES_TABLE = '/ClassFeatures.h5'
CLASS_WEIGHTS_TABLE = '/Weights.h5'
CLASS_LABELS_TABLE = '/ClassLabels.h5'


######################################################################
# Feature handling
######################################################################

# Maximum number of rows to read/write in one go
CHUNK_SIZE = 100

class PychrmStorageError(Exception):
    """
    Errors occuring in the PychrmStorage module
    """
    pass


def parseFeatureName(name):
    """
    Convert a single value feature name in the form
    'ABC ... [NN]'
    to a feature group name and size in the form
    'ABC ...', NN
    """
    ft, idx = name.split(' [')
    idx = int(idx[:-1])
    return (ft, idx)


def createFeatureName(ft, idx):
    """
    The inverse of parseFeatureName
    """
    name = '%s [%d]' % (ft, idx)
    return name


def featureSizes(names):
    """
    Convert a list of single value feature names to a dictionary of
    feature group names and sizes
    """
    featSizes = {}
    for name in names:
        ft, idx = parseFeatureName(name)
        if ft in featSizes:
            featSizes[ft] = max(featSizes[ft], idx)
        else:
            featSizes[ft] = idx

    # Indexing starts at 0, so add one to sizes
    for ft in featSizes:
        featSizes[ft] += 1
    return featSizes


class FeatureTable(object):
    #def connFeatureTable(client, tableName):
    def __init__(self, client, tableName):
        self.tc = FeatureTableConnection(client=client, tableName=tableName)

    def close(self):
        self.tc.close(False)

    @property
    def conn(self):
        return self.tc.conn


    def createTable(self, featureNames):
        """
        Initialise an OMERO.table for storing features
        @param featureNames Either a mapping of feature names to feature sizes,
        or a list of single value feature names which can be parsed using
        parseFeatureName
        """
        # Unparsed list or dict?
        if hasattr(featureNames, 'keys'):
            features = featureNames
        else:
            features = featureSizes(featureNames)

        colNames = sorted(features.keys())
        desc = [(name, features[name]) for name in colNames]
        self.tc.createNewTable('id', desc)


    def openTable(self, tableId):
        try:
            self.tc.openTable(tableId)
            return True
        except TableConnectionError as e:
            print "No table found: %s" % e
            return False


    def isTableCompatible(self, features):
        """
        Check whether an existing table is compatible with this set of features,
        that is whether suitable columsn exist
        @return true if this set of features can be stored in this table
        """
        cols = self.tc.getHeaders()
        colMap = dict([(c.name, c) for c in cols])
        featSizes = featureSizes(features.names)

        for (ft, sz) in featSizes.iteritems():
            if (ft not in colMap or colMap[ft].size != sz):
                print '%s [%d] is incompatible' %  (ft, sz)
                return False
        return True


    def tableContainsId(self, id):
        """
        Check whether this ID is already present in the table
        """
        return self.tc.getRowId(id) is not None


    def saveFeatures(self, id, features):
        """
        Save the features to a table
        @param features an object with field names holding a list of single
        value feature names, and values holding a list of doubles corresponding
        to names
        """
        cols = self.tc.getHeaders()
        colMap = dict([(c.name, c) for c in cols])
        cols[0].values = [id]

        for (name, value) in izip(features.names, features.values):
            ft, idx = parseFeatureName(name)
            col = colMap[ft]
            if not col.values:
                col.values = [[float('nan')] * col.size]
            col.values[0][idx] = value

        self.tc.addData(cols)


    def loadFeatures(self, id):
        """
        Load features for an object from a table
        @return a (names, values) tuple where names is a list of single value
        features and value are the corresponding feature values
        """
        r = self.tc.getRowId(id)
        # Skip the first id column
        colNumbers = range(1, len(self.tc.getHeaders()))
        cols = self.tc.readArray(colNumbers, r, r + 1)
        names = []
        values = []
        for col in cols:
            names.extend([createFeatureName(col.name, x)
                          for x in xrange(col.size)])
            values.extend(col.values[0])

        return (names, values)


    def bulkLoadFeatures(self):
        """
        Load features for all objects in a table
        @return a (names, values, ids) tuple where names is a list of single
        value features, values is a list of lists of the corresponding feature
        values and ids is a list of object IDs.
        In other words values[i] is the list of feature values corresponding to
        object with ID given by ids[i].
        """
        colNumbers = range(len(self.tc.getHeaders()))
        nr = self.tc.getNumberOfRows()
        cols = self.tc.readArray(colNumbers, 0, nr, CHUNK_SIZE)
        names = []
        ids = cols[0].values

        for col in cols[1:]:
            names.extend([createFeatureName(col.name, x) for x in xrange(col.size)])
            values = map(lambda *args: list(chain.from_iterable(args)),
                         *[c.values for c in cols[1:]])

        return (names, values, ids)



######################################################################
# Save a classifier
######################################################################

class ClassifierTables(object):
    """
    Create a set of OMERO.tables for storing the state of a trained image
    classifier. The first table stores the training samples with reduced
    features and classes, the second stores a list of weights and feature
    names, and the third stores the class IDs and class names
    """

    def __init__(self, client, tableNameF, tableNameW, tableNameL):
        self.tcF = TableConnection(client=client, tableName=tableNameF)
        self.tcW = TableConnection(client=client, tableName=tableNameW)
        self.tcL = TableConnection(client=client, tableName=tableNameL)

    def close(self):
        self.tcF.close(False)
        self.tcW.close(False)
        self.tcL.close(False)

    def openTables(self, tidF, tidW, tidL):
        try:
            self.tcF.openTable(tidF)
            self.tcW.openTable(tidW)
            self.tcL.openTable(tidL)
            return True
        except TableConnectionError as e:
            print "Failed to open one or more tables: %s" % e
            return False


    def createClassifierTables(self, featureNames):
        schemaF = [
            omero.grid.LongColumn('id'),
            omero.grid.LongColumn('label'),
            omero.grid.DoubleArrayColumn('features', '', len(featureNames)),
            ]
        self.tcF.newTable(schemaF)

        schemaW = [
            omero.grid.StringColumn('featurename', '', 1024),
            omero.grid.DoubleColumn('weight'),
            ]
        self.tcW.newTable(schemaW)

        schemaL = [
            omero.grid.LongColumn('classID'),
            omero.grid.StringColumn('className', '', 1024),
            ]
        self.tcL.newTable(schemaL)


    def saveClassifierTables(self,
                             ids, classIds, featureMatrix,
                             featureNames, weights, classNames):
        """
        Save the classifier state (reduced features, labels and weights)
        """
        colsF = self.tcF.getHeaders()
        colsF[0].values = ids
        colsF[1].values = classIds
        colsF[2].values = featureMatrix
        self.tcF.chunkedAddData(colsF, CHUNK_SIZE)

        colsW = self.tcW.getHeaders()
        colsW[0].values = featureNames
        colsW[1].values = weights
        self.tcW.chunkedAddData(colsW, CHUNK_SIZE)

        colsL = self.tcL.getHeaders()
        colsL[0].values = range(len(classNames))
        colsL[1].values = classNames
        self.tcL.chunkedAddData(colsL, CHUNK_SIZE)


    def loadClassifierTables(self):
        """
        Load the classifier state (reduced features, labels and weights)
        """
        dF = self.tcF.chunkedRead(
            range(len(self.tcF.getHeaders())), 0,
            self.tcF.getNumberOfRows(), CHUNK_SIZE)
        colsF = dF.columns
        ids = colsF[0].values
        trainClassIds = colsF[1].values
        featureMatrix = colsF[2].values

        dW = self.tcW.chunkedRead(
            range(len(self.tcW.getHeaders())), 0,
            self.tcW.getNumberOfRows(), CHUNK_SIZE)
        colsW = dW.columns
        featureNames = colsW[0].values
        weights = colsW[1].values

        dL = self.tcL.chunkedRead(
            range(len(self.tcL.getHeaders())), 0,
            self.tcL.getNumberOfRows(), CHUNK_SIZE)
        colsL = dL.columns
        classIds = colsL[0].values
        classNames = colsL[1].values

        return {'ids': ids, 'trainClassIds': trainClassIds,
                'featureMatrix': featureMatrix,
                'featureNames': featureNames, 'weights': weights,
                'classIds': classIds, 'classNames': classNames}


######################################################################
# Version annotations
######################################################################

def getVersionAnnotation(conn, version):
    """
    Get the Annotation object used to represent a particular PyChrm version,
    or None if not found

    TODO: Should we filter by user, since the owner of a TagAnnotation
    could change the namespace without us knowing?
    TODO: Should we allow multiple identical version tags?
    """
    qs = conn.getQueryService()

    p = omero.sys.ParametersI()
    p.map['ns'] = wrap(PYCHRM_VERSION_NAMESPACE)
    p.map['v'] = wrap(version)
    vtag = qs.findByQuery(
        'from TagAnnotation a where a.ns=:ns and a.textValue=:v', p)

    return vtag


def createVersionAnnotation(conn, version):
    """
    Create the Annotation object used to represent a particular PyChrm version
    """
    assert(getVersionAnnotation(conn, version) is None)
    us = conn.getUpdateService()

    tag = omero.model.TagAnnotationI()
    tag.setNs(wrap(PYCHRM_VERSION_NAMESPACE))
    tag.setTextValue(wrap(version))
    tag = us.saveAndReturnObject(tag)
    return tag


def getVersion(conn, objType, objId):
    """
    Get the PyCHRM version associated with an object
    """
    obj = conn.getObject(objType, objId)
    anns = list(obj.listAnnotations(PYCHRM_VERSION_NAMESPACE))
    if len(anns) == 1:
        return anns[0]
    if not anns:
        return None

    raise PychrmStorageError(
        'Multiple versions attached to %s:%d' % (objType, objId))




######################################################################
# Annotations
######################################################################


def addFileAnnotationTo(tc, obj):
    """
    Attach a table as an annotation to an object (dataset/project) if not
    already attached
    """
    tfile = tc.table.getOriginalFile()
    oclass = obj.OMERO_CLASS

    obj = tc.conn.getObject(oclass, obj.getId())
    for a in obj.listAnnotations(PYCHRM_NAMESPACE):
        if isinstance(a, omero.gateway.FileAnnotationWrapper):
            if tfile.getId() == a._obj.getFile().getId():
                return 'Already attached'

    fa = omero.model.FileAnnotationI()
    fa.setFile(tfile)
    fa.setNs(wrap(PYCHRM_NAMESPACE))
    fa.setDescription(wrap(PYCHRM_NAMESPACE + ':' + tfile.getName().val))

    objClass = getattr(omero.model, oclass + 'I')
    linkClass = getattr(omero.model, oclass + 'AnnotationLinkI')
    annLink = linkClass()
    annLink.link(objClass(obj.getId(), False), fa)

    annLink = tc.conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached file id:%d to %s id:%d\n' % \
        (tfile.getId().getValue(), oclass, obj.getId())


def getAttachedTableFile(tc, obj):
    """
    See if this object (dataset/project) has a table file annotation
    """
    # Refresh the dataset, as the cached view might not show the latest
    # annotations
    obj = tc.conn.getObject(obj.OMERO_CLASS, obj.getId())

    for a in obj.listAnnotations(PYCHRM_NAMESPACE):
        if isinstance(a, omero.gateway.FileAnnotationWrapper):
            if tc.tableName == a.getFileName():
                return a._obj.getFile().getId().getValue()

    return None


def addCommentTo(conn, comment, objType, objId):
    """
    Add a comment to an object (dataset/project/image)
    """
    ca = omero.model.CommentAnnotationI()
    ca.setNs(wrap(PYCHRM_NAMESPACE))
    ca.setTextValue(wrap(comment))

    objClass = getattr(omero.model, objType + 'I')
    linkClass = getattr(omero.model, objType + 'AnnotationLinkI')
    annLink = linkClass()
    annLink.link(objClass(objId, False), ca)

    annLink = conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached comment to %s id:%d\n' % (objType, objId)


def addTagTo(conn, tag, objType, objId):
    """
    Add a tag to an object (dataset/project/image)
    """
    obj = conn.getObject(objType, objId)
    for a in obj.listAnnotations():
        if isinstance(a, omero.gateway.TagAnnotationWrapper) and \
                unwrap(tag.getId()) == a.getId():
            return 'Already tagged %s id:%d\n' % (objType, objId)

    objClass = getattr(omero.model, objType + 'I')
    linkClass = getattr(omero.model, objType + 'AnnotationLinkI')
    annLink = linkClass()
    annLink.link(objClass(objId, False), tag)

    annLink = conn.getUpdateService().saveAndReturnObject(annLink)
    return 'Attached tag to %s id:%d\n' % (objType, objId)


def createClassifierTagSet(conn, classifierName, instanceName, labels,
                           project = None):
    """
    Create a tagset and labels associated with an instance of a classifier
    """
    us = conn.getUpdateService()

    tagSet = omero.model.TagAnnotationI()
    instanceNs = classifierName + '/' + instanceName
    tagSet.setTextValue(wrap(instanceNs));
    tagSet.setNs(wrap(omero.constants.metadata.NSINSIGHTTAGSET));
    tagSet.setDescription(wrap('Classification labels for ' + instanceNs))
    tagSetR = us.saveAndReturnObject(tagSet);
    tagSetR.unload()

    for lb in labels:
        tag = omero.model.TagAnnotationI()
        tag.setTextValue(wrap(lb));
        tag.setNs(wrap(instanceNs));
        tagR = us.saveAndReturnObject(tag);

        link = omero.model.AnnotationAnnotationLinkI()
        link.setChild(tagR)
        link.setParent(tagSetR)
        linkR = us.saveAndReturnObject(link)
        assert(linkR)

    if project:
        annLink = omero.model.ProjectAnnotationLinkI()
        annLink.link(omero.model.ProjectI(project.getId(), False), tagSetR)
        us.saveAndReturnObject(annLink);

    return instanceNs


def getClassifierTagSet(classifierName, instanceName, project):
    ns = classifierName + '/' + instanceName
    for ann in project.listAnnotations():
        if ann.getNs() == omero.constants.metadata.NSINSIGHTTAGSET and \
                ann.getValue() == ns and \
                isinstance(ann, omero.gateway.TagAnnotationWrapper):
            return ann

    return None


######################################################################
# Fetching objects
######################################################################
def datasetGenerator(conn, dataType, ids):
    if dataType == 'Project':
        projects = conn.getObjects(dataType, ids)
        for p in projects:
            datasets = p.listChildren()
            for d in datasets:
                yield d
    else:
        datasets = conn.getObjects(dataType, ids)
        for d in datasets:
            yield d


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

#
#

import xml.dom.minidom
try:
    from xml.etree.ElementTree import XML, Element#, SubElement, Comment, ElementTree, tostring
except ImportError:
    from elementtree.ElementTree import XML, Element#, SubElement, Comment, ElementTree, tostring


class ClassifierXmlInvalid(Exception):
    pass

class Algorithm(object):
    def __init__(self, id, parameters):
        self.id = id
        self.parameters = parameters

class Image(object):
    def __init__(self, id, z, c, t):
        self.id = id
        self.z = z
        self.c = c
        self.t = t

class Prediction(object):
    def __init__(self, id, z, c, t, label):
        self.id = id
        self.z = z
        self.c = c
        self.t = t
        self.label = label

class FeatureSet(object):
    def __init__(self, algorithm, tableId, images):
        self.algorithm = algorithm
        self.tableId = tableId
        self.images = images

class ClassifierInstance(object):
    def __init__(self, algorithm, trainingIds, selectedId, weightsId, labels):
        self.algorithm = algorithm
        self.trainingIds = trainingIds
        self.selectedId = selectedId
        self.weightsId = weightsId
        self.labels = labels

class ClassifierPrediction(object):
    def __init__(self, algorithm, predictions):
        self.algorithm = algorithm
        self.predictions = predictions





class Reader(object):

    def __init__(self, xmlText=None, xmlFile=None):
        if xmlFile:
            if xmlText:
                raise Exception(
                    'Only one of xmlText or xmlFile should be provided')
            with open(xmlFile) as f:
                xmlText = f.read()

        self.xml = XML(xmlText)
        self.ns = self.getNs()

    def getNs(self):
        tag = self.xml.tag
        a = tag.find('{')
        b = tag.find('}')
        if a == -1:
            assert(b == -1)
            return None

        assert(a == 0 and b > 0)
        return tag[(a + 1):b]

    def preNs(self, tag):
        return '{%s}%s' % (self.ns, tag)

    def getFeatureSet(self, xml):
        """
        Look for FeatureSet elements
        """
        xs = xml.findall('.//' + self.preNs('FeatureSet'))
        fs = [self.parseFeatureSet(x) for x in xs]
        return fs

    def getClassifierInstance(self, xml):
        """
        Look for ClassifierInstance elements
        """
        xs = xml.findall('.//' + self.preNs('ClassifierInstance'))
        ci = [self.parseClassifierInstance(x) for x in xs]
        return ci

    def getClassifierPrediction(self, xml):
        """
        Look for ClassifierPrediction elements
        """
        xs = xml.findall('.//' + self.preNs('ClassifierPrediction'))
        cp = [self.parseClassifierPrediction(x) for x in xs]
        return cp

    ######################################################################

    def attribNotNone(self, xml, name):
        """
        Get an attribute from an element, raise an exception if not present
        """
        a = xml.get(name)
        if a is None:
            raise ClassifierXmlInvalid('Attribute %s not found in <%s>' % (
                    a, xml.tag))
        return a

    def parseParameter(self, xml):
        if len(xml) != 0:
            raise ClassifierXmlInvalid('Unexpected children in <%s>' % xml.tag)

        name = self.attribNotNone(xml, 'name')
        value = self.attribNotNone(xml, 'value')
        return (name, value)

    def parseAlgorithm(self, xml):
        id = self.attribNotNone(xml, 'scriptId')
        params = {}
        for x in xml:
            if x.tag != self.preNs('Parameter'):
                raise ClassifierXmlInvalid(
                    'Expected <Parameter>, found <%s>' % x.tag)
            name, value = self.parseParameter(x)
            if name in params:
                raise ClassifierXmlInvalid(
                    'Duplicate parameter name: %s' % name)
            params[name] = value

        return Algorithm(id, params)

    def parseTable(self, xml):
        originalFileId = self.attribNotNone(xml, 'originalFileId')
        if len(xml) != 0:
            raise ClassifierXmlInvalid('Unexpected children in <%s>' % xml.tag)

        return originalFileId

    def parseImage(self, xml):
        id = self.attribNotNone(xml, 'id')
        z = xml.get('z')
        c = xml.get('c')
        if c is not None:
            c = map(int, c.split(','))
        t = xml.get('t')

        if len(xml) != 0:
            raise ClassifierXmlInvalid('Unexpected children in <%s>' % xml.tag)

        return Image(id, z, c, t)

    def parseAnnotation(self, xml):
        annotationId = self.attribNotNone(xml, 'annotationId')
        if len(xml) != 0:
            raise ClassifierXmlInvalid('Unexpected children in <%s>' % xml.tag)

        return annotationId

    def parseClassLabel(self, xml):
        index = self.attribNotNone(xml, 'index')
        label = self.parseLabel(xml)
        return (index, label)

    def parseLabel(self, xml):
        if not xml.text:
            raise ClassifierXmlInvalid(
                'Expected text content in <%s>' % xml.tag)
        if len(xml) != 0:
            raise ClassifierXmlInvalid('Unexpected children in <%s>' % xml.tag)

        return xml.text

    def parsePrediction(self, xml):
        label = None

        id = self.attribNotNone(xml, 'imageId')
        z = xml.get('z')
        c = xml.get('c')
        if c is not None:
            c = map(int, c.split(','))
        t = xml.get('t')

        for x in xml:
            if x.tag == self.preNs('Label'):
                if label:
                    raise ClassifierXmlInvalid(
                        'Multiple <Label> found in <%s>' % xml.tag)
                label = self.parseLabel(x)

            else:
                # @todo Ignore additional elements instead of throwing?
                raise ClassifierXmlInvalid(
                    'Unexpected element <%s> found in <%s>' % (x.tag, xml.tag))

        return Prediction(id, z, c, t, label)

    ######################################################################

    def parseFeatureSet(self, xml):
        algorithm = None
        tableId = None
        images = []

        for x in xml:
            if x.tag == self.preNs('Algorithm'):
                if algorithm:
                    raise ClassifierXmlInvalid(
                        'Multiple <Algorithm> found in <%s>' % xml.tag)
                algorithm = self.parseAlgorithm(x)

            elif x.tag == self.preNs('FeatureTable'):
                if tableId is not None:
                    raise ClassifierXmlInvalid(
                        'Multiple <FeatureTable> found in <%s>' %
                        xml.tag)
                tableId = self.parseTable(x)

            elif x.tag == self.preNs('Image'):
                images.append(self.parseImage(x))

            else:
                raise ClassifierXmlInvalid(
                    'Unexpected element <%s> found in <%s>' % (x.tag, xml.tag))

        return FeatureSet(algorithm, tableId, images)

    def parseClassifierInstance(self, xml):
        algorithm = None
        features = []
        featuresTable = None
        weightsTable = None
        labels = {}

        for x in xml:
            if x.tag == self.preNs('Algorithm'):
                if algorithm:
                    raise ClassifierXmlInvalid(
                        'Multiple <Algorithm> found in <%s>' % xml.tag)
                algorithm = self.parseAlgorithm(x)

            elif x.tag == self.preNs('TrainingFeatures'):
                table = self.parseAnnotation(x)

            elif x.tag == self.preNs('SelectedFeaturesTable'):
                if featuresTable is not None:
                    raise ClassifierXmlInvalid(
                        'Multiple <SelectedFeaturesTable> found in <%s>' %
                        xml.tag)
                featuresTable = self.parseTable(x)

            elif x.tag == self.preNs('FeatureWeightsTable'):
                if weightsTable is not None:
                    raise ClassifierXmlInvalid(
                        'Multiple <FeatureWeightsTable> found in <%s>' %
                        xml.tag)
                weightsTable = self.parseTable(x)

            elif x.tag == self.preNs('ClassLabel'):
                index, name = self.parseClassLabel(x)
                if index in labels:
                    raise ClassifierXmlInvalid('Duplicate index: %s' % index)
                labels[index] = name

            else:
                raise ClassifierXmlInvalid(
                    'Unexpected element <%s> found in <%s>' % (x.tag, xml.tag))

        return ClassifierInstance(
            algorithm, features, featuresTable, weightsTable, labels)


    def parseClassifierPrediction(self, xml):
        algorithm = None
        predictions = []

        for x in xml:
            if x.tag == self.preNs('Algorithm'):
                if algorithm:
                    raise ClassifierXmlInvalid(
                        'Multiple <Algorithm> found in <%s>' % xml.tag)
                algorithm = self.parseAlgorithm(x)

            elif x.tag == self.preNs('Prediction'):
                predictions.append(self.parsePrediction(x))

            else:
                raise ClassifierXmlInvalid(
                    'Unexpected element <%s> found in <%s>' % (x.tag, xml.tag))

        return ClassifierPrediction(algorithm, predictions)


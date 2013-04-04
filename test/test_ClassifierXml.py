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
import unittest
import collections
import itertools

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from ClassifierXml import *


class TestReader(unittest.TestCase):

    def setUp(self):
        self.filename = os.path.join(os.path.dirname(__file__), 'sample.xml')

    def getXml(self):
        return Reader(xmlFile=self.filename)

    def test_getNs(self):
        xml = self.getXml()
        self.assertEqual(
            xml.getNs(), 'http://www.openmicroscopy.org/Schemas/OME/2012-06')

    def test_getFeatureSet(self):
        xml = self.getXml()
        fs = xml.getFeatureSet(xml.xml)
        self.assertEqual(len(fs), 1)
        fs = fs[0]

        a = fs.algorithm
        self.assertIsNotNone(a)
        self.assertEqual(a.id, 1234)
        self.assertEqual(sorted(a.parameters.keys()), ['bar', 'foo'])
        self.assertEqual(a.parameters['foo'], '345')
        self.assertEqual(a.parameters['bar'], 'abc')

        self.assertEqual(fs.tableId, 654)

        ims = fs.images
        self.assertEqual(len(ims), 3)
        for (i, im) in itertools.izip([52, 53, 54], ims):
            self.assertEqual(im.id, i)
            self.assertEqual(im.z, 0)
            self.assertEqual(im.c, [0, 1, 2])
            self.assertEqual(im.t, 0)


    def test_getClassifierInstance(self):
        xml = self.getXml()
        ci = xml.getClassifierInstance(xml.xml)
        self.assertEqual(len(ci), 1)
        ci = ci[0]

        a = ci.algorithm
        self.assertIsNotNone(a)
        self.assertEqual(a.id, 534)
        self.assertEqual(a.parameters.keys(), ['threshold'])
        self.assertEqual(a.parameters['threshold'], '0.234')

        self.assertEqual(ci.trainingIds, [4235, 6543])
        self.assertEqual(ci.selectedId, 654)
        self.assertEqual(ci.weightsId, 356)

        ls = ci.labels
        self.assertEqual(sorted(ls.keys()), [0, 1])
        self.assertEqual(ls[0], 'cat')
        self.assertEqual(ls[1], 'dog')


    def test_getClassifierPrediction(self):
        xml = self.getXml()
        cp = xml.getClassifierPrediction(xml.xml)
        self.assertEqual(len(cp), 1)
        cp = cp[0]

        a = cp.algorithm
        self.assertIsNotNone(a)
        self.assertEqual(a.id, 599)
        self.assertEqual(a.parameters.keys(), ['g'])
        self.assertEqual(a.parameters['g'], '9.81')

        ps = cp.predictions
        self.assertEqual(len(ps), 2)
        for (i, p) in itertools.izip([52, 53], ps):
            self.assertEqual(p.id, i)
            self.assertEqual(p.z, 0)
            self.assertEqual(p.c, [0])
            self.assertEqual(p.t, 0)




if __name__ == '__main__':
    unittest.main()

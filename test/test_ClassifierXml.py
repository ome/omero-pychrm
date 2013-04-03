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

    def test_getClassifierInstance(self):
        xml = self.getXml()
        fs = xml.getClassifierInstance(xml.xml)


    def test_getClassifierPrediction(self):
        xml = self.getXml()
        fs = xml.getClassifierPrediction(xml.xml)




if __name__ == '__main__':
    unittest.main()

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
import omero
from omero.rtypes import unwrap

import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

#from TableConnection import Connection, TableConnection, FeatureTableConnection
import FeatureHandler
from FeatureHandler import FeatureTable, ClassifierTables


class ClientHelper(unittest.TestCase):

    def create_client(self):
        cli = omero.client()
        sess = cli.createSession()
        return (cli, sess)

    def setUp(self):
        """
        Create a connection for creating the test tables.
        ICE_CONFIG must be set.
        """
        self.cli, self.sess = self.create_client()
        self.tableName = '/test_FeatureHandler/test.h5'

    def tearDown(self):
        self.cli.closeSession()


class TestFeatures(object):
    def __init__(self, inc = 0):
        self.names = ['a [0]', 'a [1]', 'b [0]']
        self.values = map(lambda x: x + inc, [10., 11., 12.])


class TestFeatureHandler(unittest.TestCase):

    def test_parseFeatureName(self):
        r = FeatureHandler.parseFeatureName('a b [321]')
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0], 'a b')
        self.assertEqual(r[1], 321)

    def createFeatureName(self):
        r = FeatureHandler.createFeatureName('a b', 321)
        self.assertEqual(r, 'a b [321]')

    def featureSizes(self):
        ftsz = FeatureHandler.featureSizes(['a b [12]', 'c d [3]', 'a b [14]'])
        self.assertEqual(len(ftsz.keys), 2)
        self.assertIn(ftsz, 'a b')
        self.assertIn(ftsz, 'c d')
        self.assertEqual(ftsz['a b'], 14)
        self.assertEqual(ftsz['c d'], 3)


class TestFeatureTable(ClientHelper):

    def create_table(self):
        cli, sess = self.create_client()
        ft = FeatureTable(client=cli, tableName=self.tableName)
        fts = ['a [0]', 'a [1]', 'b [0]']
        ft.createTable(fts)
        tid = ft.tc.tableId
        ft.close()
        return tid

    def create_table_with_data(self):
        cli, sess = self.create_client()
        ft = FeatureTable(client=cli, tableName=self.tableName)
        fts = ['a [0]', 'a [1]', 'b [0]']
        ft.createTable(fts)

        cols = ft.tc.getHeaders()
        cols[0].values = [7, 8]
        cols[1].values = [[1., 2.], [3., 4.]]
        cols[2].values = [[5.], [6.]]
        ft.tc.addData(cols)

        tid = ft.tc.tableId
        ft.close()
        return tid


    def test_createTable(self):
        tid = self.create_table()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        ft.openTable(tid)

        headers = ft.tc.getHeaders()
        self.assertEqual(len(headers), 3)
        self.assertEqual([h.name for h in headers], ['id', 'a', 'b'])
        self.assertEqual([h.size for h in headers[1:]], [2, 1])

    def test_openTable(self):
        self.test_createTable()

    def test_isTableCompatible(self):
        tid = self.create_table()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        t = ft.openTable(tid)

        fts = TestFeatures()
        self.assertTrue(ft.isTableCompatible(fts))
        fts.names.append('a [2]')
        fts.values.append(13.)
        self.assertFalse(ft.isTableCompatible(fts))

    def test_tableContainsId(self):
        tid = self.create_table_with_data()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        ft.openTable(tid)
        self.assertTrue(ft.tableContainsId(7))

    def test_saveFeatures(self):
        tid = self.create_table_with_data()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        ft.openTable(tid)
        fts = TestFeatures()
        ft.saveFeatures(101, fts)

        self.assertEqual(ft.tc.getNumberOfRows(), 3)
        xs = ft.tc.readArray([0, 1, 2], 0, 4, chunk=3)
        self.assertEqual(xs[0].values, [7, 8, 101])
        self.assertEqual(xs[1].values, [[1., 2.], [3., 4.], [10., 11.]])
        self.assertEqual(xs[2].values, [[5.], [6.], [12.]])

    def test_loadFeatures(self):
        tid = self.create_table_with_data()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        ft.openTable(tid)

        names, values = ft.loadFeatures(7)
        self.assertEqual(names, ['a [0]', 'a [1]', 'b [0]'])
        self.assertEqual(values, [1., 2., 5.])

        names, values = ft.loadFeatures(8)
        self.assertEqual(names, ['a [0]', 'a [1]', 'b [0]'])
        self.assertEqual(values, [3., 4., 6.])

    def test_bulkLoadFeatures(self):
        tid = self.create_table_with_data()
        ft = FeatureTable(client=self.cli, tableName=self.tableName)
        ft.openTable(tid)

        names, values, ids = ft.bulkLoadFeatures()
        self.assertEqual(names, ['a [0]', 'a [1]', 'b [0]'])
        self.assertEqual(values, [[1., 2., 5.], [3., 4., 6.]])
        self.assertEqual(ids, [7, 8])


class TestClassifierTables(ClientHelper):

    def setUp(self):
        super(TestClassifierTables, self).setUp()
        self.tableNameF = '/test_FeatureHandler/ClassFeatures.h5'
        self.tableNameW = '/test_FeatureHandler/Weights.h5'
        self.tableNameL = '/test_FeatureHandler/ClassLabels.h5'

    def create_classifierTables(self):
        cli, sess = self.create_client()
        ct = ClassifierTables(cli, self.tableNameF, self.tableNameW,
                              self.tableNameL)
        return ct


    def test_createClassifierTables(self):
        ct = self.create_classifierTables()
        fts = TestFeatures()
        ct.createClassifierTables(fts.names)

        headers = ct.tcF.getHeaders()
        self.assertEqual([h.name for h in headers],
                         ['id', 'label', 'features'])

        headers = ct.tcW.getHeaders()
        self.assertEqual([h.name for h in headers], ['featurename', 'weight'])

        headers = ct.tcL.getHeaders()
        self.assertEqual([h.name for h in headers], ['classID', 'className'])

    def test_saveClassifierTables(self):
        ct = self.create_classifierTables()
        fts0 = TestFeatures()
        fts1 = TestFeatures(10)
        ct.createClassifierTables(fts0.names)

        ids = [7, 8]
        classIds = [1, 0]
        featureMatrix = [fts0.values, fts1.values]
        featureNames = fts0.names
        weights = [0.125, 0.375, 0.5]
        classNames = ['Cat', 'Hedgehog']
        ct.saveClassifierTables(ids, classIds, featureMatrix,
                                featureNames, weights, classNames)

        self.assertEqual(ct.tcF.getNumberOfRows(), 2)
        self.assertEqual(ct.tcW.getNumberOfRows(), 3)
        self.assertEqual(ct.tcL.getNumberOfRows(), 2)

        d = ct.tcF.table.readCoordinates([0, 1])
        self.assertEqual(d.columns[0].values, [7, 8])
        self.assertEqual(d.columns[1].values, [1, 0])
        self.assertEqual(d.columns[2].values,
                         [[10., 11., 12.], [20., 21., 22.]])

        d = ct.tcW.table.readCoordinates([0, 1, 2])
        self.assertEqual(d.columns[0].values, ['a [0]', 'a [1]', 'b [0]'])
        self.assertEqual(d.columns[1].values, [0.125, 0.375, 0.5])

        d = ct.tcL.table.readCoordinates([0, 1])
        self.assertEqual(d.columns[0].values, [0, 1])
        self.assertEqual(d.columns[1].values, ['Cat', 'Hedgehog'])

    def test_loadClassifierTables(self):
        ct = self.create_classifierTables()
        fts0 = TestFeatures()
        fts1 = TestFeatures(10)
        ct.createClassifierTables(fts0.names)

        ids = [7, 8]
        classIds = [1, 0]
        featureMatrix = [fts0.values, fts1.values]
        featureNames = fts0.names
        weights = [0.125, 0.375, 0.5]
        classNames = ['Cat', 'Hedgehog']
        ct.saveClassifierTables(ids, classIds, featureMatrix,
                                featureNames, weights, classNames)

        data = ct.loadClassifierTables()

        self.assertEqual(data['ids'], [7, 8])
        self.assertEqual(data['trainClassIds'], [1, 0])
        self.assertEqual(data['featureMatrix'],
                         [[10., 11., 12.], [20., 21., 22.]])
        self.assertEqual(data['featureNames'], ['a [0]', 'a [1]', 'b [0]'])
        self.assertEqual(data['weights'], [0.125, 0.375, 0.5])
        self.assertEqual(data['classIds'], [0, 1])
        self.assertEqual(data['classNames'], ['Cat', 'Hedgehog'])


@unittest.skip("TODO: Implement")
class TestAnnotations(ClientHelper):

    def test_addFileAnnotationTo(self):
        FeatureHandler.addFileAnnotationTo(tc, obj)

    def test_getAttachedTableFile(self):
        FeatureHandler.getAttachedTableFile(tc, obj)

    def test_addCommentTo(self):
        FeatureHandler.addCommentTo(conn, comment, objType, objId)

    def test_addTagTo(self):
        FeatureHandler.addTagTo(conn, tag, objType, objId)

    def test_createClassifierTagSet(self):
        FeatureHandler.createClassifierTagSet(
            conn, classifierName, instanceName, labels, project)

    def test_getClassifierTagSet(self):
        FeatureHandler.getClassifierTagSet(
            classifierName, instanceName, project)

    def test_datasetGenerator(self):
        FeatureHandler.datasetGenerator(conn, dataType, ids)



if __name__ == '__main__':
    unittest.main()

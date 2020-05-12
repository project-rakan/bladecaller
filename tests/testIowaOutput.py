import json
import unittest
import os

with open('tests/expected/iowaExpected.idx.json') as f:
    data = json.load(f)

# expected data
expMagicNum = data["magic_num"]
expCheckSum = data["checkSum"]
expStCode = data["stCode"]
expNumNodes = data["numNodes"]
expNumDistricts = data["numDistricts"]
expNodeRecords = data["node_records"]
expNodes = data["nodes"]

with open('tests/expected/iowaExpected.json') as f:
    expJSON = json.load(f)
        

class testCreateOutput(unittest.TestCase):
    def testGis2Idx(self):
        self.assertEqual(os.system(f'python3.7 gis2idx iowa -readable -idx -json -districts -novert'), 0)

class testIDXOutput(unittest.TestCase):
    def testReadableIdx(self):
        location = 'output/iowa/iowa.idx.json'
        with open(location) as f:
            actual = json.load(f)
        self.assertEqual(expMagicNum, actual["magic_num"])
        self.assertEqual(expCheckSum, actual["checkSum"])
        self.assertEqual(expStCode, actual["stCode"])
        self.assertEqual(expNumNodes, actual["numNodes"])
        self.assertEqual(expNumDistricts, actual["numDistricts"])
        self.assertEqual(expNodeRecords, actual["node_records"])
        self.assertEqual(expNodes, actual["nodes"])

    def testIDXBinary(self):
        self.assertTrue(True) #TODO

class testJSONOutput(unittest.TestCase):
    def testStateJSON(self):
        location = 'output/iowa/iowa.json'
        with open(location) as f:
            actual = json.load(f)
        
        self.assertEqual(actual["state"], expStCode)
        self.assertEqual(actual["maxDistricts"], expNumDistricts)
        self.assertEqual(actual["fips"], 19)

        actPrecincts = actual["precincts"]
        expPrecincts = expJSON["precincts"]
        self.assertEqual(len(actPrecincts), len(expPrecincts), expNumNodes)
        
        for i in range(expNumNodes):
            self.assertEqual(actPrecincts[i], expPrecincts[i])

    def testNovertJSON(self):
        location = 'output/iowa/iowa.novert.json'
        with open(location) as f:
            actual = json.load(f)
        
        self.assertEqual(actual["state"], expStCode)
        self.assertEqual(actual["maxDistricts"], expNumDistricts)
        self.assertEqual(actual["fips"], 19)

        actPrecincts = actual["precincts"]
        expPrecincts = expJSON["precincts"]
        self.assertEqual(len(actPrecincts), len(expPrecincts), expNumNodes)
        
        for i in range(expNumNodes):
            self.assertEqual(actPrecincts[i]["name"], expPrecincts[i]["name"])
            self.assertEqual(actPrecincts[i]["id"], expPrecincts[i]["id"], i)
            self.assertEqual(len(actPrecincts[i]["vertices"]), 0)

    def testDistrictsJSON(self):
        location = 'output/iowa/iowa.districts.json'
        with open(location) as f:
            actual = json.load(f)
        
        self.assertEqual(actual["state"], expStCode)
        actMap = actual["map"]
        self.assertEqual(type(actMap), type(dict([])))
        self.assertEqual(len(actMap), expNumNodes)
        self.assertEqual(set(actMap.values()), set(range(expNumDistricts + 1)[1:]))

        for i in range(expNumNodes):
            self.assertTrue(actMap.__contains__(str(i)))


if __name__ == '__main__':
    unittest.main()
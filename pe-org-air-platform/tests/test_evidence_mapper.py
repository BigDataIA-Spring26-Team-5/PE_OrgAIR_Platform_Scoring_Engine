# tests/test_evidence_mapper.py
import unittest 
from app.evidence_mapper import EvidenceMapper
class TestEvidenceMapper(unittest.TestCase):
    def setUp(self):
        self.evidence_mapper = EvidenceMapper()

    def test_map_evidence(self):
        board_analysis = {
            "sentiment": "positive"
        }
        glassdoor_data = {
            "reviews": [
                {"role": "Engineer", "rating": 4},
                {"role": "Manager", "rating": 5}
            ]
        }
        expected_mapped_evidence = {
            "board_sentiment": "positive",
            "glassdoor_reviews": [
                {"role": "Engineer", "rating": 4},
                {"role": "Manager", "rating": 5}
            ]
        }
        mapped_evidence = self.evidence_mapper.map_evidence(board_analysis, glassdoor_data)
        self.assertEqual(mapped_evidence, expected_mapped_evidence)
# tests/test_rubric_scorer.py
import unittest
from app.scoring.rubric_scorer import RubricScorer
class TestRubricScorer(unittest.TestCase):
    def setUp(self):
        self.rubric_scorer = RubricScorer()

    def test_score_positive_sentiment(self):
        mapped_evidence = {
            "board_sentiment": "positive",
            "glassdoor_reviews": [
                {"role": "Engineer", "rating": 4},
                {"role": "Manager", "rating": 5}
            ]
        }
        expected_score = 5 + (2 * 2)  # 5 for positive sentiment + 2 reviews * 2 points each
        score = self.rubric_scorer.score(mapped_evidence)
        self.assertEqual(score, expected_score)

    def test_score_neutral_sentiment(self):
        mapped_evidence = {
            "board_sentiment": "neutral",
            "glassdoor_reviews": [
                {"role": "Engineer", "rating": 3}
            ]
        }
        expected_score = 0 + (1 * 2)  # 0 for neutral sentiment + 1 review * 2 points
        score = self.rubric_scorer.score(mapped_evidence)
        self.assertEqual(score, expected_score)

    def test_score_no_reviews(self):
        mapped_evidence = {
            "board_sentiment": "positive",
            "glassdoor_reviews": []
        }
        expected_score = 5 + (0 * 2)  # 5 for positive sentiment + 0 reviews
        score = self.rubric_scorer.score(mapped_evidence)
        self.assertEqual(score, expected_score)
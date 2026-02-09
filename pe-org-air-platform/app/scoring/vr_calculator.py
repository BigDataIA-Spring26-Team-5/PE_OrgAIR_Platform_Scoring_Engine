# app/scoring/vr_calculator.py
from app.scoring.utils import ScoringUtils
class VRCalculator:
    def __init__(self):
        self.scoring_utils = ScoringUtils()

    def calculate_vr_score(self, board_data, glassdoor_data):
        # Calculate various scores based on the integrated data
        scores = self.scoring_utils.calculate_scores(board_data, glassdoor_data)
        
        # Combine the scores into a final VR score
        vr_score = (scores["position_score"] * 0.4) + (scores["rubric_score"] * 0.4) + (scores["concentration_score"] * 0.2)
        
        return vr_score
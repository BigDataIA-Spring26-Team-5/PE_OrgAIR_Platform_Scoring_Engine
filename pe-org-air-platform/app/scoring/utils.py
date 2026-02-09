# app/scoring/utils.py
from app.scoring.integration_service import IntegrationService
from app.scoring.position_factor import PositionFactor
from app.scoring.rubric_scorer import RubricScorer
from app.scoring.talent_concentration import TalentConcentration
class ScoringUtils:
    def __init__(self):
        self.integration_service = IntegrationService()
        self.position_factor = PositionFactor()
        self.rubric_scorer = RubricScorer()
        self.talent_concentration = TalentConcentration()

    def calculate_scores(self, board_data, glassdoor_data):
        # Integrate data from board analysis and Glassdoor reviews
        mapped_evidence = self.integration_service.integrate_data(board_data, glassdoor_data)
        
        # Calculate position factor
        position_score = self.position_factor.calculate_position_factor(mapped_evidence)
        
        # Calculate rubric score
        rubric_score = self.rubric_scorer.score(mapped_evidence)
        
        # Calculate talent concentration score
        concentration_score = self.talent_concentration.calculate_concentration(mapped_evidence)
        
        return {
            "position_score": position_score,
            "rubric_score": rubric_score,
            "concentration_score": concentration_score
        }
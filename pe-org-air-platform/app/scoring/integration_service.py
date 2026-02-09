# app/scpring/integration_service.py
from app.board_analyzer import BoardAnalyzer
from app.evidence_mapper import EvidenceMapper
class IntegrationService:
    def __init__(self):
        self.board_analyzer = BoardAnalyzer()
        self.evidence_mapper = EvidenceMapper()

    def integrate_data(self, board_data, glassdoor_data):
        # Analyze the board data
        board_analysis = self.board_analyzer.analyze_board(board_data)
        
        # Map the evidence from board analysis and Glassdoor data
        mapped_evidence = self.evidence_mapper.map_evidence(board_analysis, glassdoor_data)
        
        return mapped_evidence

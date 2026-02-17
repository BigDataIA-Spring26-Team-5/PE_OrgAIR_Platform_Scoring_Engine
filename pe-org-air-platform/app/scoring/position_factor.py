# scoring/position_factor.py

from decimal import Decimal
from typing import Dict

class PositionFactorCalculator:
    """
    Calculate position factor for H^R.
    
    IMPORTANT: This calculator does NOT calculate VR.
    It receives VR as an input that was already calculated.
    """
    
    SECTOR_AVG_VR: Dict[str, float] = {
        "technology": 65.0,
        "financial_services": 55.0,
        "healthcare": 52.0,
        "business_services": 50.0,
        "retail": 48.0,
        "manufacturing": 45.0,
    }
    
    def calculate_position_factor(
        self,
        vr_score: float,  # ← Already calculated by VRCalculator
        sector: str,
        market_cap_percentile: float,
    ) -> Decimal:
        """
        Calculate position factor from pre-calculated V^R and market cap.
        
        Args:
            vr_score: Company's V^R score (0-100) - ALREADY CALCULATED
            sector: Company sector
            market_cap_percentile: Manual input (0-1)
        
        Returns:
            Position factor in [-1, 1]
        """
        # Validate inputs
        if not 0.0 <= market_cap_percentile <= 1.0:
            raise ValueError(f"market_cap_percentile must be in [0, 1]")
        
        if not 0 <= vr_score <= 100:
            raise ValueError(f"vr_score must be in [0, 100]")
        
        # Get sector average
        sector_avg = self.SECTOR_AVG_VR.get(sector.lower(), 50.0)
        
        # Calculate VR component
        vr_diff = vr_score - sector_avg
        vr_component = max(-1, min(1, vr_diff / 50))
        
        # Calculate market cap component
        mcap_component = (market_cap_percentile - 0.5) * 2
        
        # Weighted combination
        pf = 0.6 * vr_component + 0.4 * mcap_component
        
        # Bound to [-1, 1]
        pf_bounded = max(-1, min(1, pf))
        return Decimal(str(pf_bounded)).quantize(Decimal("0.0001"))
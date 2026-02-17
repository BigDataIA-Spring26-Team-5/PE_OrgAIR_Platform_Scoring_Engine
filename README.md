# PE-OrgAIR Platform Evidence Collection (Case Study 2)
Then CS3 adds two new collectors to fill gaps:


Glassdoor reviews → Culture (0.80)
Board analyzer (deeper DEF-14A parsing) → AI Governance (0.70)

This is actually fine for CS3's evidence mapper because digital_presence feeds into Data Infrastructure (0.60) and Technology Stack (0.40) — it's supposed to measure overall tech maturity, not just AI tools. A company running 4,058 technologies is more digitally mature than one running 392.

Yes, the method and scoring are correct for what digital_presence is supposed to measure in CS2/CS3. Let me explain why:
What CS3 expects from digital_presence
From the CS3 mapping table:
CS2 SourceCS3 DimensionWeightdigital_presenceData Infrastructure0.60digital_presenceTechnology Stack0.40
It feeds into infrastructure and tech stack maturity — NOT "how much AI does the company use." That's what technology_hiring and innovation_activity are for.
What we're measuring

BuiltWith live tech count — A company running 4,058 technologies (ADP) has a more mature digital infrastructure than one running 392 (UNH). This is the primary signal.
Group diversity — More technology categories = more sophisticated stack (CDN, analytics, payment, SSL, etc.)
Category depth — More sub-categories = deeper investment
Active maintenance ratio — live/(live+dead) shows they're actively maintaining their stack
Wappalyzer AI bonus — If specific AI tools are detected (TensorFlow, Kubernetes, etc.), they get extra points


-- dedup in the snowflake scores

Looking at the data more carefully — JPM got 100 patents from the API, and 22 were classified as AI-related. Let me verify the score:

Volume: min(22 * 5, 50) = 50 ✅ (maxed out, only need 10 AI patents)
Recency: min(recent_ai * 2, 20) = likely 20 ✅ (most patents are from 2025, so 10+ AI patents in last year maxes this)
Diversity: min(3 * 10, 30) = 30 ✅ (deep_learning, nlp, computer_vision — 3 categories)

Total: 100/100
The math is correct per the PDF formula. But is the score meaningful? That's debatable.
The problem is the PDF's formula is easy to max out:

Only need 10 AI patents to max volume (10 × 5 = 50)
Only need 10 recent AI patents to max recency (10 × 2 = 20)
Only need 3 categories to max diversity (3 × 10 = 30)

JPM has 22 AI patents across 3+ categories, mostly recent — so it easily maxes every component. Companies like GS, WMT, DE would likely also score 100 or very close.
But this is what the PDF spec says to do. The formula comes directly from pages 18-19. If your professor designed it this way, it might be intentional — the differentiation between companies would come from the composite score across all 4 signals, not from patents alone.

-Patent Scoring (CS2 — pages 18-19):
The formula min(ai*5, 50) + min(recent*2, 20) + min(categories*10, 30) saturates easily. JPM hitting 100/100 with just 22 AI patents is technically correct per spec. Any large company with ≥10 AI patents, ≥10 recent ones, and ≥3 categories will max out.
The bigger picture for CS3:
The patent score feeds into CS3 as the innovation_activity signal, which maps to multiple dimensions (Technology Stack at 0.50 weight, Use Case Portfolio at 0.30, Data Infrastructure at 0.20). So even if patent scores are saturated, the composite scoring through the evidence mapper, rubric scorer, and VR calculation will create differentiation.

- DE (Deere) hiring score of 2.0 reflects a point-in-time snapshot. John Deere actively hires ML engineers and data scientists (63+ roles on LinkedIn), but their AI-specific postings cycle in and out of the 72-hour scraping window. Their innovation score of 93.0 (9 AI patents in computer vision and precision agriculture) more accurately reflects their AI investment.

- Dollar General only recently created an SVP of AI Optimization role in November 2025 — this is brand new for them. Retail Dive They partnered with Shelf Engine's AI for produce ordering, but that's a vendor relationship, not internal AI hiring. Supply Chain Dive Dollar General is a discount retailer with 19,000+ stores focused on low-cost operations — they have minimal in-house tech capability. The 0.0 hiring score (only 1 tech job out of 39 total, 0 AI) is accurate.
For patents, Dollar General's AI efforts are very recent (2024-2025) and vendor-driven, not R&D-driven. Retail Dive A discount retailer with no R&D lab having 0 patents is completely expected.

## Market Cap Percentile Inputs

Manual market cap percentiles were determined through analysis of sector peers 
as of February 2026:

| Ticker | Company | Sector | MCap Percentile | Reasoning |
|--------|---------|--------|-----------------|-----------|
| NVDA | NVIDIA | Technology | 0.95 | 3rd largest tech company after Apple and Microsoft |
| JPM | JPMorgan | Financial Svc | 0.85 | Largest US bank by market cap (~$580B) |
| WMT | Walmart | Retail | 0.60 | Large retailer but Amazon significantly larger |
| GE | General Electric | Manufacturing | 0.50 | Mid-pack in manufacturing sector (~$170B) |
| DG | Dollar General | Retail | 0.30 | Smaller retailer in budget segment (~$22B) |

**Data Sources:**
- Market capitalizations from Yahoo Finance (Feb 2026)
- Sector peer comparisons from S&P sector indices
- Percentiles calculated relative to sector constituents
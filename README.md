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
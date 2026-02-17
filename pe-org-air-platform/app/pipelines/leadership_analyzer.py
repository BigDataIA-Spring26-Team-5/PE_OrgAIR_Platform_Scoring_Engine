# #app/pipelines/leadership_analyzer.py
# import re
# import logging
# from typing import Dict, List, Tuple, Optional
# from dataclasses import dataclass

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)-8s | %(message)s',
#     datefmt='%H:%M:%S'
# )
# logger = logging.getLogger(__name__)


# @dataclass
# class LeadershipScores:
#     """Breakdown of leadership signal scores."""
#     tech_exec_score: float  # Max 30
#     keyword_score: float  # Max 30
#     performance_metric_score: float  # Max 25
#     board_tech_score: float  # Max 15
#     total_score: float  # Max 100
    
#     # Details
#     tech_execs_found: List[str]
#     keyword_counts: Dict[str, int]
#     tech_metrics_found: List[str]
#     board_indicators: List[str]


# class LeadershipAnalyzer:
#     """Analyze DEF 14A filings for leadership signals."""
    
#     # Tech executive titles to search for
#     TECH_EXEC_TITLES = {
#         "chief technology officer": 10,
#         "cto": 10,
#         "chief digital officer": 10,
#         "cdo": 8,
#         "chief data officer": 8,
#         "chief ai officer": 12,
#         "chief artificial intelligence officer": 12,
#         "chief information officer": 6,
#         "cio": 6,
#         "chief analytics officer": 8,
#         "chief innovation officer": 8,
#         "vp of artificial intelligence": 6,
#         "vp of machine learning": 6,
#         "vp of data science": 5,
#         "svp of technology": 5,
#         "head of ai": 7,
#         "head of machine learning": 7,
#     }
    
#     # AI/Tech keywords for compensation discussion
#     TECH_KEYWORDS = {
#         "artificial intelligence": 3,
#         "machine learning": 3,
#         "ai": 2,
#         "digital transformation": 2,
#         "automation": 1.5,
#         "data analytics": 1.5,
#         "technology modernization": 1.5,
#         "cloud": 1,
#         "digital": 1,
#         "innovation": 0.5,
#         "technology": 0.5,
#     }
    
#     # Phrases indicating tech-linked performance metrics
#     TECH_PERFORMANCE_PHRASES = [
#         (r"technology\s+(?:metric|goal|objective|target|initiative)", 8),
#         (r"digital\s+transformation\s+(?:bonus|incentive|award|metric)", 8),
#         (r"(?:ai|artificial intelligence)\s+(?:initiative|deployment|implementation)", 10),
#         (r"automation\s+(?:metric|goal|savings|efficiency)", 6),
#         (r"innovation\s+(?:metric|performance|bonus)", 5),
#         (r"(?:tech|technology|digital)\s+(?:investment|spend|budget)", 4),
#         (r"data\s+(?:strategy|analytics|platform)\s+(?:goal|metric)", 5),
#     ]
    
#     # Board tech expertise indicators
#     BOARD_TECH_INDICATORS = [
#         (r"(?:google|microsoft|amazon|meta|apple|nvidia|intel|ibm|oracle|salesforce)", 5),
#         (r"technology\s+(?:executive|leader|officer|expert)", 4),
#         (r"(?:cto|cio|chief technology|chief digital)\s+(?:at|of|for)", 5),
#         (r"(?:software|tech|digital)\s+(?:company|industry|sector)\s+(?:experience|background)", 3),
#         (r"computer science|engineering degree|technical background", 2),
#     ]
    
#     def __init__(self):
#         logger.info("🎯 Leadership Analyzer initialized")
    
#     def analyze(
#         self,
#         text_content: str,
#         sections: Dict[str, str],
#         tables: List[Dict]
#     ) -> LeadershipScores:
#         """
#         Analyze DEF 14A content for leadership signals.
        
#         Returns scores out of 100:
#         - Tech Exec Presence: 30 pts
#         - AI/Tech Keywords: 30 pts
#         - Tech-Linked Metrics: 25 pts
#         - Board Tech Expertise: 15 pts
#         """
#         logger.info("  🔍 Analyzing leadership signals...")
        
#         # Combine all text for analysis
#         full_text = text_content.lower()
#         exec_comp_text = sections.get("executive_compensation", "").lower()
#         director_text = sections.get("director_compensation", "").lower()
        
#         # 1. Tech Executive Presence (max 30 points)
#         tech_exec_score, tech_execs = self._analyze_tech_execs(full_text, tables)
#         tech_exec_score = min(tech_exec_score, 30)
#         logger.info(f"    • Tech Exec Score: {tech_exec_score}/30 | Found: {tech_execs}")
        
#         # 2. AI/Tech Keywords in Compensation (max 30 points)
#         keyword_score, keyword_counts = self._analyze_keywords(exec_comp_text or full_text)
#         keyword_score = min(keyword_score, 30)
#         logger.info(f"    • Keyword Score: {keyword_score}/30 | Counts: {sum(keyword_counts.values())} mentions")
        
#         # 3. Tech-Linked Performance Metrics (max 25 points)
#         perf_score, tech_metrics = self._analyze_performance_metrics(exec_comp_text or full_text)
#         perf_score = min(perf_score, 25)
#         logger.info(f"    • Performance Metric Score: {perf_score}/25 | Found: {len(tech_metrics)} metrics")
        
#         # 4. Board Tech Expertise (max 15 points)
#         board_score, board_indicators = self._analyze_board_expertise(director_text or full_text)
#         board_score = min(board_score, 15)
#         logger.info(f"    • Board Tech Score: {board_score}/15 | Indicators: {len(board_indicators)}")
        
#         # Calculate total
#         total_score = tech_exec_score + keyword_score + perf_score + board_score
#         logger.info(f"  ✅ Total Leadership Score: {total_score}/100")
        
#         return LeadershipScores(
#             tech_exec_score=tech_exec_score,
#             keyword_score=keyword_score,
#             performance_metric_score=perf_score,
#             board_tech_score=board_score,
#             total_score=total_score,
#             tech_execs_found=tech_execs,
#             keyword_counts=keyword_counts,
#             tech_metrics_found=tech_metrics,
#             board_indicators=board_indicators
#         )
    
#     def _analyze_tech_execs(self, text: str, tables: List[Dict]) -> Tuple[float, List[str]]:
#         """Find tech executive titles in text and tables."""
#         found_titles = []
#         score = 0
        
#         # Search in text
#         for title, points in self.TECH_EXEC_TITLES.items():
#             if title in text:
#                 found_titles.append(title.title())
#                 score += points
        
#         # Search in tables (executive compensation tables)
#         for table in tables:
#             headers = [h.lower() if h else "" for h in table.get("headers", [])]
#             if any("name" in h or "officer" in h or "executive" in h for h in headers):
#                 for row in table.get("rows", []):
#                     row_text = " ".join(str(cell).lower() for cell in row if cell)
#                     for title, points in self.TECH_EXEC_TITLES.items():
#                         if title in row_text and title.title() not in found_titles:
#                             found_titles.append(title.title())
#                             score += points
        
#         return score, list(set(found_titles))
    
#     def _analyze_keywords(self, text: str) -> Tuple[float, Dict[str, int]]:
#         """Count AI/tech keywords in compensation discussion."""
#         keyword_counts = {}
#         score = 0
        
#         for keyword, points_per_mention in self.TECH_KEYWORDS.items():
#             # Count occurrences (max 5 counted per keyword)
#             count = len(re.findall(re.escape(keyword), text))
#             if count > 0:
#                 keyword_counts[keyword] = count
#                 score += min(count, 5) * points_per_mention
        
#         return score, keyword_counts
    
#     def _analyze_performance_metrics(self, text: str) -> Tuple[float, List[str]]:
#         """Find tech-linked performance metrics."""
#         metrics_found = []
#         score = 0
        
#         for pattern, points in self.TECH_PERFORMANCE_PHRASES:
#             matches = re.findall(pattern, text, re.IGNORECASE)
#             if matches:
#                 metrics_found.extend(matches)
#                 score += points
        
#         return score, list(set(metrics_found))
    
#     def _analyze_board_expertise(self, text: str) -> Tuple[float, List[str]]:
#         """Analyze board/director tech expertise."""
#         indicators_found = []
#         score = 0
        
#         for pattern, points in self.BOARD_TECH_INDICATORS:
#             matches = re.findall(pattern, text, re.IGNORECASE)
#             if matches:
#                 indicators_found.extend(matches)
#                 score += points
        
#         return score, list(set(indicators_found))
    
#     def calculate_confidence(self, text_length: int, sections_found: int, tables_count: int) -> float:
#         """Calculate confidence based on data quality."""
#         # Base confidence for SEC filing (reliable source)
#         base = 0.75
        
#         # Bonus for more content
#         if text_length > 50000:
#             base += 0.05
#         if text_length > 100000:
#             base += 0.05
        
#         # Bonus for sections found
#         if sections_found >= 1:
#             base += 0.03
#         if sections_found >= 2:
#             base += 0.02
        
#         # Bonus for tables (compensation tables are valuable)
#         if tables_count >= 5:
#             base += 0.03
#         if tables_count >= 10:
#             base += 0.02
        
#         return min(base, 0.95)


# # Singleton
# _analyzer: Optional[LeadershipAnalyzer] = None

# def get_leadership_analyzer() -> LeadershipAnalyzer:
#     global _analyzer
#     if _analyzer is None:
#         _analyzer = LeadershipAnalyzer()
#     return _analyzer

# app/pipelines/leadership_analyzer.py
# """
# Leadership Signal Analyzer - DEF 14A Proxy Statement Analysis

# ALIGNED WITH:
#   - CS2 PDF: Signal category "leadership_signals" (weight 0.20)
#   - CS2 PDF page 6: DEF-14A -> "Executive compensation tied to tech"
#   - CS3 PDF page 7: Maps to Leadership(0.60), AI_Governance(0.25), Culture(0.15)
#   - CS3 PDF page 12: Leadership rubric (5 levels)
#   - CS3 PDF page 11: AI Governance rubric (5 levels)

# Scoring breakdown (total 100):
#   Component                    Max   CS3 Primary Dimension
#   Tech Executive Presence       25   Leadership
#   AI/Tech Strategy Keywords     20   Leadership
#   Tech-Linked Comp Metrics      15   Leadership
#   Board Tech Expertise          20   AI Governance
#   Governance Structure          10   AI Governance
#   Innovation Culture Signals    10   Culture
#   TOTAL                        100
# """
# import re
# import logging
# from typing import Dict, List, Tuple, Optional
# from dataclasses import dataclass, field

# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)-8s | %(message)s',
#     datefmt='%H:%M:%S'
# )
# logger = logging.getLogger(__name__)


# @dataclass
# class LeadershipScores:
#     """Breakdown of leadership signal scores with CS3 dimension sub-scores."""
#     tech_exec_score: float
#     strategy_keyword_score: float
#     comp_metric_score: float
#     board_tech_score: float
#     governance_score: float
#     culture_score: float
#     total_score: float
#     leadership_sub: float
#     governance_sub: float
#     culture_sub: float
#     tech_execs_found: List[str] = field(default_factory=list)
#     strategy_keywords_found: Dict[str, int] = field(default_factory=dict)
#     comp_metrics_found: List[str] = field(default_factory=list)
#     board_indicators: List[str] = field(default_factory=list)
#     governance_indicators: List[str] = field(default_factory=list)
#     culture_indicators: List[str] = field(default_factory=list)


# class LeadershipAnalyzer:
#     """Analyze DEF 14A filings for leadership signals."""

#     TECH_EXEC_TITLES = {
#         "chief ai officer": 10,
#         "chief artificial intelligence officer": 10,
#         "chief technology officer": 8,
#         "chief digital officer": 8,
#         "chief data officer": 8,
#         "chief analytics officer": 7,
#         "chief innovation officer": 7,
#         "chief information officer": 5,
#         "vp of artificial intelligence": 5,
#         "vp of machine learning": 5,
#         "vp of data science": 4,
#         "svp of technology": 4,
#         "svp of digital": 4,
#         "head of ai": 5,
#         "head of machine learning": 5,
#         "head of data science": 4,
#         "head of technology": 4,
#         "head of analytics": 4,
#         "head of digital": 4,
#         "global technology": 5,
#         "global head of technology": 6,
#         "co-chief operating officer": 3,
#     }

#     TECH_EXEC_ABBREVS_STRICT = {
#         "CTO": 8,
#         "CDO": 8,
#         "CAIO": 10,
#         "CIO": 5,
#         "CAO": 7,
#     }

#     STRATEGY_KEYWORDS = {
#         "artificial intelligence": 3,
#         "machine learning": 3,
#         "deep learning": 3,
#         "digital transformation": 2.5,
#         "ai strategy": 4,
#         "ai initiative": 3,
#         "data analytics": 2,
#         "technology modernization": 2,
#         "automation strategy": 2,
#         "predictive analytics": 2,
#         "generative ai": 3,
#         "large language model": 3,
#         "data driven": 1.5,
#         "cloud transformation": 1.5,
#         "technology investment": 1.5,
#         "digital strategy": 1.5,
#         "technology platform": 1.5,
#         "technology spend": 2,
#         "technology budget": 2,
#         "technology capabilities": 1.5,
#         "fintech": 2,
#         "financial technology": 2,
#     }

#     COMP_METRIC_PATTERNS = [
#         (r'(?:technology|digital|ai|innovation)\s+(?:metric|goal|objective|target|priority|priorities)', 5),
#         (r'(?:digital\s+transformation|ai|automation)\s+(?:bonus|incentive|award|compensation)', 6),
#         (r'(?:ai|artificial intelligence)\s+(?:initiative|deployment|implementation|investment)', 5),
#         (r'(?:technology|digital)\s+(?:investment|spend|budget|spending)', 4),
#         (r'(?:data|analytics|ai)\s+(?:strategy|platform|capabilities)\s+(?:goal|metric|milestone|growth|investment)', 5),
#         (r'(?:automation|efficiency)\s+(?:metric|savings|target|improvement)', 3),
#         (r'(?:innovation|r&d|research)\s+(?:metric|performance|milestone|investment|spend)', 3),
#         (r'(?:tech|technology|digital)\s+(?:revenue|growth|transformation)\s+(?:target|goal|initiative|strategy)', 5),
#         (r'technology\s+(?:and\s+)?(?:operations|infrastructure)', 3),
#         (r'strategic\s+(?:technology|digital|ai)', 4),
#         (r'(?:invested|investment|spend|spent)\s+(?:[\w\s]*?)(?:technology|digital|ai)', 3),
#     ]

#     BOARD_EXPERTISE_PATTERNS = [
#         (r'\b(?:google|alphabet|microsoft|amazon|meta|apple|nvidia|intel|ibm|oracle|salesforce|adobe|cisco|sap|palantir|snowflake|databricks|accenture|intuit)\b', 4),
#         (r'\b(?:chief technology officer|chief digital officer|chief data officer|chief ai officer|chief information officer)\b', 5),
#         (r'\b(?:technology|digital|data|ai)\s+(?:executive|leader|officer|expert|expertise|experience)\b', 3),
#         (r'\b(?:financial technology|fintech|data-driven|technology sector|technology industry|software industry|tech industry)\b', 3),
#         (r'\b(?:computer science|software engineering|data science|machine learning|artificial intelligence)\b', 2),
#         (r'\b(?:technology and (?:operations|innovation|consumer|digital)|digital (?:commerce|banking|platforms|transformation))\b', 3),
#         (r'\b(?:growth and innovation|innovation in the|technology (?:and|or) (?:cyber|information|data))\b', 2),
#         (r'\b(?:technology risk|cyber\s*security|information security)\b', 2),
#     ]

#     GOVERNANCE_PATTERNS = [
#         (r'\b(?:technology|digital|innovation|ai|data|cyber)\s+committee\b', 5),
#         (r'\b(?:technology\s+risk\s+subcommittee|tris)\b', 5),
#         (r'\bai\s+(?:governance|policy|ethics|oversight|framework|principles)\b', 5),
#         (r'\b(?:model\s+risk|algorithm|ai)\s+(?:management|oversight|review)\b', 4),
#         (r'\b(?:responsible|ethical)\s+(?:ai|artificial intelligence|technology)\b', 3),
#         (r'\brisk\s+committee.*?(?:technology|cyber|digital|ai)\b', 4),
#         (r'\b(?:data\s+governance|data\s+privacy|data\s+protection)\s+(?:policy|framework|oversight)\b', 3),
#         (r'\b(?:cybersecurity|cyber\s+security)\s+(?:oversight|risk|governance|framework)\b', 3),
#         (r'\b(?:technology|digital|information)\s+(?:risk|security)\s+(?:oversight|management|governance)\b', 3),
#         (r'\bboard\s+(?:oversight|review|discussion).*?(?:technology|ai|cyber|digital)\b', 4),
#         (r'\b(?:manages?|managing)\s+(?:opportunities? and )?risks?\s+related\s+to\s+(?:artificial intelligence|ai|technology)\b', 4),
#     ]

#     CULTURE_PATTERNS = [
#         (r'\b(?:innovation|innovative)\s+(?:culture|mindset|environment)\b', 3),
#         (r'\b(?:data[- ]driven|evidence[- ]based)\s+(?:culture|decision|organization|growth)\b', 3),
#         (r'\b(?:fail[- ]fast|experimentation|test and learn)\b', 3),
#         (r'\b(?:continuous\s+improvement|growth\s+mindset|learning\s+culture)\b', 2),
#         (r'\b(?:agile|adaptive|fast[- ]paced)\s+(?:culture|environment|organization)\b', 2),
#         (r'\b(?:employee\s+innovation|hackathon|innovation\s+lab|skunkworks)\b', 3),
#         (r'\bpromote\s+(?:innovation|creativity)\b', 2),
#         (r'\bfoster(?:ed|ing)?\s+(?:a\s+)?(?:culture|environment)\s+of\s+(?:innovation|respect|inclusion|creativity)\b', 3),
#         (r'\b(?:innovation|innovative),?\s+(?:creativity|productivity|growth)\b', 2),
#         (r'\b(?:culture\s+of\s+(?:respect|innovation|inclusion|excellence))\b', 2),
#         (r'\b(?:change and innovation|periods? of change)\b', 1),
#         (r'\bdata[- ]driven\b', 1),
#     ]

#     XBRL_NOISE_PATTERNS = [
#         re.compile(r'\b[a-z]{2,6}:[A-Z][A-Za-z]{5,}\b'),
#         re.compile(r'\b\d{10}\b'),
#         re.compile(r'(?m)^\d{4}-\d{2}-\d{2}$'),
#         re.compile(r'\b(?:iso4217|xbrli):[A-Za-z]+\b'),
#         re.compile(r'\b[a-z]{2,5}-\d{8}\b'),
#     ]

#     # MIN_SECTION_LENGTH = 10000
#     # MIN_SECTION_WORD_COUNT = 500

#     MIN_SECTION_LENGTH = 3000
#     MIN_SECTION_WORD_COUNT = 200

#     def __init__(self):
#         self._exec_title_patterns = {
#             title: (re.compile(r'\b' + re.escape(title) + r'\b', re.IGNORECASE), pts)
#             for title, pts in self.TECH_EXEC_TITLES.items()
#         }
#         self._exec_abbrev_patterns = {
#             abbr: (re.compile(r'(?<![A-Za-z])' + re.escape(abbr) + r'(?![A-Za-z])'), pts)
#             for abbr, pts in self.TECH_EXEC_ABBREVS_STRICT.items()
#         }
#         self._strategy_patterns = {
#             kw: (re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE), pts)
#             for kw, pts in self.STRATEGY_KEYWORDS.items()
#         }
#         self._comp_patterns = [
#             (re.compile(p, re.IGNORECASE), pts) for p, pts in self.COMP_METRIC_PATTERNS
#         ]
#         self._board_patterns = [
#             (re.compile(p, re.IGNORECASE), pts) for p, pts in self.BOARD_EXPERTISE_PATTERNS
#         ]
#         self._gov_patterns = [
#             (re.compile(p, re.IGNORECASE), pts) for p, pts in self.GOVERNANCE_PATTERNS
#         ]
#         self._culture_patterns = [
#             (re.compile(p, re.IGNORECASE), pts) for p, pts in self.CULTURE_PATTERNS
#         ]
#         logger.info("Leadership Analyzer initialized (CS3-ready, word-boundary matching)")

#     def _clean_xbrl_text(self, raw_text: str) -> str:
#         text = raw_text
#         for pattern in self.XBRL_NOISE_PATTERNS:
#             text = pattern.sub('', text)
#         text = re.sub(r'\n{3,}', '\n\n', text)
#         text = re.sub(r'[ \t]{2,}', ' ', text)
#         lines = [ln.strip() for ln in text.splitlines()]
#         text = '\n'.join(ln for ln in lines if ln)
#         return text

#     # def _select_text(self, sections: Dict[str, str], section_key: str,
#     #                  fallback_text: str) -> str:
#     #     section = sections.get(section_key, "")
#     #     if (len(section) >= self.MIN_SECTION_LENGTH
#     #             and len(section.split()) >= self.MIN_SECTION_WORD_COUNT):
#     #         return section
#     #     return fallback_text

#     def _select_text(self, sections: Dict[str, str], section_key: str,
#                      fallback_text: str) -> str:
#         """Select best text source for analysis.
        
#         If a dedicated section exists but is small, combine it with fallback
#         so we don't miss signals that might be in either location.
#         """
#         section = sections.get(section_key, "")
#         if (len(section) >= self.MIN_SECTION_LENGTH
#                 and len(section.split()) >= self.MIN_SECTION_WORD_COUNT):
#             return section
        
#         # If section exists but is small, combine both for broader coverage
#         if section and len(section) > 500:
#             # Deduplicate: only prepend section if it's not already in fallback
#             if section[:200] not in fallback_text[:len(fallback_text)]:
#                 return section + "\n\n" + fallback_text
        
#         return fallback_text


#     def analyze(self, text_content: str, sections: Dict[str, str],
#                 tables: List[Dict]) -> LeadershipScores:
#         logger.info("  Analyzing leadership signals...")

#         full_text = self._clean_xbrl_text(text_content)
#         cleaned_len = len(full_text)
#         logger.info(
#             f"  Cleaned text: {len(text_content):,} -> {cleaned_len:,} chars "
#             f"({len(text_content) - cleaned_len:,} XBRL chars removed)"
#         )

#         clean_sections = {k: self._clean_xbrl_text(v) for k, v in sections.items()}

#         exec_text = self._select_text(clean_sections, "executive_compensation", full_text)
#         director_text = self._select_text(clean_sections, "director_compensation", full_text)
#         governance_text = self._select_text(clean_sections, "corporate_governance", full_text)

#         logger.info(
#             f"  Text sources -> exec_comp: {len(exec_text):,} chars | "
#             f"director: {len(director_text):,} chars | "
#             f"governance: {len(governance_text):,} chars"
#         )

#         tech_exec_score, tech_execs = self._analyze_tech_execs(full_text, tables)
#         tech_exec_score = min(tech_exec_score, 25)
#         logger.info(f"    Tech Exec Score: {tech_exec_score}/25 | Found: {tech_execs}")

#         strategy_score, strategy_kw = self._analyze_strategy_keywords(exec_text)
#         strategy_score = min(strategy_score, 20)
#         logger.info(f"    Strategy Keyword Score: {strategy_score}/20 | "
#                      f"{sum(strategy_kw.values())} mentions across {len(strategy_kw)} keywords")

#         comp_score, comp_metrics = self._analyze_comp_metrics(exec_text)
#         comp_score = min(comp_score, 15)
#         logger.info(f"    Comp Metric Score: {comp_score}/15 | Found: {len(comp_metrics)} metrics")

#         board_score, board_indicators = self._analyze_board_expertise(director_text)
#         board_score = min(board_score, 20)
#         logger.info(f"    Board Tech Score: {board_score}/20 | Indicators: {len(board_indicators)}")

#         gov_score, gov_indicators = self._analyze_governance(governance_text)
#         gov_score = min(gov_score, 10)
#         logger.info(f"    Governance Score: {gov_score}/10 | Indicators: {len(gov_indicators)}")

#         culture_score, culture_indicators = self._analyze_culture(full_text)
#         culture_score = min(culture_score, 10)
#         logger.info(f"    Culture Score: {culture_score}/10 | Indicators: {len(culture_indicators)}")

#         total = (tech_exec_score + strategy_score + comp_score
#                  + board_score + gov_score + culture_score)
#         leadership_sub = tech_exec_score + strategy_score + comp_score
#         governance_sub = board_score + gov_score
#         culture_sub = culture_score

#         logger.info(f"  Total Leadership Score: {total}/100")
#         logger.info(f"  CS3 subs -> Leadership: {leadership_sub}/60 | "
#                      f"Governance: {governance_sub}/30 | Culture: {culture_sub}/10")

#         return LeadershipScores(
#             tech_exec_score=tech_exec_score, strategy_keyword_score=strategy_score,
#             comp_metric_score=comp_score, board_tech_score=board_score,
#             governance_score=gov_score, culture_score=culture_score,
#             total_score=total, leadership_sub=leadership_sub,
#             governance_sub=governance_sub, culture_sub=culture_sub,
#             tech_execs_found=tech_execs, strategy_keywords_found=strategy_kw,
#             comp_metrics_found=comp_metrics, board_indicators=board_indicators,
#             governance_indicators=gov_indicators, culture_indicators=culture_indicators,
#         )

#     def _analyze_tech_execs(self, text: str, tables: List[Dict]) -> Tuple[float, List[str]]:
#         found = set()
#         score = 0.0
#         for title, (pattern, pts) in self._exec_title_patterns.items():
#             if pattern.search(text):
#                 found.add(title.title())
#                 score += pts
#         for abbr, (pattern, pts) in self._exec_abbrev_patterns.items():
#             full_already = any(abbr.lower() in t.lower() for t in found)
#             if not full_already and pattern.search(text):
#                 found.add(abbr)
#                 score += pts
#         for table in tables:
#             headers = [h.lower() if h else "" for h in table.get("headers", [])]
#             if any(kw in h for h in headers for kw in ("name", "officer", "executive", "title")):
#                 for row in table.get("rows", []):
#                     row_text = " ".join(str(c) for c in row if c)
#                     for title, (pattern, pts) in self._exec_title_patterns.items():
#                         if pattern.search(row_text) and title.title() not in found:
#                             found.add(title.title())
#                             score += pts
#                     for abbr, (pattern, pts) in self._exec_abbrev_patterns.items():
#                         if abbr not in found and pattern.search(row_text):
#                             found.add(abbr)
#                             score += pts
#         return score, sorted(found)

#     def _analyze_strategy_keywords(self, text: str) -> Tuple[float, Dict[str, int]]:
#         kw_counts = {}
#         score = 0.0
#         for kw, (pattern, pts) in self._strategy_patterns.items():
#             matches = pattern.findall(text)
#             count = len(matches)
#             if count > 0:
#                 kw_counts[kw] = count
#                 score += min(count, 5) * pts
#         return score, kw_counts

#     def _analyze_comp_metrics(self, text: str) -> Tuple[float, List[str]]:
#         found = []
#         score = 0.0
#         for pattern, pts in self._comp_patterns:
#             matches = pattern.findall(text)
#             if matches:
#                 found.extend(matches)
#                 score += pts
#         return score, sorted(set(found))

#     def _analyze_board_expertise(self, text: str) -> Tuple[float, List[str]]:
#         found = []
#         score = 0.0
#         for pattern, pts in self._board_patterns:
#             matches = pattern.findall(text)
#             if matches:
#                 found.extend(matches)
#                 score += pts
#         return score, sorted(set(found))

#     def _analyze_governance(self, text: str) -> Tuple[float, List[str]]:
#         found = []
#         score = 0.0
#         for pattern, pts in self._gov_patterns:
#             matches = pattern.findall(text)
#             if matches:
#                 found.extend(matches)
#                 score += pts
#         return score, sorted(set(found))

#     def _analyze_culture(self, text: str) -> Tuple[float, List[str]]:
#         found = []
#         score = 0.0
#         for pattern, pts in self._culture_patterns:
#             matches = pattern.findall(text)
#             if matches:
#                 found.extend(matches)
#                 score += pts
#         return score, sorted(set(found))

#     def calculate_confidence(self, text_length: int, sections_found: int,
#                              tables_count: int, cleaned_text_length: int = 0) -> float:
#         effective_len = cleaned_text_length if cleaned_text_length > 0 else text_length
#         base = 0.70
#         if effective_len > 30000: base += 0.04
#         if effective_len > 75000: base += 0.04
#         if effective_len > 150000: base += 0.02
#         if sections_found >= 1: base += 0.03
#         if sections_found >= 3: base += 0.02
#         if tables_count >= 3: base += 0.03
#         if tables_count >= 8: base += 0.02
#         if tables_count >= 15: base += 0.02
#         return min(base, 0.92)


# _analyzer: Optional[LeadershipAnalyzer] = None

# def get_leadership_analyzer() -> LeadershipAnalyzer:
#     global _analyzer
#     if _analyzer is None:
#         _analyzer = LeadershipAnalyzer()
#     return _analyzer

# app/pipelines/leadership_analyzer.py
"""
Leadership Signal Analyzer - DEF 14A Proxy Statement Analysis

ALIGNED WITH:
  - CS2 PDF: Signal category "leadership_signals" (weight 0.20)
  - CS2 PDF page 6: DEF-14A -> "Executive compensation tied to tech"
  - CS3 PDF page 7: Maps to Leadership(0.60), AI_Governance(0.25), Culture(0.15)
  - CS3 PDF page 12: Leadership rubric (5 levels)
  - CS3 PDF page 11: AI Governance rubric (5 levels)

Scoring breakdown (total 100):
  Component                    Max   CS3 Primary Dimension
  Tech Executive Presence       25   Leadership
  AI/Tech Strategy Keywords     20   Leadership
  Tech-Linked Comp Metrics      15   Leadership
  Board Tech Expertise          20   AI Governance
  Governance Structure          10   AI Governance
  Innovation Culture Signals    10   Culture
  TOTAL                        100

FIXES (v2):
  - Lowered MIN_SECTION_LENGTH 10000->3000, MIN_SECTION_WORD_COUNT 500->200
    so small but real sections (e.g. GS exec_comp=3702 chars) aren't discarded
  - _select_text now combines small section + fallback instead of dropping it
    (GS embeds comp discussion inside governance text, not a separate section)
  - This fixes GS leadership scoring 15/100 when it should be ~25-35
"""
import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class LeadershipScores:
    """Breakdown of leadership signal scores with CS3 dimension sub-scores."""
    tech_exec_score: float
    strategy_keyword_score: float
    comp_metric_score: float
    board_tech_score: float
    governance_score: float
    culture_score: float
    total_score: float
    leadership_sub: float
    governance_sub: float
    culture_sub: float
    tech_execs_found: List[str] = field(default_factory=list)
    strategy_keywords_found: Dict[str, int] = field(default_factory=dict)
    comp_metrics_found: List[str] = field(default_factory=list)
    board_indicators: List[str] = field(default_factory=list)
    governance_indicators: List[str] = field(default_factory=list)
    culture_indicators: List[str] = field(default_factory=list)


class LeadershipAnalyzer:
    """Analyze DEF 14A filings for leadership signals."""

    TECH_EXEC_TITLES = {
        "chief ai officer": 10,
        "chief artificial intelligence officer": 10,
        "chief technology officer": 8,
        "chief digital officer": 8,
        "chief data officer": 8,
        "chief analytics officer": 7,
        "chief innovation officer": 7,
        "chief information officer": 5,
        "vp of artificial intelligence": 5,
        "vp of machine learning": 5,
        "vp of data science": 4,
        "svp of technology": 4,
        "svp of digital": 4,
        "head of ai": 5,
        "head of machine learning": 5,
        "head of data science": 4,
        "head of technology": 4,
        "head of analytics": 4,
        "head of digital": 4,
        "global technology": 5,
        "global head of technology": 6,
        "co-chief operating officer": 3,
        "co-chief operating officer": 3,
        "chief scientist": 7,
        "svp of research": 5,
    }

    TECH_EXEC_ABBREVS_STRICT = {
        "CTO": 8,
        "CDO": 8,
        "CAIO": 10,
        "CIO": 5,
        "CAO": 7,
    }

    STRATEGY_KEYWORDS = {
        "artificial intelligence": 3,
        "machine learning": 3,
        "deep learning": 3,
        "digital transformation": 2.5,
        "ai strategy": 4,
        "ai initiative": 3,
        "data analytics": 2,
        "technology modernization": 2,
        "automation strategy": 2,
        "predictive analytics": 2,
        "generative ai": 3,
        "large language model": 3,
        "data driven": 1.5,
        "cloud transformation": 1.5,
        "technology investment": 1.5,
        "digital strategy": 1.5,
        "technology platform": 1.5,
        "technology spend": 2,
        "technology budget": 2,
        "technology capabilities": 1.5,
        "fintech": 2,
        "financial technology": 2,
         "accelerated computing": 3,
        "full-stack": 2,
        "full stack computing": 3,
        "data center scale": 2,
        "gpu computing": 2.5,
        "inference platform": 3,
        "training platform": 3,
        "cuda": 2,
        "tensor core": 2,
        "ai computing": 3,
        "ai infrastructure": 3,
        "semiconductor": 1.5,
        "parallel processing": 2,
        "compute platform": 2,
        "ai workload": 2.5,
    }

    COMP_METRIC_PATTERNS = [
        (r'(?:technology|digital|ai|innovation)\s+(?:metric|goal|objective|target|priority|priorities)', 5),
        (r'(?:digital\s+transformation|ai|automation)\s+(?:bonus|incentive|award|compensation)', 6),
        (r'(?:ai|artificial intelligence)\s+(?:initiative|deployment|implementation|investment)', 5),
        (r'(?:technology|digital)\s+(?:investment|spend|budget|spending)', 4),
        (r'(?:data|analytics|ai)\s+(?:strategy|platform|capabilities)\s+(?:goal|metric|milestone|growth|investment)', 5),
        (r'(?:automation|efficiency)\s+(?:metric|savings|target|improvement)', 3),
        (r'(?:innovation|r&d|research)\s+(?:metric|performance|milestone|investment|spend)', 3),
        (r'(?:tech|technology|digital)\s+(?:revenue|growth|transformation)\s+(?:target|goal|initiative|strategy)', 5),
        (r'technology\s+(?:and\s+)?(?:operations|infrastructure)', 3),
        (r'strategic\s+(?:technology|digital|ai)', 4),
        (r'(?:invested|investment|spend|spent)\s+(?:[\w\s]*?)(?:technology|digital|ai)', 3),
        #  (r'(?:gpu|chip|semiconductor|compute)\\s+(?:revenue|growth|market\\s+share)', 4),
        # (r'(?:data\\s+center|datacenter)\\s+(?:revenue|growth|demand)', 4),
        # (r'(?:platform|ecosystem)\\s+(?:growth|adoption|expansion)', 3),
        # (r'(?:research|r&d|engineering)\\s+(?:investment|headcount|productivity)', 3),
        (r'(?:gpu|chip|semiconductor|compute)\s+(?:revenue|growth|market\s+share)', 4),
        (r'(?:data\s+center|datacenter)\s+(?:revenue|growth|demand)', 4),
        (r'(?:platform|ecosystem)\s+(?:growth|adoption|expansion)', 3),
        (r'(?:research|r&d|engineering)\s+(?:investment|headcount|productivity)', 3),
    ]

    BOARD_EXPERTISE_PATTERNS = [
        (r'\b(?:google|alphabet|microsoft|amazon|meta|apple|nvidia|intel|ibm|oracle|salesforce|adobe|cisco|sap|palantir|snowflake|databricks|accenture|intuit)\b', 4),
        (r'\b(?:chief technology officer|chief digital officer|chief data officer|chief ai officer|chief information officer)\b', 5),
        (r'\b(?:technology|digital|data|ai)\s+(?:executive|leader|officer|expert|expertise|experience)\b', 3),
        (r'\b(?:financial technology|fintech|data-driven|technology sector|technology industry|software industry|tech industry)\b', 3),
        (r'\b(?:computer science|software engineering|data science|machine learning|artificial intelligence)\b', 2),
        (r'\b(?:technology and (?:operations|innovation|consumer|digital)|digital (?:commerce|banking|platforms|transformation))\b', 3),
        (r'\b(?:growth and innovation|innovation in the|technology (?:and|or) (?:cyber|information|data))\b', 2),
        (r'\b(?:technology risk|cyber\s*security|information security)\b', 2),
    ]

    GOVERNANCE_PATTERNS = [
        (r'\b(?:technology|digital|innovation|ai|data|cyber)\s+committee\b', 5),
        (r'\b(?:technology\s+risk\s+subcommittee|tris)\b', 5),
        (r'\bai\s+(?:governance|policy|ethics|oversight|framework|principles)\b', 5),
        (r'\b(?:model\s+risk|algorithm|ai)\s+(?:management|oversight|review)\b', 4),
        (r'\b(?:responsible|ethical)\s+(?:ai|artificial intelligence|technology)\b', 3),
        (r'\brisk\s+committee.*?(?:technology|cyber|digital|ai)\b', 4),
        (r'\b(?:data\s+governance|data\s+privacy|data\s+protection)\s+(?:policy|framework|oversight)\b', 3),
        (r'\b(?:cybersecurity|cyber\s+security)\s+(?:oversight|risk|governance|framework)\b', 3),
        (r'\b(?:technology|digital|information)\s+(?:risk|security)\s+(?:oversight|management|governance)\b', 3),
        (r'\bboard\s+(?:oversight|review|discussion).*?(?:technology|ai|cyber|digital)\b', 4),
        (r'\b(?:manages?|managing)\s+(?:opportunities? and )?risks?\s+related\s+to\s+(?:artificial intelligence|ai|technology)\b', 4),
    ]

    CULTURE_PATTERNS = [
        (r'\b(?:innovation|innovative)\s+(?:culture|mindset|environment)\b', 3),
        (r'\b(?:data[- ]driven|evidence[- ]based)\s+(?:culture|decision|organization|growth)\b', 3),
        (r'\b(?:fail[- ]fast|experimentation|test and learn)\b', 3),
        (r'\b(?:continuous\s+improvement|growth\s+mindset|learning\s+culture)\b', 2),
        (r'\b(?:agile|adaptive|fast[- ]paced)\s+(?:culture|environment|organization)\b', 2),
        (r'\b(?:employee\s+innovation|hackathon|innovation\s+lab|skunkworks)\b', 3),
        (r'\bpromote\s+(?:innovation|creativity)\b', 2),
        (r'\bfoster(?:ed|ing)?\s+(?:a\s+)?(?:culture|environment)\s+of\s+(?:innovation|respect|inclusion|creativity)\b', 3),
        (r'\b(?:innovation|innovative),?\s+(?:creativity|productivity|growth)\b', 2),
        (r'\b(?:culture\s+of\s+(?:respect|innovation|inclusion|excellence))\b', 2),
        (r'\b(?:change and innovation|periods? of change)\b', 1),
        (r'\bdata[- ]driven\b', 1),
    ]

    XBRL_NOISE_PATTERNS = [
        re.compile(r'\b[a-z]{2,6}:[A-Z][A-Za-z]{5,}\b'),
        re.compile(r'\b\d{10}\b'),
        re.compile(r'(?m)^\d{4}-\d{2}-\d{2}$'),
        re.compile(r'\b(?:iso4217|xbrli):[A-Za-z]+\b'),
        re.compile(r'\b[a-z]{2,5}-\d{8}\b'),
    ]

    # FIX: Lowered thresholds so small-but-real sections aren't discarded.
    # GS's DEF 14A had exec_comp = 3,702 chars — that's a real section with
    # meaningful content, not noise. Old threshold (10000) threw it away.
    MIN_SECTION_LENGTH = 3000
    MIN_SECTION_WORD_COUNT = 200

    def __init__(self):
        self._exec_title_patterns = {
            title: (re.compile(r'\b' + re.escape(title) + r'\b', re.IGNORECASE), pts)
            for title, pts in self.TECH_EXEC_TITLES.items()
        }
        self._exec_abbrev_patterns = {
            abbr: (re.compile(r'(?<![A-Za-z])' + re.escape(abbr) + r'(?![A-Za-z])'), pts)
            for abbr, pts in self.TECH_EXEC_ABBREVS_STRICT.items()
        }
        self._strategy_patterns = {
            kw: (re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE), pts)
            for kw, pts in self.STRATEGY_KEYWORDS.items()
        }
        self._comp_patterns = [
            (re.compile(p, re.IGNORECASE), pts) for p, pts in self.COMP_METRIC_PATTERNS
        ]
        self._board_patterns = [
            (re.compile(p, re.IGNORECASE), pts) for p, pts in self.BOARD_EXPERTISE_PATTERNS
        ]
        self._gov_patterns = [
            (re.compile(p, re.IGNORECASE), pts) for p, pts in self.GOVERNANCE_PATTERNS
        ]
        self._culture_patterns = [
            (re.compile(p, re.IGNORECASE), pts) for p, pts in self.CULTURE_PATTERNS
        ]
        logger.info("Leadership Analyzer initialized (CS3-ready, word-boundary matching)")

    def _clean_xbrl_text(self, raw_text: str) -> str:
        text = raw_text
        for pattern in self.XBRL_NOISE_PATTERNS:
            text = pattern.sub('', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        lines = [ln.strip() for ln in text.splitlines()]
        text = '\n'.join(ln for ln in lines if ln)
        return text

    def _select_text(self, sections: Dict[str, str], section_key: str,
                     fallback_text: str) -> str:
        """Select best text source for analysis.

        If a dedicated section exists and meets thresholds, use it alone.
        If it exists but is small (500-3000 chars), combine it with fallback
        so we don't miss signals that live in either location.

        FIX: GS's proxy embeds compensation discussion inside governance text
        rather than a separate section. The old logic discarded the 3,702-char
        exec_comp section entirely and searched only full_text (539K chars of
        mostly governance boilerplate), causing strategy keywords and comp
        metrics to score 0. Now we combine both for broader coverage.
        """
        section = sections.get(section_key, "")
        if (len(section) >= self.MIN_SECTION_LENGTH
                and len(section.split()) >= self.MIN_SECTION_WORD_COUNT):
            return section

        # Section exists but is small — combine with fallback for broader coverage
        if section and len(section) > 500:
            # Prepend the dedicated section so its signals get priority,
            # then append full text to catch anything embedded elsewhere
            return section + "\n\n" + fallback_text

        return fallback_text

    def analyze(self, text_content: str, sections: Dict[str, str],
                tables: List[Dict]) -> LeadershipScores:
        logger.info("  Analyzing leadership signals...")

        full_text = self._clean_xbrl_text(text_content)
        cleaned_len = len(full_text)
        logger.info(
            f"  Cleaned text: {len(text_content):,} -> {cleaned_len:,} chars "
            f"({len(text_content) - cleaned_len:,} XBRL chars removed)"
        )

        clean_sections = {k: self._clean_xbrl_text(v) for k, v in sections.items()}

        exec_text = self._select_text(clean_sections, "executive_compensation", full_text)
        director_text = self._select_text(clean_sections, "director_compensation", full_text)
        governance_text = self._select_text(clean_sections, "corporate_governance", full_text)

        logger.info(
            f"  Text sources -> exec_comp: {len(exec_text):,} chars | "
            f"director: {len(director_text):,} chars | "
            f"governance: {len(governance_text):,} chars"
        )

        tech_exec_score, tech_execs = self._analyze_tech_execs(full_text, tables)
        tech_exec_score = min(tech_exec_score, 25)
        logger.info(f"    Tech Exec Score: {tech_exec_score}/25 | Found: {tech_execs}")

        strategy_score, strategy_kw = self._analyze_strategy_keywords(exec_text)
        strategy_score = min(strategy_score, 20)
        logger.info(f"    Strategy Keyword Score: {strategy_score}/20 | "
                     f"{sum(strategy_kw.values())} mentions across {len(strategy_kw)} keywords")

        comp_score, comp_metrics = self._analyze_comp_metrics(exec_text)
        comp_score = min(comp_score, 15)
        logger.info(f"    Comp Metric Score: {comp_score}/15 | Found: {len(comp_metrics)} metrics")

        board_score, board_indicators = self._analyze_board_expertise(director_text)
        board_score = min(board_score, 20)
        logger.info(f"    Board Tech Score: {board_score}/20 | Indicators: {len(board_indicators)}")

        gov_score, gov_indicators = self._analyze_governance(governance_text)
        gov_score = min(gov_score, 10)
        logger.info(f"    Governance Score: {gov_score}/10 | Indicators: {len(gov_indicators)}")

        culture_score, culture_indicators = self._analyze_culture(full_text)
        culture_score = min(culture_score, 10)
        logger.info(f"    Culture Score: {culture_score}/10 | Indicators: {len(culture_indicators)}")

        total = (tech_exec_score + strategy_score + comp_score
                 + board_score + gov_score + culture_score)
        leadership_sub = tech_exec_score + strategy_score + comp_score
        governance_sub = board_score + gov_score
        culture_sub = culture_score

        logger.info(f"  Total Leadership Score: {total}/100")
        logger.info(f"  CS3 subs -> Leadership: {leadership_sub}/60 | "
                     f"Governance: {governance_sub}/30 | Culture: {culture_sub}/10")

        return LeadershipScores(
            tech_exec_score=tech_exec_score, strategy_keyword_score=strategy_score,
            comp_metric_score=comp_score, board_tech_score=board_score,
            governance_score=gov_score, culture_score=culture_score,
            total_score=total, leadership_sub=leadership_sub,
            governance_sub=governance_sub, culture_sub=culture_sub,
            tech_execs_found=tech_execs, strategy_keywords_found=strategy_kw,
            comp_metrics_found=comp_metrics, board_indicators=board_indicators,
            governance_indicators=gov_indicators, culture_indicators=culture_indicators,
        )

    def _analyze_tech_execs(self, text: str, tables: List[Dict]) -> Tuple[float, List[str]]:
        found = set()
        score = 0.0
        for title, (pattern, pts) in self._exec_title_patterns.items():
            if pattern.search(text):
                found.add(title.title())
                score += pts
        for abbr, (pattern, pts) in self._exec_abbrev_patterns.items():
            full_already = any(abbr.lower() in t.lower() for t in found)
            if not full_already and pattern.search(text):
                found.add(abbr)
                score += pts
        for table in tables:
            headers = [h.lower() if h else "" for h in table.get("headers", [])]
            if any(kw in h for h in headers for kw in ("name", "officer", "executive", "title")):
                for row in table.get("rows", []):
                    row_text = " ".join(str(c) for c in row if c)
                    for title, (pattern, pts) in self._exec_title_patterns.items():
                        if pattern.search(row_text) and title.title() not in found:
                            found.add(title.title())
                            score += pts
                    for abbr, (pattern, pts) in self._exec_abbrev_patterns.items():
                        if abbr not in found and pattern.search(row_text):
                            found.add(abbr)
                            score += pts
        return score, sorted(found)

    def _analyze_strategy_keywords(self, text: str) -> Tuple[float, Dict[str, int]]:
        kw_counts = {}
        score = 0.0
        for kw, (pattern, pts) in self._strategy_patterns.items():
            matches = pattern.findall(text)
            count = len(matches)
            if count > 0:
                kw_counts[kw] = count
                score += min(count, 5) * pts
        return score, kw_counts

    def _analyze_comp_metrics(self, text: str) -> Tuple[float, List[str]]:
        found = []
        score = 0.0
        for pattern, pts in self._comp_patterns:
            matches = pattern.findall(text)
            if matches:
                found.extend(matches)
                score += pts
        return score, sorted(set(found))

    def _analyze_board_expertise(self, text: str) -> Tuple[float, List[str]]:
        found = []
        score = 0.0
        for pattern, pts in self._board_patterns:
            matches = pattern.findall(text)
            if matches:
                found.extend(matches)
                score += pts
        return score, sorted(set(found))

    def _analyze_governance(self, text: str) -> Tuple[float, List[str]]:
        found = []
        score = 0.0
        for pattern, pts in self._gov_patterns:
            matches = pattern.findall(text)
            if matches:
                found.extend(matches)
                score += pts
        return score, sorted(set(found))

    def _analyze_culture(self, text: str) -> Tuple[float, List[str]]:
        found = []
        score = 0.0
        for pattern, pts in self._culture_patterns:
            matches = pattern.findall(text)
            if matches:
                found.extend(matches)
                score += pts
        return score, sorted(set(found))

    def calculate_confidence(self, text_length: int, sections_found: int,
                             tables_count: int, cleaned_text_length: int = 0) -> float:
        effective_len = cleaned_text_length if cleaned_text_length > 0 else text_length
        base = 0.70
        if effective_len > 30000: base += 0.04
        if effective_len > 75000: base += 0.04
        if effective_len > 150000: base += 0.02
        if sections_found >= 1: base += 0.03
        if sections_found >= 3: base += 0.02
        if tables_count >= 3: base += 0.03
        if tables_count >= 8: base += 0.02
        if tables_count >= 15: base += 0.02
        return min(base, 0.92)


_analyzer: Optional[LeadershipAnalyzer] = None

def get_leadership_analyzer() -> LeadershipAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = LeadershipAnalyzer()
    return _analyzer
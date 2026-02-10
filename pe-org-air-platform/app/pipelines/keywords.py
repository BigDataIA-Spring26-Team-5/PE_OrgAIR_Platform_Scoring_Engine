# """
# AI Keywords and Tech Stack Keywords for Pipeline 2
# app/pipelines/keywords.py
# """

# from __future__ import annotations

# # AI-related keywords for job posting classification
# AI_KEYWORDS = frozenset([
#     # Core AI / ML terms
#     "artificial intelligence",
#     "machine learning",
#     "deep learning",
#     "neural network",
#     "neural networks",
#     "nlp",
#     "natural language processing",
#     "computer vision",
#     "reinforcement learning",
#     "generative ai",
#     "gen ai",
#     "genai",
#     "foundation model",
#     "foundation models",
#     "multimodal",
#     "self supervised learning",
#     "self-supervised learning",

#     # Shorthand terms (important for title-only matching)
#     "ai",
#     "ml",
#     "ai/ml",
#     "ml/ai",
#     "dl",
#     "cv",  # computer vision

#     # Models & architectures
#     "large language model",
#     "large language models",
#     "llm",
#     "llms",
#     "gpt",
#     "gpt-4",
#     "gpt-5",
#     "bert",
#     "t5",
#     "llama",
#     "mistral",
#     "claude",
#     "gemini",
#     "transformer",
#     "transformers",
#     "attention mechanism",
#     "diffusion model",
#     "diffusion models",
#     "stable diffusion",
#     "midjourney",
#     "dall-e",
#     "gan",
#     "gans",
#     "generative adversarial network",
#     "convolutional neural network",
#     "cnn",
#     "rnn",
#     "lstm",
#     "gru",
#     "vae",
#     "autoencoder",

#     # ML techniques
#     "supervised learning",
#     "unsupervised learning",
#     "semi supervised learning",
#     "transfer learning",
#     "fine tuning",
#     "fine-tuning",
#     "finetuning",
#     "post-training",
#     "post training",
#     "pretraining",
#     "pre-training",
#     "rlhf",
#     "hyperparameter tuning",
#     "feature engineering",
#     "model training",
#     "model evaluation",
#     "model deployment",
#     "model serving",
#     "model monitoring",
#     "model optimization",
#     "experiment tracking",
#     "ablation",

#     # Libraries & frameworks
#     "tensorflow",
#     "pytorch",
#     "torch",
#     "keras",
#     "scikit-learn",
#     "sklearn",
#     "hugging face",
#     "huggingface",
#     "transformers",
#     "langchain",
#     "llamaindex",
#     "llama-index",
#     "ray",
#     "mlflow",
#     "onnx",
#     "fastai",
#     "jax",
#     "flax",
#     "triton",
#     "tensorrt",
#     "vllm",
#     "tgi",
#     "deepspeed",
#     "megatron",
#     "nemo",
#     "paddlepaddle",
#     "mxnet",
#     "caffe",

#     # LLM / GenAI tooling
#     "prompt engineering",
#     "prompt engineer",
#     "rag",
#     "retrieval augmented generation",
#     "retrieval-augmented",
#     "vector database",
#     "vector db",
#     "vector search",
#     "embeddings",
#     "embedding",
#     "semantic search",
#     "faiss",
#     "pinecone",
#     "weaviate",
#     "chroma",
#     "milvus",
#     "qdrant",
#     "agents",
#     "agentic",
#     "tool use",
#     "function calling",

#     # Providers & platforms
#     "openai",
#     "anthropic",
#     "cohere",
#     "azure openai",
#     "aws sagemaker",
#     "sagemaker",
#     "google vertex ai",
#     "vertex ai",
#     "bedrock",
#     "replicate",
#     "together ai",
#     "anyscale",
#     "modal",
#     "runpod",
#     "lambda labs",

#     # Data science & analytics
#     "data science",
#     "data scientist",
#     "predictive analytics",
#     "predictive modeling",
#     "statistical modeling",
#     "time series forecasting",
#     "time series",
#     "forecasting",
#     "anomaly detection",
#     "recommendation system",
#     "recommendation engine",
#     "recommender",
#     "personalization",

#     # NLP / Vision tasks
#     "sentiment analysis",
#     "text classification",
#     "named entity recognition",
#     "ner",
#     "topic modeling",
#     "speech recognition",
#     "speech to text",
#     "text to speech",
#     "asr",
#     "tts",
#     "object detection",
#     "image recognition",
#     "image classification",
#     "image segmentation",
#     "semantic segmentation",
#     "ocr",
#     "document ai",
#     "document understanding",

#     # ML roles & job titles
#     "ml engineer",
#     "machine learning engineer",
#     "ai engineer",
#     "mlops",
#     "ml ops",
#     "aiops",
#     "ai ops",
#     "llmops",
#     "ai researcher",
#     "ml researcher",
#     "ai specialist",
#     "applied scientist",
#     "research scientist",
#     "research engineer",
#     "computer vision engineer",
#     "nlp engineer",
#     "data architect",
#     "ml architect",
#     "ai architect",
#     "ml scientist",
#     "ai scientist",

#     # Ops & infra
#     "ml pipeline",
#     "ml pipelines",
#     "training pipeline",
#     "inference pipeline",
#     "model registry",
#     "feature store",
#     "distributed training",
#     "model parallelism",
#     "data parallelism",
#     "tensor parallelism",
#     "pipeline parallelism",
#     "gpu",
#     "gpus",
#     "cuda",
#     "tpu",
#     "inference",
#     "batch inference",
#     "real-time inference",
#     "serving infrastructure",

#     # HPC / Performance (AI context)
#     "hpc",
#     "high performance computing",
#     "accelerated computing",
#     "gpu cluster",
#     "gpu clusters",
# ])

# # Tech stack keywords for AI presence scoring
# AI_TECHSTACK_KEYWORDS = frozenset([
#     # Cloud AI / ML platforms
#     "aws sagemaker",
#     "azure ml",
#     "azure machine learning",
#     "google vertex ai",
#     "aws bedrock",
#     "azure openai",
#     "google ai platform",
#     "databricks",
#     "databricks ml",
#     "snowflake",
#     "snowflake cortex",
#     "bigquery",
#     "redshift",

#     # ML / data infrastructure
#     "kubernetes",
#     "k8s",
#     "docker",
#     "containerization",
#     "mlflow",
#     "kubeflow",
#     "airflow",
#     "apache airflow",
#     "prefect",
#     "dagster",
#     "argo workflows",
#     "argo",

#     # Data processing & streaming
#     "spark",
#     "apache spark",
#     "pyspark",
#     "hadoop",
#     "kafka",
#     "apache kafka",
#     "flink",
#     "apache flink",
#     "beam",
#     "apache beam",
#     "dbt",
#     "fivetran",
#     "airbyte",

#     # Programming languages (AI-heavy usage)
#     "python",
#     "r programming",
#     "r language",
#     "julia",
#     "scala",

#     # Databases & storage
#     "postgresql",
#     "mysql",
#     "mongodb",
#     "elasticsearch",
#     "opensearch",
#     "redis",
#     "neo4j",
#     "cassandra",
#     "dynamodb",
#     "bigtable",

#     # Vector databases / semantic search
#     "vector database",
#     "vector db",
#     "pinecone",
#     "weaviate",
#     "milvus",
#     "qdrant",
#     "chroma",
#     "faiss",

#     # Model serving & inference
#     "model serving",
#     "model inference",
#     "torchserve",
#     "tensorflow serving",
#     "triton inference server",
#     "bentoml",
#     "seldon",
#     "kserve",

#     # Visualization / BI
#     "tableau",
#     "power bi",
#     "looker",
#     "superset",
#     "metabase",

#     # Version control / MLOps tooling
#     "git",
#     "github",
#     "gitlab",
#     "bitbucket",
#     "dvc",
#     "wandb",
#     "weights and biases",
#     "neptune",
#     "clearml",

#     # Experimentation & monitoring
#     "experiment tracking",
#     "model monitoring",
#     "data drift",
#     "concept drift",

#     # AI-specific compute & distributed systems
#     "gpu",
#     "gpu cluster",
#     "nvidia",
#     "cuda",
#     "cudnn",
#     "ray",
#     "dask",
#     "horovod",
#     "distributed training",
# ])

# # Leadership background keywords for DEF 14A parsing
# AI_LEADERSHIP_KEYWORDS = frozenset([
#     # Titles
#     "chief data officer",
#     "cdo",
#     "chief analytics officer",
#     "chief ai officer",
#     "caio",
#     "chief technology officer",
#     "cto",
#     "vp of data",
#     "vp of ai",
#     "vp of engineering",
#     "head of data science",
#     "head of ai",
#     "head of ml",
#     "data science background",
#     "ai experience",
#     "machine learning",
#     "phd in computer science",
#     "stanford ai",
#     "mit ai",
#     "google ai",
#     "meta ai",
#     "deepmind",
#     "openai",
#     "anthropic",
#     "computer science degree",
#     "statistics degree",
#     "mathematics degree",
#     "engineering degree",
# ])

# # Patent classification keywords for PatentsView
# PATENT_AI_KEYWORDS = frozenset([
#     "artificial intelligence",
#     "machine learning",
#     "neural network",
#     "deep learning",
#     "natural language processing",
#     "computer vision",
#     "pattern recognition",
#     "automated decision",
#     "predictive model",
#     "classification algorithm",
#     "clustering algorithm",
#     "recommendation engine",
#     "speech recognition",
#     "image processing",
#     "data mining",
#     "knowledge graph",
# ])

# # Top AI tools for bonus scoring in tech stack
# TOP_AI_TOOLS = frozenset([
#     "tensorflow",
#     "pytorch",
#     "kubernetes",
#     "spark",
#     "databricks",
#     "aws sagemaker",
#     "mlflow",
#     "hugging face",
# ])
"""
AI Keywords and Tech Stack Keywords for Pipeline 2
app/pipelines/keywords.py

ALIGNED WITH CASE STUDY 2 PDF SPEC (pages 14-16).

Key changes from previous version:
  - AI_KEYWORDS now uses multi-word phrases to avoid false positives
  - Removed single-word terms that match addresses/cities/generic business text
    (e.g. "bert", "modal", "spark", "forecasting", "ray", "agents", "gpu", "ai", "ml")
  - Added TECH_JOB_TITLE_KEYWORDS for _is_tech_job() filter (PDF page 16)
  - AI_SKILLS list added per PDF spec (page 15) for diversity scoring
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# TECH JOB TITLE KEYWORDS (PDF page 16)
# Used by _is_tech_job() to filter tech roles BEFORE calculating AI ratio.
# Matched against job TITLE only.
# ---------------------------------------------------------------------------
TECH_JOB_TITLE_KEYWORDS = [
    "engineer",
    "developer",
    "programmer",
    "software",
    "data",
    "analyst",
    "scientist",
    "technical",
    "architect",
    "devops",
    "sre",
    "machine learning",
    "cloud",
    "platform",
    "infrastructure",
    "security",
    "product manager",
    "technology",
]


# ---------------------------------------------------------------------------
# AI KEYWORDS (PDF page 14 — expanded with safe multi-word terms)
# Used to classify a job posting as AI-related.
# Matched against title + description.
#
# RULE: Every keyword must be specific enough to NOT match:
#   - Street addresses (e.g. "BERT KOUNS BLVD")
#   - City/state names (e.g. "Sparks, NV")
#   - Generic business language (e.g. "budgeting and forecasting")
#   - Training modalities for truck drivers (e.g. "leveraging different modalities")
# ---------------------------------------------------------------------------
AI_KEYWORDS = frozenset([
    # ---- Core terms from PDF page 14 ----
    "machine learning",
    "ml engineer",
    "data scientist",
    "artificial intelligence",
    "deep learning",
    "natural language processing",
    "computer vision",
    "mlops",
    "ai engineer",
    "pytorch",
    "tensorflow",
    "large language model",
    "large language models",

    # ---- Safe multi-word expansions ----
    # Core AI/ML
    "neural network",
    "neural networks",
    "reinforcement learning",
    "generative ai",
    "foundation model",
    "foundation models",
    "self-supervised learning",

    # Specific model names (multi-word or unambiguous)
    "large language model",
    "stable diffusion",
    "generative adversarial network",
    "convolutional neural network",
    "recurrent neural network",

    # ML techniques (multi-word only)
    "supervised learning",
    "unsupervised learning",
    "semi-supervised learning",
    "transfer learning",
    "fine-tuning",
    "model training",
    "model deployment",
    "model serving",
    "hyperparameter tuning",
    "feature engineering",
    "distributed training",
    "prompt engineering",
    "retrieval augmented generation",

    # Specific frameworks/tools (unambiguous)
    "scikit-learn",
    "hugging face",
    "huggingface",
    "langchain",
    "llamaindex",
    "llama-index",
    "aws sagemaker",
    "azure openai",
    "google vertex ai",
    "vertex ai",

    # Roles (multi-word)
    "machine learning engineer",
    "ai researcher",
    "ml researcher",
    "applied scientist",
    "research scientist",
    "research engineer",
    "computer vision engineer",
    "nlp engineer",
    "ml architect",
    "ai architect",
    "data science",
    "ai specialist",

    # NLP/Vision tasks (multi-word)
    "sentiment analysis",
    "named entity recognition",
    "object detection",
    "image recognition",
    "image classification",
    "image segmentation",
    "speech recognition",
    "text classification",
    "topic modeling",
    "anomaly detection",
    "recommendation system",
    "recommendation engine",
    "predictive modeling",
    "predictive analytics",
    "time series forecasting",

    # Infrastructure (multi-word, unambiguous)
    "vector database",
    "vector search",
    "semantic search",
    "ml pipeline",
    "ml pipelines",
    "training pipeline",
    "inference pipeline",
    "feature store",
    "model registry",
    "gpu cluster",
    "gpu clusters",
    "batch inference",
    "real-time inference",
    "serving infrastructure",
    "experiment tracking",
])


# ---------------------------------------------------------------------------
# AI SKILLS (PDF page 15)
# Used for diversity scoring — counts unique skills found across all postings.
# These are specific tool/technology names unlikely to cause false positives.
# ---------------------------------------------------------------------------
AI_SKILLS = frozenset([
    "python",
    "pytorch",
    "tensorflow",
    "scikit-learn",
    "spark",
    "hadoop",
    "kubernetes",
    "docker",
    "aws sagemaker",
    "azure ml",
    "gcp vertex",
    "huggingface",
    "langchain",
    "openai",
])


# ---------------------------------------------------------------------------
# AI TECHSTACK KEYWORDS
# Used by tech_signals.py for digital_presence scoring.
# These are scanned in job descriptions for tech stack evidence.
# Keep specific multi-word terms where possible.
# ---------------------------------------------------------------------------
AI_TECHSTACK_KEYWORDS = frozenset([
    # Cloud AI/ML platforms
    "aws sagemaker",
    "azure ml",
    "azure machine learning",
    "google vertex ai",
    "aws bedrock",
    "azure openai",
    "databricks",
    "snowflake",
    "snowflake cortex",
    "bigquery",

    # ML/data infrastructure
    "kubernetes",
    "docker",
    "mlflow",
    "kubeflow",
    "apache airflow",
    "prefect",
    "dagster",

    # Data processing
    "apache spark",
    "pyspark",
    "apache kafka",
    "apache flink",
    "apache beam",

    # Programming languages
    "python",
    "scala",
    "julia",

    # Databases
    "postgresql",
    "mongodb",
    "elasticsearch",
    "redis",
    "neo4j",

    # Vector databases
    "pinecone",
    "weaviate",
    "milvus",
    "qdrant",
    "chroma",

    # Model serving
    "triton inference server",
    "torchserve",
    "tensorflow serving",
    "bentoml",

    # Visualization/BI
    "tableau",
    "power bi",
    "looker",
])


# ---------------------------------------------------------------------------
# Leadership keywords for DEF 14A parsing
# ---------------------------------------------------------------------------
AI_LEADERSHIP_KEYWORDS = frozenset([
    "chief data officer",
    "chief analytics officer",
    "chief ai officer",
    "chief technology officer",
    "vp of data",
    "vp of ai",
    "vp of engineering",
    "head of data science",
    "head of ai",
    "head of ml",
    "machine learning",
    "phd in computer science",
    "data science background",
    "ai experience",
    "computer science degree",
])


# ---------------------------------------------------------------------------
# Patent classification keywords for PatentsView
# ---------------------------------------------------------------------------
PATENT_AI_KEYWORDS = frozenset([
    "artificial intelligence",
    "machine learning",
    "neural network",
    "deep learning",
    "natural language processing",
    "computer vision",
    "pattern recognition",
    "automated decision",
    "predictive model",
    "classification algorithm",
    "clustering algorithm",
    "recommendation engine",
    "speech recognition",
    "image processing",
    "data mining",
    "knowledge graph",
])
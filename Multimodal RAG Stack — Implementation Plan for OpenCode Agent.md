# Multimodal RAG Stack — Executive Summary & Implementation Plan
## Executive Summary
This document defines the full implementation plan for upgrading the existing LightRAG-based retrieval stack to a multimodal, high-speed RAG pipeline. The upgrade targets two outcomes: (1) richer document understanding via MinerU-based multimodal parsing through RAG-Anything, and (2) lower, more consistent chat latency via hybrid retrieval, caching, and query routing.

The current stack achieves ~0.96s RAG query time but suffers from tail latency spikes (15–20s) caused by oversized or poorly structured context being passed to the LLM. The proposed stack replaces the ingestion and retrieval layers without replacing LightRAG itself — the existing LightRAG instance, MCP server, and LiteLLM routing are preserved and extended.[^1]

**Target outcome:** <200ms retrieval, <5s consistent total chat response, elimination of timeout spikes in Chatwoot and NocoBase widgets.[^2][^1]

***
## Architecture Overview
```
┌─────────────────────────────────────────────────────────┐
│                    INGESTION PIPELINE                   │
│                                                         │
│  Nextcloud / OnlyOffice / paperless-ngx                 │
│          ↓ (file watcher / webhook)                     │
│  RAG-Anything process_document_complete()               │
│    ├─ MinerU 2.x  (PDF, DOCX, PPTX, XLSX via LibreOffice│
│    ├─ ImageModalProcessor  (VLM context injection)      │
│    ├─ TableModalProcessor  (HTML → semantic)            │
│    └─ EquationModalProcessor  (LaTeX → symbolic)        │
│          ↓                                              │
│  LightRAG KG  (existing instance, injected)             │
│  Milvus 2.6   (dense + sparse vectors, HNSW index)      │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                    RETRIEVAL PIPELINE                   │
│                                                         │
│  Chat widget query (Chatwoot / NocoBase /ask)           │
│          ↓                                              │
│  [Query Router]  ── chit-chat/simple → LLM direct       │
│          ↓ complex / knowledge query                    │
│  [Redis Cache]   ── cache hit → return cached chunks    │
│          ↓ cache miss                                   │
│  [bge-m3 Embedding]  (dense + sparse in one model)      │
│          ↓                                              │
│  [Milvus Hybrid Search]  BM25 + ANN in single query     │
│          ↓                                              │
│  [LightRAG KG Traversal]  (low + high level)            │
│          ↓                                              │
│  [RRF Fusion]  (math, no model)                         │
│          ↓                                              │
│  [Cross-Encoder Re-ranker]  (MiniLM, GPU, ~50ms)        │
│          ↓                                              │
│  [LLM via LiteLLM]  (streamed)                          │
│          ↓                                              │
│  Chat widget response                                   │
└─────────────────────────────────────────────────────────┘
```

***
## Phase 1 — Ingestion Layer (RAG-Anything + MinerU)
### 1.1 Install RAG-Anything and MinerU
```bash
# In the RAG/document processing GPU server container
pip install raganything
pip install magic-pdf[full]  # MinerU 2.x

# System-level LibreOffice for Office doc conversion
apt-get install -y libreoffice

# Optional extras
pip install raganything[image]   # BMP/TIFF/WebP support
pip install raganything[text]    # .txt / .md ingestion via ReportLab
```

MinerU requires a first-run model download. Run once interactively:[^3]

```bash
python -c "from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze; print('MinerU ready')"
```
### 1.2 Wire RAG-Anything to Existing LightRAG Instance
RAG-Anything accepts an existing LightRAG instance — do not create a second one.[^4]

```python
from raganything import RAGAnything
from lightrag import LightRAG  # your existing instance

# Import your existing configured LightRAG
from your_lightrag_setup import lightrag_instance

rag = RAGAnything(
    lightrag=lightrag_instance,         # inject existing instance
    llm_model_func=your_llm_func,       # same LiteLLM wrapper you use today
    vision_model_func=your_vlm_func,    # VLM for image context (e.g. LLaVA)
    embedding_func=your_embed_func,     # bge-m3 via Ollama
)
```
### 1.3 File Watcher — Nextcloud / OnlyOffice Bridge
Create a lightweight watchdog service that monitors the Nextcloud data directory and triggers ingestion on new or modified files.[^5][^6]

```python
# file_watcher.py
import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from raganything import RAGAnything

SUPPORTED_EXTENSIONS = {'.pdf', '.docx', '.pptx', '.xlsx', '.doc', '.png', '.jpg', '.txt', '.md'}
NEXTCLOUD_WATCH_PATH = "/mnt/nextcloud/data/IntegritasMRV/files"

class DocumentHandler(FileSystemEventHandler):
    def __init__(self, rag: RAGAnything):
        self.rag = rag

    def on_created(self, event):
        if not event.is_directory:
            ext = Path(event.src_path).suffix.lower()
            if ext in SUPPORTED_EXTENSIONS:
                asyncio.run(self.rag.process_document_complete(event.src_path))

    def on_modified(self, event):
        self.on_created(event)  # re-ingest on modification

if __name__ == "__main__":
    rag = RAGAnything(...)  # configured instance
    observer = Observer()
    observer.schedule(DocumentHandler(rag), NEXTCLOUD_WATCH_PATH, recursive=True)
    observer.start()
```

Deploy as a Docker service on the RAG server alongside the existing LightRAG container.
### 1.4 Batch Ingest Existing Corpus
For the initial IntegritasMRV corpus (~10,000 documents in paperless-ngx):

```python
await rag.process_folder_complete(
    folder_path="/export/integritasmrv_docs",
    output_dir="/tmp/raganything_output",
    max_workers=4,  # tune to GPU VRAM
    recursive=True
)
```

Run this once as a migration job. Expect ~2–5 minutes per 100 documents depending on PDF complexity.

***
## Phase 2 — Vector Store (Milvus 2.6 + HNSW + Hybrid Search)
### 2.1 Deploy Milvus via Docker (Coolify)
```yaml
# docker-compose.milvus.yml
services:
  etcd:
    image: quay.io/coreos/etcd:v3.5.5
    environment:
      - ETCD_AUTO_COMPACTION_MODE=revision
      - ETCD_AUTO_COMPACTION_RETENTION=1000
      - ETCD_QUOTA_BACKEND_BYTES=4294967296
    volumes:
      - etcd_data:/etcd

  minio:
    image: minio/minio:RELEASE.2023-03-13T19-46-17Z
    environment:
      MINIO_ACCESS_KEY: minioadmin
      MINIO_SECRET_KEY: minioadmin
    command: minio server /minio_data
    volumes:
      - minio_data:/minio_data

  milvus:
    image: milvusdb/milvus:v2.6.0
    command: ["milvus", "run", "standalone"]
    environment:
      ETCD_ENDPOINTS: etcd:2379
      MINIO_ADDRESS: minio:9000
    ports:
      - "19530:19530"
    depends_on:
      - etcd
      - minio
    volumes:
      - milvus_data:/var/lib/milvus

volumes:
  etcd_data:
  minio_data:
  milvus_data:
```
### 2.2 Create Hybrid Collection (Dense + Sparse)
Milvus 2.6 supports dense and sparse vectors in a single collection, enabling unified hybrid search from one query call.[^7]

```python
from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://milvus:19530")

schema = client.create_schema()
schema.add_field("id", DataType.INT64, is_primary=True, auto_id=True)
schema.add_field("text", DataType.VARCHAR, max_length=4096)
schema.add_field("dense_vector", DataType.FLOAT_VECTOR, dim=1024)   # bge-m3 dense
schema.add_field("sparse_vector", DataType.SPARSE_FLOAT_VECTOR)      # bge-m3 sparse
schema.add_field("doc_id", DataType.VARCHAR, max_length=256)
schema.add_field("modality", DataType.VARCHAR, max_length=64)         # text/image/table/equation

index_params = client.prepare_index_params()
index_params.add_index("dense_vector", index_type="HNSW", metric_type="COSINE",
                       params={"M": 16, "efConstruction": 200})
index_params.add_index("sparse_vector", index_type="SPARSE_INVERTED_INDEX",
                       metric_type="IP")

client.create_collection("rag_chunks", schema=schema, index_params=index_params)
```
### 2.3 bge-m3 Embedding via Ollama
`bge-m3` produces both dense and sparse embeddings from a single model call, replacing a separate BM25 index.[^8][^7]

```bash
ollama pull bge-m3
```

```python
from ollama import Client

ollama = Client(host="http://ollama:11434")

def embed(text: str) -> dict:
    response = ollama.embeddings(model="bge-m3", prompt=text)
    return {
        "dense": response["embedding"],        # 1024-dim float list
        "sparse": response.get("sparse", {})   # token → weight dict
    }
```

***
## Phase 3 — Retrieval Pipeline
### 3.1 Redis Cache Layer
```python
import redis
import hashlib, json

redis_client = redis.Redis(host="redis", port=6379, db=1)
CACHE_TTL = 3600  # 1 hour

def cache_key(query: str) -> str:
    return f"rag:cache:{hashlib.sha256(query.encode()).hexdigest()}"

async def cached_retrieve(query: str, retrieve_fn) -> list:
    key = cache_key(query)
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    result = await retrieve_fn(query)
    redis_client.setex(key, CACHE_TTL, json.dumps(result))
    return result
```
### 3.2 Query Router
Route chit-chat and trivial queries directly to the LLM, bypassing retrieval entirely.[^9][^7]

```python
DIRECT_PATTERNS = [
    r"^(hi|hello|hey|bonjour|salut)",
    r"^(merci|thanks|thank you|ok|okay|yes|no|non|oui)$",
    r"^\d+[\+\-\*/]\d+",  # arithmetic
]

def needs_retrieval(query: str) -> bool:
    import re
    q = query.strip().lower()
    for pattern in DIRECT_PATTERNS:
        if re.match(pattern, q):
            return False
    return True
```
### 3.3 Hybrid Search with RRF Fusion
```python
from pymilvus import AnnSearchRequest, RRFRanker, WeightedRanker

async def hybrid_retrieve(query: str, top_k: int = 10) -> list:
    vectors = embed(query)

    dense_req = AnnSearchRequest(
        data=[vectors["dense"]],
        anns_field="dense_vector",
        param={"metric_type": "COSINE", "params": {"ef": 100}},
        limit=top_k
    )

    sparse_req = AnnSearchRequest(
        data=[vectors["sparse"]],
        anns_field="sparse_vector",
        param={"metric_type": "IP"},
        limit=top_k
    )

    results = client.hybrid_search(
        collection_name="rag_chunks",
        reqs=[dense_req, sparse_req],
        ranker=RRFRanker(k=60),   # RRF fusion, no model needed
        limit=top_k,
        output_fields=["text", "doc_id", "modality"]
    )
    return results
```
### 3.4 LightRAG KG Traversal
Run LightRAG retrieval in parallel with Milvus hybrid search, then merge results before re-ranking.[^10][^11]

```python
import asyncio

async def full_retrieve(query: str) -> list:
    # Parallel execution
    milvus_results, kg_results = await asyncio.gather(
        hybrid_retrieve(query, top_k=10),
        lightrag_instance.aquery(query, param=QueryParam(mode="hybrid"))
    )
    # Merge and deduplicate by doc_id
    merged = merge_results(milvus_results, kg_results)
    return merged
```
### 3.5 Cross-Encoder Re-Ranker
```bash
pip install sentence-transformers
```

```python
from sentence_transformers import CrossEncoder

reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", device="cuda")

def rerank(query: str, candidates: list, top_n: int = 5) -> list:
    pairs = [(query, c["text"]) for c in candidates]
    scores = reranker.predict(pairs)
    ranked = sorted(zip(candidates, scores), key=lambda x: x[^1], reverse=True)
    return [c for c, _ in ranked[:top_n]]
```
### 3.6 Full Retrieval Orchestrator
```python
async def retrieve_for_chat(query: str) -> list:
    # 1. Route check
    if not needs_retrieval(query):
        return []

    # 2. Cache check
    async def _retrieve():
        candidates = await full_retrieve(query)
        return rerank(query, candidates, top_n=5)

    return await cached_retrieve(query, _retrieve)
```

***
## Phase 4 — FastAPI Endpoint (/ask)
Wire the full pipeline into the existing FastAPI service for NocoBase and Chatwoot.

```python
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

@app.post("/ask")
async def ask(body: dict):
    query = body["query"]
    chunks = await retrieve_for_chat(query)

    context = "\n\n".join([c["text"] for c in chunks])
    prompt = f"""Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"""

    async def stream():
        async for token in litellm_stream(prompt, max_tokens=300):
            yield token

    return StreamingResponse(stream(), media_type="text/event-stream")
```

**Always stream.** Streaming drops perceived latency from 4–8s to first-word-in-<1s.[^12][^9]

***
## Phase 5 — Docker Compose Integration
Add new services to the existing Coolify stack:

```yaml
services:
  raganything-worker:
    build: ./raganything
    volumes:
      - /mnt/nextcloud:/mnt/nextcloud:ro
      - raganything_output:/tmp/raganything_output
    environment:
      - LIGHTRAG_URL=http://lightrag:8020
      - OLLAMA_URL=http://ollama:11434
      - MILVUS_URI=http://milvus:19530
      - REDIS_URL=redis://redis:6379/1
    depends_on:
      - milvus
      - redis
      - ollama
    deploy:
      resources:
        reservations:
          devices:
            - capabilities: [gpu]

  file-watcher:
    build: ./file-watcher
    volumes:
      - /mnt/nextcloud:/mnt/nextcloud:ro
    environment:
      - RAGANYTHING_API=http://raganything-worker:8080
    restart: always

  milvus:
    image: milvusdb/milvus:v2.6.0
    # (full config in Phase 2)

  reranker-api:
    build: ./reranker
    # wraps CrossEncoder as a microservice
    deploy:
      resources:
        reservations:
          devices:
            - capabilities: [gpu]
```

***
## Expected Performance
| Metric | Current | After Implementation |
|---|---|---|
| RAG retrieval time | ~960ms[^1] | ~70–200ms[^13][^2] |
| Cache hit latency | None | <10ms[^2] |
| Total first token (fresh query) | 4–8s (good), 15–20s (spikes)[^1] | 2.5–5s consistent |
| LLM context quality | Raw text chunks | Modality-aware, structured |
| Document types supported | PDF (plain text) | PDF, DOCX, PPTX, XLSX, images, tables, equations[^4] |
| Tail latency spikes | Yes (timeouts in Chatwoot) | Eliminated via cache + routing |

***
## Dependencies Summary
| Package | Install | Purpose |
|---|---|---|
| `raganything` | `pip install raganything` | Multimodal ingestion pipeline[^4] |
| `magic-pdf[full]` | `pip install magic-pdf[full]` | MinerU 2.x PDF parser[^3] |
| `libreoffice` | `apt-get install libreoffice` | Office doc conversion[^4] |
| `raganything[image]` | `pip install raganything[image]` | Pillow — BMP/TIFF/WebP[^4] |
| `raganything[text]` | `pip install raganything[text]` | ReportLab — .txt/.md ingestion[^4] |
| `pymilvus` | `pip install pymilvus` | Milvus client[^7] |
| `sentence-transformers` | `pip install sentence-transformers` | Cross-encoder re-ranker[^8] |
| `watchdog` | `pip install watchdog` | Nextcloud file watcher |
| `redis` | `pip install redis` | Cache client |
| `bge-m3` | `ollama pull bge-m3` | Dense + sparse embeddings[^8] |

***
## Implementation Order
1. **Deploy Milvus** on RAG server (standalone Docker) and verify connectivity
2. **Install MinerU + RAG-Anything**, run model download, test on one PDF
3. **Batch ingest** existing IntegritasMRV corpus (one-time migration job)
4. **Deploy file watcher** pointed at Nextcloud data volume
5. **Implement retrieval orchestrator** (hybrid search + RRF + re-ranker)
6. **Add Redis cache** and query router
7. **Update /ask FastAPI endpoint** to use new retrieval, enable streaming
8. **Test Chatwoot + NocoBase** widgets for latency and quality regression
9. **Monitor** Milvus collection growth and re-ranker GPU utilization

---

## References

1. [So how improve the speed ?
With another model?
Better feeding the rag ?
Optimising model ?
…?](https://www.perplexity.ai/search/e32d326c-5e5a-495b-8e2a-545bf8cc4dbf) - Given your measured numbers from this session, here's a precise breakdown of where time goes and wha...

2. [What is an acceptable latency for a RAG system in an interactive ...](https://milvus.io/ai-quick-reference/what-is-an-acceptable-latency-for-a-rag-system-in-an-interactive-setting-eg-a-chatbot-and-how-do-we-ensure-both-retrieval-and-generation-phases-meet-this-target) - An acceptable latency for a RAG (Retrieval-Augmented Generation) system is typically 1-2 seconds tot...

3. [GitHub - opendatalab/MinerU: Transforms complex documents like ...](https://github.com/opendatalab/mineru) - Added mineru-router , designed for unified entry deployment and task routing across multiple service...

4. [raganything - PyPI](https://pypi.org/project/raganything/) - Installation. Option 1: Install from PyPI (Recommended). # Basic installation pip install raganythin...

5. [Does RAG Anything supports multiple documents? #96 - GitHub](https://github.com/HKUDS/RAG-Anything/discussions/96) - RAG-Anything supports multiple formats simultaneously: PDFs: Research papers, reports; Office Docs: ...

6. [Controlling Your Cloud: Integrating Nextcloud Into Your Workflow](https://gideonwolfe.com/posts/sysadmin/nextcloud/nextcloudworkflow/) - Link this with the Nextcloud client and choose which address books and calendars you want to synchro...

7. [Build Smarter RAG with Routing and Hybrid Retrieval - Milvus Blog](https://milvus.io/blog/build-smarter-rag-routing-hybrid-retrieval.md) - Use unified hybrid retrieval. Maintaining separate BM25 and vector search systems doubles storage co...

8. [How to Build a Robust Hybrid RAG Pipeline for Better Search Results](https://h3sync.com/blog/how-to-build-a-robust-hybrid-rag-pipeline-for-better-search-results/) - By integrating inverted indices based on TF-IDF principles, specifically the Okapi BM25 algorithm, w...

9. [5 Proven Strategies to Improve Chatbot Response Accuracy with ...](https://www.chatrag.ai/blog/2026-01-02-5-proven-strategies-to-improve-chatbot-response-accuracy-with-rag-in-2025) - Learn how to improve chatbot response accuracy with RAG using proven techniques like query optimizat...

10. [LightRAG Explained: Fast, Cost-Effective AI Retrieval - AIQuinta](https://aiquinta.ai/blog/lightrag-core-architecture-and-benefits/) - LightRAG is a lightweight Retrieval-Augmented Generation architecture designed to improve AI accurac...

11. [LightRAG: Simple and Fast Retrieval-Augmented Generation - arXiv](https://arxiv.org/abs/2410.05779) - This innovative framework employs a dual-level retrieval system that enhances comprehensive informat...

12. [Week 4: Optimization and Production Day 22: RAG System Latency ...](https://www.linkedin.com/pulse/week-4-optimization-production-day-22-rag-system-latency-marques-fjspe) - The Latency Challenge ... An acceptable latency for a RAG system in an interactive setting like a ch...

13. [Optimizing RAG Pipelines: Strategies for High-Speed AI Retrieval ...](https://www.linkedin.com/pulse/optimizing-rag-pipelines-strategies-high-speed-ai-retrieval-r-nrkwc) - TL;DR. RAG pairs retrieval (search) with generation (text creation). · Key metrics to watch: Latency...


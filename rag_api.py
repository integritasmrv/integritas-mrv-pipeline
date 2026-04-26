import subprocess
import json
import os
import tempfile
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import uuid

app = FastAPI(title="RAG Multimodal API")

MINERU_OUTPUT = "/tmp/mineru_output"
NEXTCLOUD_WATCH = "/mnt/nextcloud/data"

class IngestRequest(BaseModel):
    file_path: Optional[str] = None
    backend: str = "pipeline"

class IngestResponse(BaseModel):
    success: bool
    document_id: str
    output_path: str
    chunks: int
    message: str

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "rag-multimodal"}

@app.post("/ingest", response_model=IngestResponse)
async def ingest_document(
    file: UploadFile = File(...),
    backend: str = "pipeline"
):
    try:
        doc_id = str(uuid.uuid4())
        with tempfile.NamedTemporaryFile(delete=False, suffix=file.filename) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        output_dir = f"{MINERU_OUTPUT}/{doc_id}"
        os.makedirs(output_dir, exist_ok=True)
        
        result = subprocess.run(
            ["mineru", "-p", tmp_path, "-o", output_dir, "-b", backend],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        os.unlink(tmp_path)
        
        md_file = f"{output_dir}/auto/{file.filename.rsplit('.', 1)[0]}.md"
        chunks = 0
        if os.path.exists(md_file):
            with open(md_file, 'r') as f:
                content = f.read()
                chunks = len(content.split('\n\n'))
        
        return IngestResponse(
            success=result.returncode == 0,
            document_id=doc_id,
            output_path=output_dir,
            chunks=chunks,
            message=result.stdout[-500:] if result.stdout else result.stderr[-500:] if result.stderr else "Completed"
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Processing timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/ingest/path", response_model=IngestResponse)
async def ingest_by_path(request: IngestRequest):
    try:
        if not request.file_path:
            raise HTTPException(status_code=400, detail="file_path required")
        
        doc_id = str(uuid.uuid4())
        filename = os.path.basename(request.file_path)
        output_dir = f"{MINERU_OUTPUT}/{doc_id}"
        os.makedirs(output_dir, exist_ok=True)
        
        result = subprocess.run(
            ["mineru", "-p", request.file_path, "-o", output_dir, "-b", request.backend],
            capture_output=True,
            text=True,
            timeout=300
        )
        
        md_file = f"{output_dir}/auto/{filename.rsplit('.', 1)[0]}.md"
        chunks = 0
        if os.path.exists(md_file):
            with open(md_file, 'r') as f:
                content = f.read()
                chunks = len(content.split('\n\n'))
        
        return IngestResponse(
            success=result.returncode == 0,
            document_id=doc_id,
            output_path=output_dir,
            chunks=chunks,
            message=result.stdout[-500:] if result.stdout else result.stderr[-500:] if result.stderr else "Completed"
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=408, detail="Processing timeout")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
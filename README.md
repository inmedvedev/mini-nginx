## Installation and Setup

### Create Virtual Environment

```bash
python3 -m venv .venv
```

### Activate Virtual Environment

```bash
source .venv/bin/activate
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run the Project

```bash
python main.py
```

### Run upstreams

```bash
uvicorn tests.upstream:app --host 127.0.0.1 --port 9001 --workers 1
uvicorn tests.upstream:app --host 127.0.0.1 --port 9002 --workers 1
```

### Test the Proxy

```bash
curl -v http://127.0.0.1:8888/
```
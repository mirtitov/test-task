#!/usr/bin/env python3

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

STORAGE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "records.json")
DATA_DIR = os.path.dirname(STORAGE_FILE)


def load_records():
    if not os.path.exists(STORAGE_FILE):
        return {}
    try:
        with open(STORAGE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_records(records):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STORAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


_records = {}
_lock = threading.Lock()


def get_all_records():
    with _lock:
        return dict(_records)


def get_record(record_id):
    with _lock:
        return _records.get(record_id)


def put_record(record_id, data):
    with _lock:
        _records[record_id] = {"id": record_id, "data": data}
        save_records(_records)


def init_storage():
    global _records
    with _lock:
        _records = load_records()


def send_json(handler, status, body):
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.end_headers()
    handler.wfile.write(json.dumps(body, ensure_ascii=False).encode("utf-8"))


def send_error(handler, status, message):
    send_json(handler, status, {"error": message})


class RecordHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        if path == "/records":
            qs = parse_qs(parsed.query)
            ids_param = qs.get("ids", [None])[0]
            if ids_param is None or ids_param.strip() == "":
                records = get_all_records()
                send_json(self, 200, {"records": list(records.values()), "count": len(records)})
            else:
                want_ids = [s.strip() for s in ids_param.split(",") if s.strip()]
                all_records = get_all_records()
                found = [all_records[rid] for rid in want_ids if rid in all_records]
                send_json(self, 200, {"records": found, "count": len(found)})
            return
        if path.startswith("/records/"):
            record_id = path[len("/records/"):].strip()
            if not record_id:
                send_error(self, 400, "Missing record id")
                return
            record = get_record(record_id)
            if record is None:
                send_error(self, 404, f"Record not found: {record_id}")
                return
            send_json(self, 200, record)
            return
        send_error(self, 404, "Not found")

    def do_POST(self):
        if self.path.rstrip("/") != "/records":
            send_error(self, 404, "Not found")
            return
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length <= 0:
            send_error(self, 400, "Body required")
            return
        try:
            body = self.rfile.read(content_length).decode("utf-8")
            payload = json.loads(body)
        except (ValueError, json.JSONDecodeError):
            send_error(self, 400, "Invalid JSON")
            return
        record_id = payload.get("id")
        if not record_id or not isinstance(record_id, str):
            send_error(self, 400, "Field 'id' (string) required")
            return
        data = payload.get("data")
        put_record(record_id, data)
        send_json(self, 201, {"id": record_id, "data": data})

    def log_message(self, format, *args):
        print(format % args)


def run(host="0.0.0.0", port=8080):
    init_storage()
    server = ThreadingHTTPServer((host, port), RecordHandler)
    print(f"Server running at http://{host}:{port}")
    print("  POST /records     — save record")
    print("  GET  /records/<id> — get record by id")
    print("  GET  /records     — all records; GET /records?ids=id1,id2 — selected")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="HTTP record server")
    p.add_argument("--host", default="0.0.0.0", help="Host")
    p.add_argument("--port", type=int, default=8080, help="Port")
    args = p.parse_args()
    run(host=args.host, port=args.port)

"""Build a pinned Redis server locally when no system redis-server is installed."""
import hashlib
import io
import json
import shutil
import subprocess
import tarfile
import urllib.request
from pathlib import Path
root=Path(__file__).resolve().parents[1];cache=root/'.cache';cache.mkdir(exist_ok=True)
if shutil.which('redis-server'):print('Using system redis-server');raise SystemExit(0)
url='https://codeload.github.com/redis/redis/tar.gz/refs/tags/7.2.7';raw=urllib.request.urlopen(url,timeout=60).read()
(cache/'source.json').write_text(json.dumps({'url':url,'sha256':hashlib.sha256(raw).hexdigest()},indent=2))
with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as archive:archive.extractall(cache,filter='data')
subprocess.run(['make','-j2','MALLOC=libc','BUILD_TLS=no','redis-server'],cwd=cache/'redis-7.2.7',check=True,stdout=subprocess.DEVNULL)
print('Redis server built')

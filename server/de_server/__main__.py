"""``python -m de_server`` — run behind nginx; pass --proxy-headers to uvicorn."""

import uvicorn

from de_server.app import create_app
from de_server.config import from_env

if __name__ == "__main__":
    uvicorn.run(create_app(from_env()), host="127.0.0.1", port=8100,
                proxy_headers=True, forwarded_allow_ips="127.0.0.1")

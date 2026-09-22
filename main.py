"""服务入口：``uvicorn main:app``。"""

from app import create_app  # noqa: E402

app = create_app()

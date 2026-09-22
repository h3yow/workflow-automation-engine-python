"""服务入口：uvicorn main:app。"""

from workflow_engine.api import create_app

app = create_app()

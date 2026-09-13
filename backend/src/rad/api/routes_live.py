"""Live dashboard WebSocket."""

from fastapi import APIRouter, WebSocket

from rad.api.services import Services

router = APIRouter(tags=["live"])


@router.websocket("/ws/live")
async def live(websocket: WebSocket) -> None:
    services: Services = websocket.app.state.services
    await services.hub.serve(websocket)

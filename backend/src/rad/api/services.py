"""Long-lived objects created at startup and shared by all requests."""

from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Request

from rad.common.config import Settings
from rad.db.pool import DbPool
from rad.processing.writer import BatchWriter


@dataclass(slots=True)
class Services:
    settings: Settings
    pool: DbPool
    writer: BatchWriter


def get_services(request: Request) -> Services:
    services: Services = request.app.state.services
    return services


ServicesDep = Annotated[Services, Depends(get_services)]

"""Clearance event contract, schema_version 1 (TRD §2)."""

from datetime import UTC
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from ccp.common.refdata import HS_PATTERN, PORT_PATTERN

SUPPORTED_VERSIONS = frozenset({1})


class EventType(StrEnum):
    DECLARATION_FILED = "DECLARATION_FILED"
    DOCUMENTS_VERIFIED = "DOCUMENTS_VERIFIED"
    INSPECTION_HOLD = "INSPECTION_HOLD"
    CLEARED = "CLEARED"
    RELEASED = "RELEASED"
    REJECTED = "REJECTED"


class _Payload(BaseModel):
    # Strict types, unknown keys dropped: additive changes stay compatible.
    model_config = ConfigDict(strict=True, extra="ignore")


class DeclarationFiled(_Payload):
    port_code: str = Field(pattern=PORT_PATTERN)
    hs_code: str = Field(pattern=HS_PATTERN)
    declarant: str = Field(min_length=1)


class InspectionHold(_Payload):
    port_code: str = Field(pattern=PORT_PATTERN)
    reason: str = Field(min_length=1)


class Rejected(_Payload):
    reason: str = Field(min_length=1)


PAYLOAD_MODELS: dict[EventType, type[_Payload]] = {
    EventType.DECLARATION_FILED: DeclarationFiled,
    EventType.INSPECTION_HOLD: InspectionHold,
    EventType.REJECTED: Rejected,
}


class ClearanceEvent(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")

    event_id: UUID
    shipment_id: str = Field(min_length=1, max_length=64)
    container_id: str  # content is checked separately, as CONTAINER_ID_INVALID
    event_type: EventType
    occurred_at: AwareDatetime
    source: str = Field(min_length=1)
    schema_version: int
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def _to_utc(cls, v: AwareDatetime) -> AwareDatetime:
        return v.astimezone(UTC)

    @field_validator("schema_version")
    @classmethod
    def _supported(cls, v: int) -> int:
        if v not in SUPPORTED_VERSIONS:
            raise ValueError(f"unsupported schema_version {v}")
        return v

    @model_validator(mode="after")
    def _payload_for_type(self) -> "ClearanceEvent":
        model = PAYLOAD_MODELS.get(self.event_type, _Payload)
        self.payload = model.model_validate(self.payload).model_dump()
        return self


def parse_event(raw: bytes | str) -> ClearanceEvent:
    """Raises pydantic.ValidationError (SCHEMA_INVALID) for any malformed input."""
    return ClearanceEvent.model_validate_json(raw)

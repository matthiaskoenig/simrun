"""Identities of the callers of the service and the priorities of their runs."""

import hmac
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, model_validator

from simrun.workflow import Priority


class Identity(BaseModel):
    """Who calls the service: a person or an agent acting for a person.

    Attributes:
        name: Name of the person or the agent.
        kind: `person` or `agent`.
        owner: The person responsible; a person's owner is the person.
    """

    name: str
    kind: Literal["person", "agent"]
    owner: str

    @model_validator(mode="before")
    @classmethod
    def _owner(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("kind") == "person":
            owner = data.get("owner")
            if owner is not None and owner != data.get("name"):
                raise ValueError(
                    f"the owner of the person {data.get('name')!r} is the person, "
                    f"not {owner!r}"
                )
            return {**data, "owner": data.get("name")}
        if (
            isinstance(data, dict)
            and data.get("kind") == "agent"
            and not data.get("owner")
        ):
            raise ValueError(f"the agent {data.get('name')!r} has no owner")
        return data


def load_tokens(path: Path) -> dict[str, Identity]:
    """Read the tokens of the service from a YAML mapping of token to identity.

    Args:
        path: The YAML file, e.g. `{<token>: {name: mk, kind: person}}`.

    Returns:
        The identities by token.

    Raises:
        ValueError: The file is no mapping, or a token is empty.
    """
    data = yaml.safe_load(path.read_text()) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: not a mapping of token to identity")
    tokens: dict[str, Identity] = {}
    for token, identity in data.items():
        if not isinstance(token, str) or not token:
            raise ValueError(f"{path}: a token must be a non-empty string")
        tokens[token] = Identity.model_validate(identity)
    return tokens


def authenticate(tokens: dict[str, Identity], token: str) -> Identity | None:
    """The identity of a token, compared in constant time; None if unknown."""
    found = None
    for known, identity in tokens.items():
        if hmac.compare_digest(known.encode(), token.encode()):
            found = identity
    return found


def priority_of(identity: Identity, owner: str) -> Priority:
    """The priority of the runs of an identity.

    Args:
        identity: The submitting identity.
        owner: The owner of the cluster.

    Returns:
        `OWNER` for the owner and the owner's agents, `STANDARD` for other people,
        `AGENT` for their agents.
    """
    if identity.owner == owner:
        return Priority.OWNER
    if identity.kind == "person":
        return Priority.STANDARD
    return Priority.AGENT

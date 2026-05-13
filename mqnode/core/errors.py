class MQNodeError(Exception):
    """Base error for MQNODE."""


class RpcError(MQNodeError):
    """Bitcoin RPC error."""


class ValidationError(MQNodeError):
    """Payload validation error."""


class DependencyError(MQNodeError):
    """Metric dependency is missing or not ready."""


class ReorgDetectedError(MQNodeError):
    """Local BTC state diverged from the canonical node chain."""

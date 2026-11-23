"""Real-time signal broker: WebSocket fan-out with per-topic sequence numbers and replay."""
from .auth import mint_token, new_secret, verify_token
from .broker import Broker, BrokerConfig
from .client import Backoff, Delivery, GapRecord, Subscriber, SubscriberError
from .publisher import PublishError, Publisher
from .schema import PROTOCOL_VERSION, AlertSignal, ScoreSignal

__version__ = "1.0.0"

__all__ = ["Broker", "BrokerConfig", "Subscriber", "SubscriberError", "Publisher", "PublishError", "Backoff",
           "Delivery", "GapRecord", "ScoreSignal", "AlertSignal", "PROTOCOL_VERSION", "mint_token",
           "new_secret", "verify_token", "__version__"]

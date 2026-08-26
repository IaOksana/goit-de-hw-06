import os


# Fail immediately when a required Kafka connection setting is missing.
def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it before running the Kafka scripts."
        )
    return value


# Centralize broker authentication and allow safe protocol defaults.
kafka_config = {
    "bootstrap_servers": [_required_env("KAFKA_BOOTSTRAP_SERVERS")],
    "username": _required_env("KAFKA_USERNAME"),
    "password": _required_env("KAFKA_PASSWORD"),
    "security_protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
    "sasl_mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
}

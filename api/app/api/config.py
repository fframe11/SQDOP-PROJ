import os

def get_required_env(name: str) -> str:
    """Return the value of an environment variable or raise a clear error.

    Args:
        name: The environment variable name.
    Returns:
        The variable's value.
    Raises:
        RuntimeError: If the variable is not set.
    """
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(
            f"Missing required environment variable '{name}'. Set it in the deployment environment or .env file."
        )
    return value

def get_elasticsearch_url():
    # Prefer full URL if provided via environment
    es_url = os.getenv("ELASTICSEARCH_URL")
    if es_url:
        return es_url
    # Otherwise construct from components, using defaults where appropriate
    es_user = os.getenv("ELASTICSEARCH_USER", "elastic")
    es_pass = os.getenv("ELASTICSEARCH_PASSWORD", "sdoqap_secure")
    es_host = os.getenv("ELASTICSEARCH_HOST", "localhost")
    es_port = os.getenv("ELASTICSEARCH_PORT", "9200")
    return f"http://{es_user}:{es_pass}@{es_host}:{es_port}"

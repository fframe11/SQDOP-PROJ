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

# Local
from .utils.constants import AVAILABLE_CLIENTS, PIPELINES


class CustomException(Exception):  # pragma: no cover
    """Custom exception base class."""

    key = "CUSTOM_GENERIC_EXCEPTION"

    def __init__(self, message: str):
        super(CustomException, self).__init__(message)

        self.details: dict = dict(message=message)


class InvalidPipelineError(CustomException):  # pragma: no cover
    """Raised when an invalid pipeline is provided."""

    key = "INVALID_PIPELINE"

    def __init__(self, pipeline: str, allowed_pipelines: list[str] = PIPELINES):
        message = f"Invalid pipeline '{pipeline}'. Must be one of {allowed_pipelines}."

        super(InvalidPipelineError, self).__init__(message)
        self.details = dict(pipeline=pipeline, allowed_pipelines=allowed_pipelines)


class StepNotFoundError(CustomException):  # pragma: no cover
    """Error raised when a step is not found."""

    key = "STEP_NOT_FOUND"

    def __init__(self, step: str, allowed_steps: list[str]):
        message = f"Step '{step}' not found. Must be one of {allowed_steps}."

        super(StepNotFoundError, self).__init__(message)
        self.details = dict(step=step, allowed_steps=allowed_steps)


class InvalidClientError(CustomException):  # pragma: no cover
    """Raised when an invalid client is provided."""

    key = "INVALID_CLIENT"

    def __init__(self, client: str):
        message = f"Invalid client '{client}'. Must be one of {AVAILABLE_CLIENTS}."

        super(InvalidClientError, self).__init__(message)
        self.details = dict(client=client)


class MaxRetriesExceededError(CustomException):  # pragma: no cover
    """Raised when the maximum number of retries is exceeded."""

    key = "MAX_RETRIES_EXCEEDED"

    def __init__(self, url: str, max_retries: int):
        message = f"Exceeded maximum number of retries ({max_retries}) for {url=}"

        super(MaxRetriesExceededError, self).__init__(message)
        self.details = dict(url=url, max_retries=max_retries)


class MissingAPIKeyError(CustomException):  # pragma: no cover
    """Raised when the API key is missing."""

    key = "MISSING_API_KEY"

    def __init__(self, api_name: str, api_key_website: str):
        message = (
            f"Missing {api_name} API key. "
            f"Retrieve your key from {api_key_website} and add it to secrets.env file."
        )
        super(MissingAPIKeyError, self).__init__(message)
        self.details = dict(api_name=api_name)


class DirectoryCreationError(CustomException):  # pragma: no cover
    """Raised when a directory cannot be created."""

    key = "DIRECTORY_CREATION_ERROR"

    def __init__(self, directory_path: str, error: str):
        message = f"Failed to create directory at '{directory_path}'. {error=}"
        super(DirectoryCreationError, self).__init__(message)
        self.details = dict(directory_path=directory_path, error=str(error))

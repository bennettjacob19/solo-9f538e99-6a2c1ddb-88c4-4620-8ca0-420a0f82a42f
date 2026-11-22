"""Application-level errors mapped to HTTP responses in main.py."""
from __future__ import annotations


class AppError(Exception):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


class NotFoundError(AppError):
    def __init__(self, message: str):
        super().__init__(404, message)


class ConflictError(AppError):
    def __init__(self, message: str):
        super().__init__(409, message)

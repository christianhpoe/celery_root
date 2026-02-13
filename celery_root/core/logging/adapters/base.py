# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Abstract logging controller interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging

    from celery_root.config import CeleryRootConfig, LoggingConfigFile


class BaseLogController(ABC):
    """Interface for log controller implementations."""

    @abstractmethod
    def configure(self, config: CeleryRootConfig | LoggingConfigFile) -> None:
        """Configure logging based on runtime settings."""
        ...

    @abstractmethod
    def get_logger(self, name: str) -> logging.Logger:
        """Return a logger with the configured handlers."""
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Shut down logging resources."""
        ...

"""Streaming Compute Package for Trading Risk Engine.

Contains Apache Beam (Dataflow) pipelines and PySpark (Dataproc) applications.
"""

from streaming import dataflow, dataproc

__version__ = "0.1.0"

__all__ = ["dataflow", "dataproc", "__version__"]

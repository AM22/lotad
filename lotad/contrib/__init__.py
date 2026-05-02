"""Outbound write operations against external services (TouhouDB and friends).

Read-side ingestion lives in ``lotad.ingestion``; everything in this package
authenticates as the LOTAD operator and submits draft edits or contributions
back to upstream databases.
"""

from __future__ import annotations

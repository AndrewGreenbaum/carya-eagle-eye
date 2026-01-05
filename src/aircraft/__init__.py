"""
Aircraft Tracking Module
Real-time aircraft data from OpenSky Network
"""

from .tracker import router, get_aircraft, Aircraft

__all__ = ["router", "get_aircraft", "Aircraft"]

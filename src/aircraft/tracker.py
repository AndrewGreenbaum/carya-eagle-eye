"""
Aircraft Tracker - Real-time ADS-B data from OpenSky Network
Provides REST API and WebSocket endpoints for live aircraft tracking
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Optional, List, Dict, Set
from dataclasses import dataclass, asdict

import httpx
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter(prefix="/aircraft", tags=["aircraft"])

# OpenSky Network API
OPENSKY_API_URL = "https://opensky-network.org/api/states/all"

# Continental USA bounding box
USA_BOUNDS = {
    "lamin": 24.5,    # South (Florida Keys)
    "lamax": 49.5,    # North (Canadian border)
    "lomin": -125.0,  # West (Pacific coast)
    "lomax": -66.5    # East (Atlantic coast)
}

# Cache for aircraft data (to respect OpenSky rate limits)
_aircraft_cache: Dict[str, dict] = {}
_cache_timestamp: float = 0
CACHE_TTL_SECONDS = 10  # OpenSky rate limit


@dataclass
class Aircraft:
    """Aircraft state from ADS-B data"""
    id: str
    icao24: str
    callsign: Optional[str]
    origin_country: str
    latitude: float
    longitude: float
    altitude: Optional[float]
    velocity: Optional[float]
    heading: Optional[float]
    vertical_rate: Optional[float]
    on_ground: bool
    squawk: Optional[str]
    category: Optional[str]
    last_seen: str


def parse_aircraft_state(state: list) -> Optional[Aircraft]:
    """Parse OpenSky API state vector into Aircraft object"""
    try:
        latitude = state[6]
        longitude = state[5]

        if latitude is None or longitude is None:
            return None

        icao24 = state[0]
        category = get_aircraft_type(state[17] if len(state) > 17 else None)

        return Aircraft(
            id=f"ac_{icao24.upper()}",
            icao24=icao24.upper(),
            callsign=state[1].strip() if state[1] else None,
            origin_country=state[2],
            latitude=latitude,
            longitude=longitude,
            altitude=state[7],  # barometric altitude in meters
            velocity=state[9],  # m/s
            heading=state[10],  # degrees
            vertical_rate=state[11],
            on_ground=state[8],
            squawk=state[14],
            category=category,
            last_seen=datetime.utcnow().isoformat()
        )
    except (IndexError, TypeError):
        return None


def get_aircraft_type(category: Optional[int]) -> str:
    """Convert OpenSky category to readable type"""
    if category is None:
        return "unknown"

    categories = {
        0: "unknown",
        1: "unknown",
        2: "light",        # < 15,500 lbs
        3: "small",        # 15,500 - 75,000 lbs
        4: "large",        # 75,000 - 300,000 lbs
        5: "high_vortex",  # e.g., B757
        6: "heavy",        # > 300,000 lbs
        7: "high_perf",    # High Performance
        8: "rotorcraft",   # Helicopter
        9: "glider",
        10: "lighter_than_air",
        11: "parachutist",
        12: "ultralight",
        13: "reserved",
        14: "uav",
        15: "space_vehicle",
        16: "emergency",
        17: "service",
        18: "ground_obstacle",
        19: "cluster_obstacle",
        20: "line_obstacle"
    }
    return categories.get(category, "unknown")


async def fetch_aircraft_from_opensky() -> List[Aircraft]:
    """Fetch current aircraft from OpenSky Network API"""
    global _aircraft_cache, _cache_timestamp

    # Check cache first
    now = time.time()
    if now - _cache_timestamp < CACHE_TTL_SECONDS and _aircraft_cache:
        return list(_aircraft_cache.values())

    params = {
        "lamin": USA_BOUNDS["lamin"],
        "lamax": USA_BOUNDS["lamax"],
        "lomin": USA_BOUNDS["lomin"],
        "lomax": USA_BOUNDS["lomax"]
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(OPENSKY_API_URL, params=params)

            if response.status_code == 200:
                data = response.json()
                states = data.get("states", [])

                aircraft_list = []
                new_cache = {}

                for state in states:
                    ac = parse_aircraft_state(state)
                    if ac:
                        aircraft_list.append(ac)
                        new_cache[ac.id] = ac

                _aircraft_cache = new_cache
                _cache_timestamp = now

                return aircraft_list
            elif response.status_code == 429:
                print("[OPENSKY] Rate limited - using cache")
                return list(_aircraft_cache.values())
            else:
                print(f"[OPENSKY] API error: {response.status_code}")
                return list(_aircraft_cache.values())
    except Exception as e:
        print(f"[OPENSKY] Error: {e}")
        return list(_aircraft_cache.values())


async def get_aircraft() -> List[dict]:
    """Get current aircraft as list of dicts"""
    aircraft_list = await fetch_aircraft_from_opensky()
    return [asdict(ac) for ac in aircraft_list]


# WebSocket connections for real-time updates
_ws_clients: Set[WebSocket] = set()
_tracking_task: Optional[asyncio.Task] = None


async def broadcast_aircraft_updates():
    """Background task to broadcast aircraft updates to WebSocket clients"""
    while True:
        try:
            if _ws_clients:
                aircraft_list = await get_aircraft()
                message = json.dumps({
                    "type": "aircraft_update",
                    "data": aircraft_list,
                    "count": len(aircraft_list),
                    "timestamp": datetime.utcnow().isoformat()
                })

                dead_clients = set()
                for client in _ws_clients:
                    try:
                        await client.send_text(message)
                    except Exception:
                        dead_clients.add(client)

                _ws_clients.difference_update(dead_clients)

            await asyncio.sleep(10)  # OpenSky rate limit
        except Exception as e:
            print(f"[AIRCRAFT] Broadcast error: {e}")
            await asyncio.sleep(10)


# REST Endpoints

@router.get("")
async def list_aircraft():
    """Get all currently tracked aircraft over Continental USA"""
    aircraft_list = await get_aircraft()
    return {
        "aircraft": aircraft_list,
        "count": len(aircraft_list),
        "bounds": USA_BOUNDS,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/stats")
async def aircraft_stats():
    """Get aircraft tracking statistics"""
    aircraft_list = await get_aircraft()

    airborne = sum(1 for ac in aircraft_list if not ac.get("on_ground", True))
    on_ground = len(aircraft_list) - airborne

    # Category breakdown
    categories = {}
    for ac in aircraft_list:
        cat = ac.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    return {
        "total": len(aircraft_list),
        "airborne": airborne,
        "on_ground": on_ground,
        "categories": categories,
        "bounds": USA_BOUNDS,
        "cache_age_seconds": time.time() - _cache_timestamp if _cache_timestamp else None
    }


# WebSocket Endpoint

@router.websocket("/ws")
async def aircraft_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time aircraft updates"""
    global _tracking_task

    await websocket.accept()
    _ws_clients.add(websocket)
    print(f"[AIRCRAFT WS] Client connected. Total: {len(_ws_clients)}")

    # Start background tracking task if not running
    if _tracking_task is None or _tracking_task.done():
        _tracking_task = asyncio.create_task(broadcast_aircraft_updates())

    # Send initial data
    try:
        aircraft_list = await get_aircraft()
        await websocket.send_text(json.dumps({
            "type": "aircraft_update",
            "data": aircraft_list,
            "count": len(aircraft_list),
            "timestamp": datetime.utcnow().isoformat()
        }))
    except Exception as e:
        print(f"[AIRCRAFT WS] Error sending initial data: {e}")

    try:
        while True:
            # Keep connection alive, handle pings
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        print(f"[AIRCRAFT WS] Client disconnected. Total: {len(_ws_clients)}")

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv


load_dotenv()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE = 25

VRFKIT = Path(os.getenv("VRFKIT_PATH", "./vrfkit.exe"))

D1_DATABASE_ID = os.environ["D1_DATABASE_ID"]
CLOUDFLARE_ACCOUNT_ID = os.environ["CLOUDFLARE_ACCOUNT_ID"]
CLOUDFLARE_API_TOKEN = os.environ["CLOUDFLARE_API_TOKEN"]

R2_BUCKET = os.environ["R2_BUCKET"]
R2_ENDPOINT = f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com"

R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

r2 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
)


# ---------------------------------------------------------------------------
# D1
# ---------------------------------------------------------------------------

D1_URL = (
    f"https://api.cloudflare.com/client/v4/accounts/"
    f"{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}/query"
)

D1_HEADERS = {
    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
    "Content-Type": "application/json",
}


def d1(query, params=None):
    response = requests.post(
        D1_URL,
        headers=D1_HEADERS,
        json={
            "sql": query,
            "params": params or [],
        },
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if not data["success"]:
        raise RuntimeError(data)

    return data["result"][0]


def get_pending_replays():
    result = d1(
        """
        SELECT verified_hash, storage_key
        FROM replays
        WHERE processing_status = 'pending'
        LIMIT ?
        """,
        [BATCH_SIZE],
    )

    return result.get("results", [])


# ---------------------------------------------------------------------------
# R2
# ---------------------------------------------------------------------------

def download_replay(storage_key, destination):
    r2.download_file(
        R2_BUCKET,
        storage_key,
        str(destination),
    )


# ---------------------------------------------------------------------------
# vrfkit
# ---------------------------------------------------------------------------

def process_replay(replay_path, output_dir):
    subprocess.run(
        [
            str(VRFKIT),
            "export",
            str(replay_path),
            "--out",
            str(output_dir),
        ],
        check=True,
    )

    manifest_path = output_dir / "manifest.json"

    if not manifest_path.exists():
        raise RuntimeError("vrfkit did not produce manifest.json")

    with manifest_path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def extract_metadata(manifest):
    """
    Adapt this to the actual vrfkit manifest structure.

    Return:
        replay metadata
        list of player metadata
    """

    replay = {
        "map": manifest.get("map"),
        "game_version": manifest.get("game_version"),
        "duration_ms": manifest.get("duration_ms"),
    }

    players = []

    for player in manifest.get("players", []):
        players.append({
            "player_id": player.get("player_id"),
            "team": player.get("team"),
            "agent": player.get("agent"),
            "rank": player.get("rank"),
        })

    return replay, players


# ---------------------------------------------------------------------------
# Database update
# ---------------------------------------------------------------------------

def save_batch(updates, players):
    """
    One D1 request containing all UPDATE/INSERT statements.

    D1's API accepts multiple statements in one request.
    """

    statements = []

    for update in updates:
        statements.append({
            "sql": """
                UPDATE replays
                SET
                    map = ?,
                    game_version = ?,
                    duration_ms = ?,
                    processing_status = 'completed',
                    processed_at = ?
                WHERE verified_hash = ?
            """,
            "params": [
                update["map"],
                update["game_version"],
                update["duration_ms"],
                update["processed_at"],
                update["replay_hash"],
            ],
        })

    for player in players:
        statements.append({
            "sql": """
                INSERT OR REPLACE INTO replay_players (
                    replay_hash,
                    player_id,
                    team,
                    agent,
                    rank
                )
                VALUES (?, ?, ?, ?, ?)
            """,
            "params": [
                player["replay_hash"],
                player["player_id"],
                player["team"],
                player["agent"],
                player["rank"],
            ],
        })

    if not statements:
        return

    response = requests.post(
        D1_URL,
        headers=D1_HEADERS,
        json=statements,
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    if not data["success"]:
        raise RuntimeError(data)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    while True:
        replays = get_pending_replays()

        if not replays:
            print("No pending replays.")
            return

        print(f"Processing {len(replays)} replays...")

        updates = []
        players = []

        with tempfile.TemporaryDirectory() as temp:
            temp = Path(temp)

            for i, replay in enumerate(replays, 1):
                replay_hash = replay["verified_hash"]
                storage_key = replay["storage_key"]

                print(
                    f"[{i}/{len(replays)}] "
                    f"{replay_hash}"
                )

                replay_path = temp / "replay.vrf"
                output_dir = temp / "output"

                if output_dir.exists():
                    shutil.rmtree(output_dir)

                output_dir.mkdir()

                try:
                    download_replay(
                        storage_key,
                        replay_path,
                    )

                    metadata, replay_players = process_replay(
                        replay_path,
                        output_dir,
                    )

                    updates.append({
                        "replay_hash": replay_hash,
                        "map": metadata["map"],
                        "game_version": metadata["game_version"],
                        "duration_ms": metadata["duration_ms"],
                        "processed_at": int(__import__("time").time()),
                    })

                    for player in replay_players:
                        players.append({
                            "replay_hash": replay_hash,
                            **player,
                        })

                except Exception as e:
                    print(
                        f"FAILED {replay_hash}: {e}"
                    )

                    # Leave it pending so the next run retries it.

                finally:
                    if replay_path.exists():
                        replay_path.unlink()

        print(
            f"Writing {len(updates)} replay updates "
            f"and {len(players)} players..."
        )

        save_batch(updates, players)

        print("Batch complete.")


if __name__ == "__main__":
    main()

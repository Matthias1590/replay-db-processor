import json
import os
import shutil
import subprocess
import tempfile
import duckdb
from pathlib import Path

import boto3
import requests
from dotenv import load_dotenv
from replay_header import read_replay_game_version


load_dotenv()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BATCH_SIZE = 10

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

def extract_map_name(path: str) -> str:
    parts = path.split("/")
    if len(parts) != 5 or parts[0] != "" or parts[1] != "Game" or parts[2] != "Maps" or parts[3] != parts[4]:
        raise RuntimeError(f"Invalid map path {path!r}")
    return parts[-2]

def extract_agent_name(path: str) -> str:
    parts = path.split("/")
    if len(parts) != 5 or parts[0] != "" or parts[1] != "Game" or parts[2] != "Characters" or parts[4] != f"{parts[3]}_PC.{parts[3]}_PC_C":
        raise RuntimeError(f"Invalid agent path {path!r}")
    return parts[-2]

def process_replay(replay_path, output_dir: Path):
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

    with open(manifest_path.as_posix(), "r") as f:
        manifest = json.load(f)

    if len(manifest["level_names_and_times"]) != 1:
        raise RuntimeError("Replay has more than 1 map")

    metadata = {
        "map": extract_map_name(manifest["level_names_and_times"][0]["name"]),
        "duration_ms": manifest["duration_ms"],
    }

    min_player_id = float("inf")
    max_player_id = float("-inf")

    replay_players = []
    for player in manifest["players"]:
        replay_player = {
            "player_id": player["subject"],
            "rank": 0,
        }

        rank_row = duckdb.query(
            "SELECT value_i64 FROM read_parquet(?) WHERE actor_net_guid = ? AND group_path = '/Game/GameModes/Bomb/BombPlayerState.BombPlayerState_C' AND field_name = 'CompetitiveTier'",
            params=[(output_dir / "fields.parquet").as_posix(), player["actor_net_guid"]],
        ).fetchone()

        if rank_row is not None:
            replay_player["rank"] = rank_row[0]

        player_id_row = duckdb.query(
            "SELECT value_i64 FROM read_parquet(?) WHERE actor_net_guid = ? AND group_path = '/Game/GameModes/Bomb/BombPlayerState.BombPlayerState_C' AND field_name = 'PlayerId'",
            params=[(output_dir / "fields.parquet").as_posix(), player["actor_net_guid"]],
        ).fetchone()
        if player_id_row is None:
            raise RuntimeError("No player id row found")

        replay_player["game_player_id"] = player_id_row[0]
        min_player_id = min(min_player_id, player_id_row[0])
        max_player_id = max(max_player_id, player_id_row[0])

        agent_row = duckdb.query(
            "SELECT class_path FROM read_parquet(?) WHERE event = 'open' AND actor_net_guid = ? AND class_path LIKE '/Game/Characters/%/%_PC.%_PC_C'",
            params=[(output_dir / "actors.parquet").as_posix(), player["character_net_guid"]],
        ).fetchone()
        if agent_row is None:
            raise RuntimeError("No agent row found")

        replay_player["agent"] = extract_agent_name(agent_row[0])

        replay_players.append(replay_player)

    if (max_player_id - min_player_id) + 1 != 10:
        raise RuntimeError("Number of player ids is not 10")

    for player in replay_players:
        player["team"] = (player["game_player_id"] - min_player_id) // 5
        del player["game_player_id"]

    return metadata, replay_players


# ---------------------------------------------------------------------------
# Database update
# ---------------------------------------------------------------------------

def save_batch(updates, players, failures):
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

    for failure in failures:
        statements.append({
            "sql": """
                UPDATE replays
                SET
                    game_version = COALESCE(?, game_version),
                    processing_status = 'failed'
                WHERE verified_hash = ?
            """,
            "params": [
                failure["game_version"],
                failure["replay_hash"],
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
        json={"batch": statements},
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
        failures = []

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
                game_version = None

                try:
                    download_replay(
                        storage_key,
                        replay_path,
                    )

                    game_version = read_replay_game_version(replay_path)

                    metadata, replay_players = process_replay(
                        replay_path,
                        output_dir,
                    )

                    updates.append({
                        "replay_hash": replay_hash,
                        "map": metadata["map"],
                        "game_version": game_version,
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

                    failures.append({
                        "replay_hash": replay_hash,
                        "game_version": game_version,
                    })

                finally:
                    if replay_path.exists():
                        replay_path.unlink()

        print(
            f"Writing {len(updates)} completed, "
            f"{len(failures)} failed, "
            f"{len(players)} players..."
        )

        save_batch(updates, players, failures)

        print("Batch complete.")


if __name__ == "__main__":
    main()

"""Initialize fresh DEV metadata and create its administrator only if absent."""

import os
import subprocess
from pathlib import Path


def main():
    subprocess.run(["airflow", "db", "migrate"], check=True)
    # Do not invoke the scheduled DAGs or the application migration DAG.
    from airflow.www.app import create_app

    app = create_app()
    with app.app_context():
        manager = app.appbuilder.sm
        if manager.find_user(username="devadmin") is None:
            user = manager.add_user(
                username="devadmin",
                first_name="DEV",
                last_name="Administrator",
                email="devadmin@example.invalid",
                role=manager.find_role("Admin"),
                password=os.environ["DEV_ADMIN_PASSWORD"],
            )
            if not user:
                raise RuntimeError("DEV administrator creation failed")
    # speaker_turn_videos and speaker_turn_prepare refuse to schedule without this
    # pool (docs/DEPLOYMENT_NAS.md creates it by hand on the NAS); `pools set` is idempotent.
    subprocess.run(["airflow", "pools", "set", "nas_ffmpeg", "1", "Single ffmpeg execution slot"], check=True)
    for name in ("assets", "downloads", "videos"):
        Path("/opt/airflow/data/congress_videos", name).mkdir(parents=True, exist_ok=True)
    print("DEV metadata initialized; administrator preserved or created")


if __name__ == "__main__":
    main()

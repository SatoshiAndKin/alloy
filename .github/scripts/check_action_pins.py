"""Keep the repository and upstream global pin policies in read-only mode."""

import json
import os
import subprocess
from pathlib import Path

root = Path(os.environ["RUNNER_TEMP"]) / "alloy-ci-tools"
policy = root / "pin-policy.json"
policy.write_text(
    json.dumps(
        {
            "version": 3,
            "min_age": {"value": 7},
            "rules": [
                {
                    "min_age": 0,
                    "conditions": [
                        {"expr": 'ActionName matches "^tempoxyz/gh-actions(/|$)"'}
                    ],
                }
            ],
        }
    )
)
command = [
    str(root / "pinact" / "pinact"),
    "run",
    "--fix=false",
    "--verify-comment",
    "--verify-min-age",
]
if Path(".pinact.yaml").is_file():
    command += ["-c", ".pinact.yaml"]
env = dict(os.environ, PINACT_GLOBAL_CONFIG=str(policy))
subprocess.run(command, env=env, check=True)

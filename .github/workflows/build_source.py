#! /usr/bin/env python3

from pathlib import Path
import subprocess

ROOT = Path(__file__).parent.parent.parent

subprocess.run(["maturin", "sdist"])

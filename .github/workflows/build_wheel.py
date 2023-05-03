#! /usr/bin/env python3

import os
import sys
from pathlib import Path
import subprocess

ROOT = Path(__file__).parent.parent.parent

# For macOS and Windows and linux, run Maturin against the Python interpreter that's
# been installed and configured for this CI run, i.e. the one that's running
# this script.
# Note the docker image konstin2/maturin:master does not work.

os.chdir("./python")
command = [
    "maturin",
    "build",
    "--release",
    "--compatibility=manylinux_2_35",
    "--interpreter",
    sys.executable,
]
subprocess.run(command, check=True)
os.chdir("..")
wheels = [x for x in (ROOT / "target" / "wheels").iterdir()]
if len(wheels) != 1:
    raise RuntimeError("expected one wheel, found " + repr(wheels))

print("::set-output name=wheel_path::" + str(wheels[0]))
print("::set-output name=wheel_name::" + wheels[0].name)

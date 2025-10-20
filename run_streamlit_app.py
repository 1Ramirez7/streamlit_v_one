"""
run_streamlit_app.py
--------------------
Launches streamlit script to open Discrete Event Simulation dashboard as a 
localhost.

Usage
-----
# 1. Create and activate the virtual environment
# (Windows PowerShell)
python -m venv .venv
.venv\Scripts\Activate.ps1

# (Mac/Linux)
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the Streamlit app

## Option A: If using VS Code
# Simply press ▶ "Run" at the top right of this file.

## Option B: If using the terminal
python run_streamlit_app.py

Notes
-----
- Must be run from the project root directory.
- Automatically uses the current Python environment (venv or system).
- Streamlit output will appear in the terminal.
"""

import os
import sys
import subprocess

def main():
    """Launch the Streamlit app using the current Python environment."""
    venv_python = sys.executable
    app_dir = os.path.join(os.getcwd(), "streamlit_app")
    streamlit_script = os.path.join(app_dir, "main.py")

    if not os.path.exists(streamlit_script):
        print(f"Streamlit entrypoint not found: {streamlit_script}")
        print("Make sure your app structure matches: root/app/main.py")
        sys.exit(1)

    print("Starting Streamlit interface...")
    print(f"Using Python: {venv_python}")
    print(f"Launching script: {streamlit_script}\n")

    try:
        subprocess.run(
            [venv_python, "-m", "streamlit", "run", streamlit_script],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Streamlit failed to start. Exit code: {e.returncode}")
    except KeyboardInterrupt:
        print("\nStreamlit server stopped by user.")

if __name__ == "__main__":
    main()


# Change ui to have tabs MICAP, each stage, and overview analysis
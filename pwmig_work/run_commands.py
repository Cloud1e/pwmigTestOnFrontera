import subprocess
import os
def run_commands(commands):
    for cmd in commands:
        if cmd.startswith("cd "):
            path = cmd[3:].strip()
            try:
                os.chdir(path)
                print(f"Changed directory to: {os.getcwd()}")
            except FileNotFoundError:
                print(f"Directory not found: {path}")
            except PermissionError:
                print(f"Permission denied: {path}")
            except Exception as e:
                print(f"Failed to change directory to {path}. Reason: {e}")
        else:
            try:
                result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
                print(result.stdout)
            except subprocess.CalledProcessError as e:
                print(f"Command failed: {e.cmd}")
                print(f"Error: {e.stderr}")



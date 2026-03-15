import sys
import subprocess

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)

    image_path = sys.argv[1]
    
    result = subprocess.run(
        ["venv/bin/python", "classifier/classify.py", image_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        sys.exit(1)
    
    # The script prints some extra lines, such as "Reading image..."
    lines = result.stdout.strip().split("\n")
    
    # Last line should be the label
    label = lines[-1]
    
    print(label)
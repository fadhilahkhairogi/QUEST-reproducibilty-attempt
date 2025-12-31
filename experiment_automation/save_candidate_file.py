import os
import shutil
import sys
import subprocess

# Fixed source and destination root
SRC_ROOT = r"D:\Fadhil\Pelajaran\Riset\ACE\Reproduce\QUEST-R0\exp_candidate"
DEST_ROOT = r"D:\Fadhil\Pelajaran\Riset\ACE\Reproduce\QUEST-R0\exp_candidate\results"  # destination will be under the same folder

FILES_TO_COPY = [
    "all.pkl",
    "data.csv",
    "document_all.pkl",
    "sample.pkl",
    "threshold.json",
]

DIRS_TO_COPY = [
    "candi",
    "key",
]


def safe_rmtree(path):
    try:
        shutil.rmtree(path)
        return True
    except Exception as e:
        print(f"Failed to remove '{path}': {e}")
        return False


def open_folder(path):
    """Open folder in OS file explorer. Works on Windows/macOS/Linux."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)               # Windows
        elif sys.platform == "darwin":
            subprocess.run(["open", path])  # macOS
        else:
            subprocess.run(["xdg-open", path])  # Linux (many distros)
    except Exception as e:
        print(f"Could not open folder '{path}': {e}")


def empty_folder_contents(folder_path):
    """
    Remove all files and directories inside folder_path but keep the folder itself.
    """
    if not os.path.isdir(folder_path):
        return False, f"Not found: {folder_path}"
    errors = []
    for entry in os.listdir(folder_path):
        full = os.path.join(folder_path, entry)
        try:
            if os.path.isfile(full) or os.path.islink(full):
                os.unlink(full)
            elif os.path.isdir(full):
                shutil.rmtree(full)
        except Exception as e:
            errors.append((full, str(e)))
    if errors:
        msg = "; ".join(f"{p}: {err}" for p, err in errors)
        return False, msg
    return True, "Emptied"


def main():
    print("Auto-copy script (NO CONFIRMATION).")
    print(f"Source = fixed: {SRC_ROOT}\n")

    src = os.path.abspath(SRC_ROOT)
    if not os.path.isdir(src):
        print(f"Source folder does not exist: {src}")
        sys.exit(1)

    exp_name = input("Enter experiment folder name (will be created under exp_candidate): ").strip()
    if not exp_name:
        print("No name provided. Exiting.")
        sys.exit(1)

    dest = os.path.abspath(os.path.join(DEST_ROOT, exp_name))

    # Prevent dest == src
    if os.path.normcase(os.path.normpath(dest)) == os.path.normcase(os.path.normpath(src)):
        print("Destination would be the same as source. Exiting to avoid copying into itself.")
        sys.exit(1)

    # If dest exists, remove it (auto-overwrite)
    if os.path.exists(dest):
        print(f"Destination exists. Removing: {dest}")
        if not safe_rmtree(dest):
            sys.exit(1)

    # create destination
    try:
        os.makedirs(dest, exist_ok=True)
    except Exception as e:
        print(f"Unable to create destination '{dest}': {e}")
        sys.exit(1)

    copied = []
    missing = []

    # Copy files (from fixed source)
    for fname in FILES_TO_COPY:
        s = os.path.join(src, fname)
        if os.path.isfile(s):
            try:
                shutil.copy2(s, dest)
                copied.append(fname)
            except Exception as e:
                print(f"Error copying file '{fname}': {e}")
        else:
            missing.append(fname)

    # Copy directories (from fixed source)
    for dname in DIRS_TO_COPY:
        sdir = os.path.join(src, dname)
        d_dest = os.path.join(dest, dname)
        if os.path.isdir(sdir):
            try:
                shutil.copytree(sdir, d_dest)
                copied.append(dname + "/ (directory)")
            except Exception as e:
                print(f"Error copying directory '{dname}': {e}")
        else:
            missing.append(dname + "/ (directory)")

    # Summary
    print("\n=== Summary ===")
    if copied:
        print("Copied:")
        for it in copied:
            print("  -", it)
    else:
        print("Copied: (none)")

    if missing:
        print("\nMissing:")
        for it in missing:
            print("  -", it)
    else:
        print("Missing: (none)")

    print(f"\nDone. Destination created at:\n{dest}")

    # NEW FEATURE: empty the 'key' and 'candi' folders inside SRC_ROOT
    print("\nEmptying 'key' and 'candi' folders in source...")
    for folder in ["key", "candi"]:
        folder_path = os.path.join(src, folder)
        ok, msg = empty_folder_contents(folder_path)
        if ok:
            print(f"  - {folder}: emptied")
        else:
            print(f"  - {folder}: {msg}")

    # Open the destination folder in file explorer
    print("Opening destination folder...")
    open_folder(dest)


if __name__ == "__main__":
    main()

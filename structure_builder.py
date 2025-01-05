import os

def print_directory_structure(start_path, indent_level=0, file=None, exclude_root=False):
    """Recursively prints the directory structure from the start_path and writes to a file."""
    excluded_files = {"project_structure.txt", "structure_builder.py"}
    excluded_folders = {".venv", ".idea", "__pycache__", "build"}

    try:
        entries = os.listdir(start_path)
        for entry in entries:
            entry_path = os.path.join(start_path, entry)
            if entry in excluded_folders or entry in excluded_files:
                continue
            line = '    ' * indent_level + f"|- {entry}\n"
            print(line, end='')
            if file:
                file.write(line)

            if os.path.isdir(entry_path):
                print_directory_structure(entry_path, indent_level + 1, file)
    except PermissionError:
        line = '    ' * indent_level + "|- [Permission Denied]\n"
        print(line, end='')
        if file:
            file.write(line)

if __name__ == "__main__":
    # Define the root directory of your web project.
    project_root = os.path.abspath(".")  # Change "." to your project's root directory if needed.
    root_name = "SvennProcessor"  # Set your root project name.
    output_file = "project_structure.txt"

    with open(output_file, "w") as file:
        file.write(f"Web Project Directory Structure:\n\n|- {root_name}\n")
        print(f"\nWeb Project Directory Structure:\n\n|- {root_name}\n")
        print_directory_structure(project_root, indent_level=1, file=file)  # Start indent after root

    print(f"\nDirectory structure saved to {output_file}")

from pathlib import Path
from datetime import datetime


class DirectoryTree:
    def __init__(self, root_path: str):
        self.root = Path(root_path)
        self.PIPE = "│   "
        self.TEE = "├── "
        self.ELBOW = "└── "
        self.FOLDER = "📁"
        self.FILE = "📄"

    def generate(self):
        print("\nFolder structure:")
        self._walk_tree(self.root)

    def _walk_tree(self, directory: Path, prefix: str = ""):
        entries = sorted(directory.iterdir())
        entries_count = len(entries)

        for index, entry in enumerate(entries):
            connector = self.ELBOW if index == entries_count - 1 else self.TEE
            if entry.is_dir():
                self._print_directory(entry, index, entries_count, prefix, connector)
            else:
                self._print_file(entry, prefix, connector)

    def _print_directory(
        self, entry: Path, index: int, count: int, prefix: str, connector: str
    ):
        print(f"{prefix}{connector}{self.FOLDER} {entry.name}/")
        if index != count - 1:
            prefix += self.PIPE
        else:
            prefix += "    "
        self._walk_tree(entry, prefix)

    def _print_file(self, entry: Path, prefix: str, connector: str):
        size = entry.stat().st_size
        modified = datetime.fromtimestamp(entry.stat().st_mtime)
        print(
            f"{prefix}{connector}{self.FILE} {entry.name} "
            f"({size:,} bytes)"
            # f"({size:,} bytes) - {modified:%Y-%m-%d %H:%M}"
        )


if __name__ == "__main__":
    output_path = "C:/pharma_warehouse/etl_pipeline/excel_pipeline/data/output"
    tree = DirectoryTree(output_path)
    tree.generate()

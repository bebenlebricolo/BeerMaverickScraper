from pathlib import Path


class Directories :
    SCRIPT_DIR = Path(__file__).parent.parent
    CACHE_DIR = SCRIPT_DIR.joinpath(".cache")
    EXTRACTED_DIR = CACHE_DIR.joinpath("extracted")
    PROCESSED_DIR = CACHE_DIR.joinpath("processed")

    # Secrets directory resides at workspace folder
    SECRETS_DIR = SCRIPT_DIR.joinpath("../.secrets")

    @staticmethod
    def ensure_cache_directory_exists():
        Directories.ensure_directory_exists(Directories.CACHE_DIR)

    @staticmethod
    def ensure_directory_exists(dirpath : Path):
        if not dirpath.exists() :
            dirpath.mkdir(parents=True)
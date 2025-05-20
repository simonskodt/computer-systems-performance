class Colors:
    HEADER: str = '\033[95m'
    HEADER2: str = '\033[33m'
    OKBLUE: str = '\033[94m'
    OKCYAN: str = '\033[96m'
    OKGREEN: str = '\033[92m'
    WARNING: str = '\033[93m'
    FAIL: str = '\033[91m'
    ENDC: str = '\033[0m'  # Reset color
    BOLD: str = '\033[1m'
    UNDERLINE: str = '\033[4m'

    @staticmethod
    def print_colored(text: str, color: str) -> None:
        print(f"{color}{text}{Colors.ENDC}")
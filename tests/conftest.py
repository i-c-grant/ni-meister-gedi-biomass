import sys
from pathlib import Path

# Add the 'tests' directory to the Python path
sys.path.insert(0, str(Path(__file__).parent))
import logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

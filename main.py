import sys
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.chatbot import main

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error running main: {str(e)}")
        raise
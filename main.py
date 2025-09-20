import sys
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    try:
        from src.chatbot import main as chatbot_main
        chatbot_main()
    except ImportError as e:
        logger.error(f"Import error: {str(e)}")
        print("Error: Could not import required modules. Please check your file structure.")
    except Exception as e:
        logger.error(f"Error running main: {str(e)}")
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
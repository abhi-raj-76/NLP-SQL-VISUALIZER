DATABASE_CONFIG = {
    'dialect': 'sqlite',
    'database': 'background_checks.db'
}

LLM_CONFIG = {
    'model_name': 'gpt-3.5-turbo',
    'temperature': 0,
    'max_tokens': 1000
}

CHATBOT_CONFIG = {
    'max_results': 50,
    'timeout_seconds': 30
}
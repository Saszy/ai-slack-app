import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from llama_cpp import Llama
from confluence.client import Confluence
import psycopg2
from typing import List, Dict
import re
import yaml
from datetime import datetime

# Load configuration
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Initialize Slack app
app = App(token=os.environ["SLACK_BOT_TOKEN"])

# Initialize LLama model
llm = Llama(
    model_path="./models/llama-2-7b-chat.gguf",
    n_ctx=2048,
    n_threads=4
)

# Initialize Confluence client
confluence = Confluence(
    url=config['confluence']['url'],
    username=config['confluence']['username'],
    password=config['confluence']['api_token']
)

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname=config['postgres']['database'],
        user=config['postgres']['user'],
        password=config['postgres']['password'],
        host=config['postgres']['host'],
        port=config['postgres']['port']
    )

class ContentFilter:
    def __init__(self):
        # Load content filtering rules
        self.blocked_patterns = [
            r'(?i)password[s]?',
            r'(?i)credit.?card',
            r'(?i)ssn',
            r'(?i)social.?security',
            # Add more patterns as needed
        ]
        
    def is_safe_content(self, text: str) -> bool:
        """Check if content is safe to display"""
        for pattern in self.blocked_patterns:
            if re.search(pattern, text):
                return False
        return True

class KnowledgeBase:
    def __init__(self):
        self.content_filter = ContentFilter()
        
    def search_confluence(self, query: str) -> List[Dict]:
        """Search Confluence for relevant content"""
        results = confluence.search(query, limit=5)
        filtered_results = []
        
        for result in results:
            if self.content_filter.is_safe_content(result.get('content', '')):
                filtered_results.append({
                    'title': result.get('title'),
                    'excerpt': result.get('excerpt'),
                    'url': result.get('_links', {}).get('webui')
                })
                
        return filtered_results

    def query_database(self, query: str) -> List[Dict]:
        """Search database based on query"""
        # List of allowed tables for querying
        allowed_tables = config['postgres']['allowed_tables']
        
        # Convert natural language query to SQL using LLM
        prompt = f"""Convert this question to a safe SQL query that only uses SELECT statements and the following tables: {', '.join(allowed_tables)}
        Question: {query}
        SQL:"""
        
        sql_response = llm(prompt, max_tokens=100)
        sql_query = sql_response['choices'][0]['text'].strip()
        
        # Validate SQL query
        if not sql_query.lower().startswith('select'):
            return []
            
        results = []
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql_query)
                    columns = [desc[0] for desc in cur.description]
                    for row in cur.fetchall():
                        result = dict(zip(columns, row))
                        if all(self.content_filter.is_safe_content(str(value)) 
                              for value in result.values()):
                            results.append(result)
                except Exception as e:
                    print(f"Database query error: {e}")
                    
        return results

class ResponseGenerator:
    def __init__(self):
        self.kb = KnowledgeBase()
        
    def generate_response(self, query: str) -> str:
        """Generate response using LLM based on knowledge base results"""
        # Get information from both sources
        confluence_results = self.kb.search_confluence(query)
        db_results = self.kb.query_database(query)
        
        # Prepare context for LLM
        context = f"""
        Information from Confluence:
        {confluence_results}
        
        Information from Database:
        {db_results}
        
        Based on the above information, please provide a helpful and accurate response.
        Question: {query}
        Answer:"""
        
        # Generate response using LLM
        response = llm(context, max_tokens=500)
        generated_text = response['choices'][0]['text'].strip()
        
        # Add source attribution
        sources = []
        if confluence_results:
            sources.append("Confluence")
        if db_results:
            sources.append("internal database")
            
        if sources:
            generated_text += f"\n\nSources: {' and '.join(sources)}"
            
        return generated_text

# Initialize response generator
response_generator = ResponseGenerator()

@app.event("app_mention")
def handle_mention(event, say):
    """Handle mentions of the bot in Slack"""
    try:
        # Extract query from message
        query = event['text'].split('>', 1)[1].strip()
        
        # Generate and send response
        response = response_generator.generate_response(query)
        
        say(response)
        
    except Exception as e:
        say("I apologize, but I encountered an error processing your request. Please try again or rephrase your question.")
        print(f"Error processing mention: {e}")

@app.event("message")
def handle_message(event, say):
    """Handle direct messages to the bot"""
    if event.get('channel_type') == 'im':
        try:
            response = response_generator.generate_response(event['text'])
            say(response)
        except Exception as e:
            say("I apologize, but I encountered an error processing your request. Please try again or rephrase your question.")
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()

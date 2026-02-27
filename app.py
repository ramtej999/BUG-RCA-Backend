from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from extract_comments import fetch_comments
from trans_summary import translate_and_summarise
import threading
import queue

app = Flask(__name__)

# Enable CORS for all domains to allow React frontend connection
CORS(app)

from flask import Response
import json

@app.route('/')
def home():
    return jsonify({"status": "healthy", "message": "Bug-RCA Backend is running!"})

@app.route('/api/process-video', methods=['POST'])
def process_video():
    data = request.json
    if not data or 'url' not in data:
        return jsonify({'error': 'URL is required'}), 400
        
    video_url = data['url']
    youtube_api_key = data.get('youtube_api_key', '').strip()
    groq_api_key = data.get('groq_api_key', '').strip()
    
    if not youtube_api_key or not groq_api_key:
        return jsonify({'error': 'Both YouTube and Groq API keys are required.'}), 400
    
    def generate():
        try:
            abort_event = threading.Event()
            abort_result = {}
            msg_queue = queue.Queue()
            
            def progress_callback(msg):
                msg_queue.put(msg)
            
            def run_task(task_name, func, *args, **kwargs):
                def target():
                    try:
                        abort_result[task_name] = func(*args, abort_event=abort_event, **kwargs)
                    except Exception as e:
                        abort_result[f"{task_name}_error"] = str(e)
                
                t = threading.Thread(target=target)
                t.start()
                while t.is_alive():
                    while not msg_queue.empty():
                        msg = msg_queue.get()
                        yield f"data: {json.dumps({'status': task_name, 'message': msg})}\n\n"
                    
                    # Ping with 2048 spaces to FORCE Cloudflare/Nginx to flush the TCP buffer
                    yield f": {' ' * 2048}\n\n" 
                    t.join(1.0)
                    
                while not msg_queue.empty():
                    msg = msg_queue.get()
                    yield f"data: {json.dumps({'status': task_name, 'message': msg})}\n\n"
                    
            # Step 1: Extract comments dynamically
            yield f"data: {json.dumps({'status': 'extracting', 'message': 'Fetching comments from YouTube...'})}\n\n"
            yield from run_task('extract', fetch_comments, video_url, youtube_api_key)
            if abort_event.is_set(): return # Terminated early
            
            if 'extract_error' in abort_result:
                yield f"data: {json.dumps({'error': abort_result['extract_error']})}\n\n"
                return
                
            comments = abort_result.get('extract')
            
            if not comments:
                yield f"data: {json.dumps({'error': 'No comments found for this video.'})}\n\n"
                return
            
            yield f"data: {json.dumps({'status': 'extracting', 'message': 'Extracted comments successfully.'})}\n\n"
            
            # Step 2: Translate & Summarize using Gemini Pipeline
            yield f"data: {json.dumps({'status': 'translating', 'message': f'Translating and summarizing {len(comments)} comments with Groq LLaMA...'})}\n\n"
            
            yield from run_task('trans_summary', translate_and_summarise, comments, groq_api_key, progress_callback=progress_callback)
            if abort_event.is_set(): return # Terminated early
            
            if 'trans_summary_error' in abort_result:
                yield f"data: {json.dumps({'error': abort_result['trans_summary_error']})}\n\n"
                return
            
            payload = abort_result.get('trans_summary', {})
            translated_comments = payload.get("translated_comments", [])
            summary = payload.get("summary", "Summarization failed.")
            
            # Final output
            extracted_translated_sample = translated_comments[:20] if translated_comments else []
            yield f"data: {json.dumps({'status': 'complete', 'results': {'extracted_count': len(comments), 'comments': comments[:20], 'translated_comments': extracted_translated_sample, 'summary': summary}})}\n\n"
            
        except GeneratorExit:
            # Triggered when client disconnects (clicks Stop button) mid-stream
            print("Client disconnected. Cancelling internal tasks...")
            if 'abort_event' in locals():
                abort_event.set()
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            
    import time

    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive'
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)

import os
import re
import time
from groq import Groq
from dotenv import load_dotenv

# Load fallback API key (useful for local dev)
load_dotenv()
fallback_api_key = os.environ.get("GROQ_API_KEY")

def _get_client(api_key=None):
    key = api_key or fallback_api_key
    if not key:
        raise ValueError("GROQ API key must be provided from frontend or environment variables")
    return Groq(api_key=key)

def _call_groq(prompt: str, api_key=None, json_mode=False) -> str:
    client = _get_client(api_key)
    kwargs = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}]
    }
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
        
    response = client.chat.completions.create(**kwargs)
    return response.choices[0].message.content

def translate_comments(comments: list[str], api_key=None, abort_event=None) -> list[str]:
    translated_results = []
    batch_size = 20
    total_batches = (len(comments) + batch_size - 1) // batch_size
    
    for i in range(0, len(comments), batch_size):
        if abort_event and abort_event.is_set():
            print("Translation aborted by user.")
            break
        batch = comments[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        print(f"Translating batch {batch_num}/{total_batches}...")
        
        prompt_lines = ["Translate the following Japanese YouTube comments to English. Return only the translations in the exact same numbered format, preserving emojis and meaning:"]
        for idx, comment in enumerate(batch):
            prompt_lines.append(f"{idx + 1}. {comment}")
            
        prompt = "\n".join(prompt_lines)
        
        try:
            result_text = _call_groq(prompt, api_key=api_key)
            result_lines = result_text.strip().split('\n')
            
            parsed_batch = []
            for line in result_lines:
                if not line.strip():
                    continue
                match = re.search(r'^\d+[\.\)\:]\s*(.*)', line)
                if match:
                    parsed_batch.append(match.group(1))
                else:
                    parsed_batch.append(line)
                    
            if len(parsed_batch) < len(batch):
                parsed_batch.extend(batch[len(parsed_batch):])
            
            translated_results.extend(parsed_batch[:len(batch)])
            time.sleep(5)
            
        except Exception as e:
            print(f"Error translating batch {batch_num}: {e}. Skipping batch.")
            translated_results.extend(batch)
            
    return translated_results

def summarize_comments(translated_comments: list[str], api_key=None, abort_event=None) -> str:
    chunk_size = 500
    total_chunks = (len(translated_comments) + chunk_size - 1) // chunk_size
    chunk_summaries = []
    
    for i in range(0, len(translated_comments), chunk_size):
        if abort_event and abort_event.is_set():
            print("Summarization aborted by user.")
            break
        chunk = translated_comments[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        print(f"Summarising chunk {chunk_num}/{total_chunks}...")
        
        prompt = "Summarize the following English YouTube comments into the main themes, common feedback, and any recurring bug reports:\n\n"
        prompt += "\n".join([f"- {c}" for c in chunk])
        
        try:
            chunk_summaries.append(_call_groq(prompt, api_key=api_key))
            time.sleep(5)
        except Exception as e:
            print(f"Error summarizing chunk {chunk_num}: {e}")
            
    if not chunk_summaries:
        return "Not enough data or summarization failed."
        
    if getattr(abort_event, 'is_set', lambda: False)():
        return "Not enough data or summarization aborted."

    print("Generating comprehensive final summary in structured JSON format...")
    final_prompt = "Combine and format the following chunk summaries into a comprehensive analysis.\n"
    final_prompt += "You must return a strictly valid JSON object matching this exact schema:\n"
    final_prompt += "{\n"
    final_prompt += '  "overall_summary": ["Paragraph 1 of overall sentiment...", "Paragraph 2..."],\n'
    final_prompt += '  "main_issues": [\n'
    final_prompt += "    {\n"
    final_prompt += '      "title": "Short title of issue/theme",\n'
    final_prompt += '      "frequency": "e.g., Many viewers, 5 times",\n'
    final_prompt += '      "keywords": ["keyword1", "keyword2"],\n'
    final_prompt += '      "representative_comment": "A good quote representing this"\n'
    final_prompt += "    }\n"
    final_prompt += "  ],\n"
    final_prompt += '  "root_cause_hypotheses": ["Hypothesis 1 about why this happens (if any)"]\n'
    final_prompt += "}\n\n"
    final_prompt += "Chunk Summaries to analyze:\n\n"
    final_prompt += "\n\n".join(chunk_summaries)
    
    try:
        import json
        result_text = _call_groq(final_prompt, api_key=api_key, json_mode=True)
        return json.loads(result_text)
    except Exception as e:
        print(f"Error generating final summary: {e}")
        return "\n\n".join(chunk_summaries)

def translate_and_summarise(comments: list[str], api_key=None, abort_event=None) -> dict:
    translated = translate_comments(comments, api_key=api_key, abort_event=abort_event)
    
    if abort_event and abort_event.is_set():
        return {
            "translated_comments": translated,
            "summary": "Process aborted by user."
        }
        
    summary = summarize_comments(translated, api_key=api_key, abort_event=abort_event)
    
    return {
        "translated_comments": translated,
        "summary": summary
    }
import os
import re
import base64
import urllib.request
import sys

def render_mermaid(mermaid_code, output_path):
    print(f"Rendering diagram to {output_path}...")
    # Mermaid.ink expects base64 encoded JSON or just base64 encoded string
    # Simple base64 encoding of the string works for many versions
    sample_string_bytes = mermaid_code.encode("utf-8")
    base64_bytes = base64.b64encode(sample_string_bytes)
    base64_string = base64_bytes.decode("utf-8")
    
    url = f"https://mermaid.ink/img/{base64_string}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as response:
            with open(output_path, "wb") as f:
                f.write(response.read())
        print(f"✅ Success: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error rendering {output_path}: {e}")
        return False

def process_markdown_file(file_path):
    with open(file_path, "r") as f:
        content = f.read()
    
    # Pattern: ![alt](images/filename.png) followed by mermaid block
    # We use a regex that looks for the image link and then the next mermaid block
    pattern = r'!\[.*?\]\((images/(.*?\.png))\)\s*?(?:<details>.*?)?```mermaid\n(.*?)\n```'
    matches = re.findall(pattern, content, re.DOTALL)
    
    success_count = 0
    for img_path, img_name, mermaid_code in matches:
        full_output_path = os.path.join(os.getcwd(), img_path)
        if render_mermaid(mermaid_code.strip(), full_output_path):
            success_count += 1
            
    return success_count

def main():
    # Ensure images directory exists
    if not os.path.exists("images"):
        os.makedirs("images")
        
    files_to_process = ["README.md", "architecture.md"]
    total_rendered = 0
    
    for file in files_to_process:
        if os.path.exists(file):
            print(f"Processing {file}...")
            total_rendered += process_markdown_file(file)
            
    print(f"\nDone! Rendered {total_rendered} diagrams.")

if __name__ == "__main__":
    main()

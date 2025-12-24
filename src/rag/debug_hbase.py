import happybase
import json

connection = happybase.Connection(host='127.0.0.1', port=9090, transport='framed', protocol='compact')
connection.open()
table = connection.table('ustc_web_data')

print("Scanning first 10 rows...")
count = 0
for key, data in table.scan():
    if count >= 10: break
    
    title = data.get(b'info:title', b'').decode('utf-8')
    files_json = data.get(b'files:path', b'').decode('utf-8')
    
    print(f"Title: {title}")
    print(f"Files JSON: {files_json}")
    
    if files_json:
        try:
            files = json.loads(files_json)
            print(f"Parsed Files: {files}")
        except:
            print("JSON Decode Error")
    print("-" * 20)
    count += 1

connection.close()

from python
workdir /app
copy . .
run pip install -r /app/requirements.txt
cmd ['python', 'main.py', '-pr', '6', '-kvh', '0.0.0.0', '-kvp', '11202', '--kvtype', 'memcache', '-cons', 'eventual', '-p', '8001']
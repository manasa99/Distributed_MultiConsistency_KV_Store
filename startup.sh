python main.py -pr 6 -kvh 0.0.0.0 -kvp 11201 --kvtype memcache -cons linearizability -p 8000 > /dev/null 2>&1 &
python main.py -pr 6 -kvh 0.0.0.0 -kvp 11202 --kvtype memcache -cons linearizability -p 8001 > /dev/null 2>&1 &
python main.py -pr 6 -kvh 0.0.0.0 -kvp 11203 --kvtype memcache -cons linearizability -p 8002 > /dev/null 2>&1 &
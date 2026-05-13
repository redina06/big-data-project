import os

p = r'c:\Users\Hybrid Technology\Documents\3 year second semester\big data project\Group-five-big-data-main\consumer-complaints-etl\data\raw\Consumer_Complaints.csv'
print('exists', os.path.exists(p))
with open(p, 'r', encoding='utf-8', errors='ignore') as f:
    print('line1', f.readline().strip())

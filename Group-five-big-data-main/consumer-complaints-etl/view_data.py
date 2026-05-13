import duckdb

conn = duckdb.connect('output/consumer_analytics.duckdb')

print('=== ALL 51 STATES ===')
df = conn.execute('SELECT * FROM consumer_insights ORDER BY complaints_count DESC').df()
print(df.to_string(index=False))

conn.close()
print('Done!')
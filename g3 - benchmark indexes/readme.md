# Run Program
mvn clean compile exec:java -Dexec.mainClass=abd.Benchmark -Dexec.args="-d jdbc:postgresql://localhost/abd -U abduser -P segredo -p -x"

# Get Table Sizes
SELECT pg_size_pretty( pg_total_relation_size('invoice')) as inv,
pg_size_pretty( pg_total_relation_size('client')) as cli,
pg_size_pretty( pg_total_relation_size('product')) as prod;

# Queries

Products bought by client: explain select v.clientId as client, v.productId as product, count(*) as bought from invoice v group by v.clientId,v.productId order by bought desc limit 10;

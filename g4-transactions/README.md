# Skeleton Benchmark

This is a minimal skeleton benchmark for use in labs in
a database systems course at the U. Minho.

## Workload

Edit Workload.java and add your workload.

# Get Table Sizes

SELECT pg_size_pretty( pg_total_relation_size('invoice')) as inv,
pg_size_pretty( pg_total_relation_size('invoiceline')) as invl,
pg_size_pretty( pg_total_relation_size('client')) as cli,
pg_size_pretty( pg_total_relation_size('product')) as prod;

## Usage with an IDE

Run Benchmark.main() with arguments such as:

    -d jdbc:posgtresql://localhost/mydb -U myuser -P mypass -p -x
    
See Options.java for all options and defaults.

## Usage from the command line

Build with Maven:
    
    $ mvn package
    
Run self-contained jar file with arguments such as:

    java -jar target/benchmark-1.0-SNAPSHOT.jar -d jdbc:posgtresql://localhost/mydb -U myuser -P mypass -p -x
 
Use `--help` to list all options and defaults.

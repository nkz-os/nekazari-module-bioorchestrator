FROM neo4j:5.26-community

ARG N10S_VERSION=5.1.0

# Download neosemantics (n10s) JAR into the plugins directory.
# APOC is auto-downloaded by Neo4j 5.x when listed in NEO4J_PLUGINS env var.
RUN wget -q \
    -O /var/lib/neo4j/plugins/n10s-${N10S_VERSION}.jar \
    "https://github.com/neo4j-labs/neosemantics/releases/download/${N10S_VERSION}/n10s-${N10S_VERSION}.jar"

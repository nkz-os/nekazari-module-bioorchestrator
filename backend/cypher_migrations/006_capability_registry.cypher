// 006_capability_registry.cypher
// Constraints + indexes for the Capability Registry subgraph.
// Idempotent (IF NOT EXISTS / MERGE).

CREATE CONSTRAINT module_id_unique IF NOT EXISTS
FOR (m:Module) REQUIRE m.id IS UNIQUE;

CREATE CONSTRAINT capability_key_unique IF NOT EXISTS
FOR (c:Capability) REQUIRE (c.entityType, c.attributeName) IS UNIQUE;

CREATE INDEX capability_entity_type_ix IF NOT EXISTS
FOR (c:Capability) ON (c.entityType);

CREATE INDEX capability_entitlement_ix IF NOT EXISTS
FOR (c:Capability) ON (c.entitlement);

CREATE CONSTRAINT entitlement_name_unique IF NOT EXISTS
FOR (e:Entitlement) REQUIRE e.name IS UNIQUE;

MERGE (:Entitlement {name: 'open', description: 'No restrictions'});

MERGE (:Entitlement {name: 'tier-pro', description: 'Requires Pro tier'});

MERGE (:Entitlement {name: 'esdb-noncommercial', description: 'ESDB / HYPRES non-commercial license — see OPEN-001'});

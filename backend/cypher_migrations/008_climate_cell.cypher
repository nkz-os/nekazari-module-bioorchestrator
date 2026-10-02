CREATE CONSTRAINT climate_cell_key IF NOT EXISTS FOR (c:ClimateCell) REQUIRE c.key IS UNIQUE;

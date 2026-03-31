EXTENSION    = pg_bitemporal
MODULE_big   = pg_bitemporal
OBJS         = src/pg_bitemporal.o src/allen_operators.o src/bitemporal_utils.o

DATA         = sql/pg_bitemporal--0.1.sql
PGFILEDESC   = "pg_bitemporal - Bitemporal table support with Allen interval algebra"

REGRESS      = allen_operators
REGRESS_OPTS = --inputdir=test

PG_CONFIG   ?= pg_config
PGXS        := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

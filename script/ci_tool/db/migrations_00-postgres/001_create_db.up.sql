-- NOTE(prozlach): our migration tool expect database where the migrations
-- execute to already exists, hence we need two sets of migrations to fix
-- chicken and egg problem. Plain `\c` in the sql migration file does not wor
-- in the sql migration file does not work.
CREATE DATABASE ci_analitics;

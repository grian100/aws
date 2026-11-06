-- Generato da cripto.ddl(): le colonne devono restare nell'ordine dei file Parquet.

CREATE SCHEMA IF NOT EXISTS bitcoin;
CREATE SCHEMA IF NOT EXISTS monero;

CREATE TABLE IF NOT EXISTS bitcoin.prezzi_giornalieri (
    data DATE,
    prezzo DOUBLE PRECISION,
    prezzo_stimato BOOLEAN,
    media_mobile_10g DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS bitcoin.settimanale (
    settimana DATE,
    prezzo_medio DOUBLE PRECISION,
    prezzo_chiusura DOUBLE PRECISION,
    variazione_pct DOUBLE PRECISION,
    interesse BIGINT,
    giorni_stimati BIGINT
);

CREATE TABLE IF NOT EXISTS monero.prezzi_giornalieri (
    data DATE,
    prezzo DOUBLE PRECISION,
    prezzo_stimato BOOLEAN,
    media_mobile_10g DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS monero.settimanale (
    settimana DATE,
    prezzo_medio DOUBLE PRECISION,
    prezzo_chiusura DOUBLE PRECISION,
    variazione_pct DOUBLE PRECISION,
    interesse BIGINT,
    giorni_stimati BIGINT
);

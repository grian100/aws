-- Query di esempio sulle tabelle caricate dalla pipeline (vedi sql/tabelle.sql).

-- 1. Bitcoin e Monero a confronto, settimana per settimana
SELECT b.settimana,
       b.prezzo_medio  AS btc_prezzo_medio,
       b.interesse     AS btc_interesse,
       m.prezzo_medio  AS xmr_prezzo_medio,
       m.interesse     AS xmr_interesse
FROM bitcoin.settimanale b
JOIN monero.settimanale m ON m.settimana = b.settimana
ORDER BY b.settimana;

-- 2. Le 10 settimane con il maggiore interesse per Bitcoin e la variazione del prezzo
SELECT settimana, interesse, ROUND(prezzo_medio, 0) AS prezzo_medio, ROUND(variazione_pct, 1) AS variazione_pct
FROM bitcoin.settimanale
ORDER BY interesse DESC
LIMIT 10;

-- 3. Interesse medio per classe di variazione settimanale del prezzo
SELECT CASE
           WHEN ABS(variazione_pct) < 5  THEN '1. meno del 5%'
           WHEN ABS(variazione_pct) < 10 THEN '2. dal 5 al 10%'
           ELSE '3. oltre il 10%'
       END AS variazione_settimanale,
       COUNT(*) AS settimane,
       ROUND(AVG(interesse), 1) AS interesse_medio
FROM bitcoin.settimanale
WHERE variazione_pct IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- 4. Prezzo medio mensile e giorni con prezzo stimato (valori -1 nei dati grezzi)
SELECT DATE_TRUNC('month', data) AS mese,
       ROUND(AVG(prezzo), 2) AS prezzo_medio,
       SUM(CASE WHEN prezzo_stimato THEN 1 ELSE 0 END) AS giorni_stimati
FROM monero.prezzi_giornalieri
GROUP BY 1
ORDER BY 1;

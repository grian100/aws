import io
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import cripto

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
BTC, XMR = cripto.CRIPTO["btc"], cripto.CRIPTO["xmr"]


class FakeS3:
    """Client S3 in memoria con le sole operazioni usate dalla pipeline."""

    def __init__(self, oggetti=None):
        self.oggetti = dict(oggetti or {})

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.oggetti[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.oggetti[(Bucket, Key)] = Body

    def upload_file(self, Filename, Bucket, Key):
        if "/" in Bucket:
            raise ValueError(f"Invalid bucket name {Bucket!r}")
        self.oggetti[(Bucket, Key)] = Path(Filename).read_bytes()


@pytest.fixture(params=["btc", "xmr"])
def moneta(request):
    return cripto.CRIPTO[request.param]


@pytest.fixture
def prezzi(moneta):
    return cripto.leggi_prezzi(DATA / moneta.file_prezzi)


def test_leggi_prezzi(moneta, prezzi):
    qualita = cripto.report_qualita(prezzi)
    assert qualita["date_duplicate"] == 0 and qualita["giorni_mancanti"] == 0
    assert qualita["prezzi_non_validi"] == {"btc": 18, "xmr": 22}[moneta.codice]
    assert prezzi["prezzo"].notna().all()
    if moneta.codice == "btc":  # "65,619.5": virgola come separatore delle migliaia
        assert prezzi.loc[prezzi["data"] == "2024-03-12", "prezzo"].item() == 65619.5


def test_date_mese_giorno_anno(prezzi):
    # "03/12/2024" è il 12 marzo, non il 3 dicembre
    assert prezzi["data"].max() == pd.Timestamp("2024-03-12")


def test_pulisci_prezzi_sostituisce_i_non_validi_tra_i_giorni_vicini(prezzi):
    pulito = cripto.pulisci_prezzi(prezzi)
    assert pulito["data"].is_monotonic_increasing
    assert (pulito["prezzo"] > 0).all()
    assert pulito["prezzo_stimato"].sum() == (prezzi["prezzo"] == -1).sum()
    validi = pulito["prezzo"].where(~pulito["prezzo_stimato"])
    precedente, successivo = validi.ffill(), validi.bfill()
    stimati = pulito[pulito["prezzo_stimato"]]
    basso = np.minimum(precedente, successivo)[stimati.index]
    alto = np.maximum(precedente, successivo)[stimati.index]
    assert ((stimati["prezzo"] >= basso - 1e-9) & (stimati["prezzo"] <= alto + 1e-9)).all()


def test_regressione_bug_dei_valori_minus_uno():
    # Il codice originale sostituiva il prezzo del 10/02/2024 con 3.154 EUR (media di 5 prezzi
    # del 2019): il valore stimato deve stare tra il 9 e l'11 febbraio
    pulito = cripto.pulisci_prezzi(cripto.leggi_prezzi(DATA / BTC.file_prezzi)).set_index("data")["prezzo"]
    assert 43000 < pulito["2024-02-10"] < 45000
    assert pulito["2024-02-09"] <= pulito["2024-02-10"] <= pulito["2024-02-11"]


def test_interpolazione_ai_bordi_e_valori_consecutivi():
    df = pd.DataFrame({"data": pd.date_range("2024-01-01", periods=6)[::-1],
                       "prezzo": [-1, 60.0, -1, -1, 30.0, -1]})
    pulito = cripto.pulisci_prezzi(df)
    assert pulito["prezzo"].tolist() == [30.0, 30.0, 40.0, 50.0, 60.0, 60.0]
    assert pulito["prezzo_stimato"].tolist() == [True, False, True, True, False, True]


def test_media_mobile_usa_solo_il_passato():
    df = pd.DataFrame({"data": pd.date_range("2024-01-01", periods=12)[::-1], "prezzo": np.arange(12.0)[::-1]})
    risultato = cripto.aggiungi_media_mobile(df, giorni=3)
    assert risultato["media_mobile_10g"].isna().sum() == 2
    # giorno 5 (prezzo 5): media dei giorni 3, 4, 5
    assert risultato.loc[5, "media_mobile_10g"] == 4.0


def test_inizio_settimana_domenica():
    date = pd.Series(pd.to_datetime(["2024-03-03", "2024-03-04", "2024-03-09", "2024-03-10"]))
    assert cripto.inizio_settimana(date).dt.strftime("%Y-%m-%d").tolist() == [
        "2024-03-03", "2024-03-03", "2024-03-03", "2024-03-10"]


def test_unisci_settimanale(moneta):
    prezzi = cripto.pulisci_prezzi(cripto.leggi_prezzi(DATA / moneta.file_prezzi))
    trend = cripto.leggi_trend(DATA / moneta.file_trend, moneta.colonna_trend)
    settimanale = cripto.unisci_settimanale(prezzi, trend)
    assert len(settimanale) == len(trend) == 260
    assert list(settimanale.columns) == [c for c, _ in cripto.TABELLE["settimanale"]]
    # la settimana del 3/3/2024 è la media dei prezzi dal 3 al 9 marzo
    riga = settimanale.set_index("settimana").loc["2024-03-03"]
    giorni = prezzi.set_index("data").loc["2024-03-03":"2024-03-09", "prezzo"]
    assert len(giorni) == 7
    assert riga["prezzo_medio"] == pytest.approx(giorni.mean())
    assert riga["prezzo_chiusura"] == giorni.iloc[-1]
    assert settimanale["giorni_stimati"].sum() <= prezzi["prezzo_stimato"].sum()


def test_settimane_incomplete_escluse():
    prezzi = pd.DataFrame({"data": pd.date_range("2024-03-03", "2024-03-12"), "prezzo": 1.0, "prezzo_stimato": False})
    trend = pd.DataFrame({"settimana": pd.to_datetime(["2024-03-03", "2024-03-10"]), "interesse": [10, 20]})
    assert cripto.unisci_settimanale(prezzi, trend)["settimana"].tolist() == [pd.Timestamp("2024-03-03")]


def test_trend_minore_di_uno(tmp_path):
    percorso = tmp_path / "trend.csv"
    percorso.write_text('"Settimana","interesse"\n2024-03-10,<1\n2024-03-03,5\n', encoding="utf-8")
    trend = cripto.leggi_trend(percorso, "interesse")
    assert trend["interesse"].tolist() == [5, 0]


def test_pipeline_s3_end_to_end(moneta):
    s3 = FakeS3({("raw", f"dataset/{nome}"): (DATA / nome).read_bytes()
                 for nome in (moneta.file_prezzi, moneta.file_trend)})
    qualita = cripto.prepara_dati(s3, moneta, "raw", "argento")
    assert qualita["righe"] > 1800
    righe = cripto.unisci_trend(s3, moneta, "argento")
    assert righe["settimanale"] == 260
    for tabella, colonne in cripto.TABELLE.items():
        import pyarrow.parquet as pq
        schema = pq.read_schema(io.BytesIO(s3.oggetti[("argento", f"{moneta.nome}/{tabella}.parquet")]))
        assert schema.names == [c for c, _ in colonne]
        tipi = {c: str(schema.field(c).type) for c in schema.names}
        attesi = {"DATE": "date32[day]", "DOUBLE PRECISION": "double", "BOOLEAN": "bool", "BIGINT": "int64"}
        assert tipi == {c: attesi[t] for c, t in colonne}


def test_ddl_aggiornata():
    assert (ROOT / "sql" / "tabelle.sql").read_text(encoding="utf-8") == cripto.ddl()


def test_sql_caricamento():
    sql = cripto.sql_caricamento(XMR, "progettoaws-argento", "arn:aws:iam::123456789012:role/redshift-s3")
    assert sql[0] == "DELETE FROM monero.prezzi_giornalieri;"
    assert "COPY monero.prezzi_giornalieri FROM 's3://progettoaws-argento/monero/prezzi_giornalieri.parquet'" in sql[1]
    assert "IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-s3' FORMAT AS PARQUET" in sql[1]
    assert all("bitcoin" not in s for s in sql)
    assert "IAM_ROLE default" in cripto.sql_caricamento(BTC, "progettoaws-argento")[1]
    with pytest.raises(ValueError):
        cripto.sql_caricamento(BTC, "bucket'; DROP TABLE x; --")
    with pytest.raises(ValueError):
        cripto.sql_caricamento(BTC, "progettoaws-argento", "ruolo' x")


class FakeRedshiftData:
    def __init__(self, stati):
        self.stati, self.richieste, self.annullati = list(stati), [], []

    def batch_execute_statement(self, **kwargs):
        self.richieste.append(kwargs)
        return {"Id": "abc"}

    def describe_statement(self, Id):
        return self.stati.pop(0) if len(self.stati) > 1 else self.stati[0]

    def cancel_statement(self, Id):
        self.annullati.append(Id)


def test_carica_redshift():
    client = FakeRedshiftData([{"Status": "STARTED"}, {"Status": "FINISHED"}])
    cripto.carica_redshift(client, BTC, "progettoaws-argento", "wg", "dev", secret_arn="arn:segreto", attesa=0)
    richiesta = client.richieste[0]
    assert richiesta["WorkgroupName"] == "wg" and richiesta["SecretArn"] == "arn:segreto"
    assert len(richiesta["Sqls"]) == 4

    senza_segreto = FakeRedshiftData([{"Status": "FINISHED"}])
    cripto.carica_redshift(senza_segreto, BTC, "progettoaws-argento", "wg", "dev", attesa=0)
    assert "SecretArn" not in senza_segreto.richieste[0]

    with pytest.raises(RuntimeError, match="FAILED"):
        cripto.carica_redshift(FakeRedshiftData([{"Status": "FAILED", "Error": "tabella inesistente"}]),
                               BTC, "progettoaws-argento", "wg", "dev", attesa=0)
    lento = FakeRedshiftData([{"Status": "STARTED"}])
    with pytest.raises(TimeoutError):
        cripto.carica_redshift(lento, BTC, "progettoaws-argento", "wg", "dev", attesa=0, timeout=0)
    assert lento.annullati == ["abc"]


def test_carica_su_s3(capsys):
    import carica_su_s3

    s3 = FakeS3()
    assert carica_su_s3.main(["--bucket", "progettoaws-raw", "--codice"], s3=s3) == 0
    chiavi = {k for _, k in s3.oggetti}
    assert "dataset/XMR_EUR Kraken Historical Data.csv" in chiavi
    assert {"codice/cripto.py", "codice/prepara_dati.py", "codice/unisci_trend.py", "codice/carica_redshift.py"} <= chiavi
    assert all("\\" not in k for k in chiavi)
    with pytest.raises(SystemExit):
        carica_su_s3.main(["--bucket", "progettoaws-raw/dataset"], s3=FakeS3())


def test_definizione_step_functions():
    asl = json.loads((ROOT / "orchestration" / "pipeline.asl.json").read_text(encoding="utf-8"))
    rami = asl["States"]["Pipeline parallele"]["Branches"]
    assert len(rami) == 2
    for ramo in rami:
        task = [s for s in ramo["States"].values() if s["Type"] == "Task"]
        assert [t["Parameters"]["JobName"] for t in task] == [
            "cripto-prepara-dati", "cripto-unisci-trend", "cripto-carica-redshift"]
        assert all(t["Resource"].endswith("startJobRun.sync") for t in task)
        assert all(t["Catch"][0]["Next"] in ramo["States"] for t in task)

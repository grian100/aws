import pytest
from fastapi.testclient import TestClient

import cripto
from api import create_app, tabelle_da_csv
from test_cripto import FakeS3


@pytest.fixture(scope="module")
def client():
    with TestClient(create_app(sorgente="", api_key="")) as test_client:
        yield test_client


def test_health(client):
    assert client.get("/health").json() == {"stato": "ok"}


def test_monete(client):
    monete = {m["codice"]: m for m in client.get("/monete").json()}
    assert monete["btc"] == {"codice": "btc", "nome": "bitcoin", "dal": "2019-01-12", "al": "2024-03-12",
                             "giorni": 1887, "giorni_stimati": 18, "settimane": 260}
    assert monete["xmr"]["giorni_stimati"] == 22


def test_prezzi(client):
    righe = client.get("/monete/btc/prezzi", params={"dal": "2024-02-09", "al": "2024-02-11"}).json()
    assert [r["data"] for r in righe] == ["2024-02-09", "2024-02-10", "2024-02-11"]
    assert righe[1]["prezzo_stimato"] is True and 43000 < righe[1]["prezzo"] < 45000
    primo = client.get("/monete/bitcoin/prezzi", params={"al": "2019-01-12"}).json()
    assert primo == [{"data": "2019-01-12", "prezzo": 3168.1, "prezzo_stimato": False, "media_mobile_10g": None}]
    stimati = client.get("/monete/xmr/prezzi", params={"solo_stimati": True}).json()
    assert len(stimati) == 22 and all(r["prezzo_stimato"] for r in stimati)


def test_settimanale(client):
    righe = client.get("/monete/monero/settimanale", params={"dal": "2024-02-25"}).json()
    assert [r["settimana"] for r in righe] == ["2024-02-25", "2024-03-03"]
    assert set(righe[0]) == {c for c, _ in cripto.TABELLE["settimanale"]}
    assert isinstance(righe[0]["interesse"], int)
    assert len(client.get("/monete/btc/settimanale").json()) == 260


def test_statistiche(client):
    stat = client.get("/monete/btc/statistiche").json()
    assert stat["settimane"] == 260
    assert stat["correlazione_prezzo_interesse"] == pytest.approx(0.61, abs=0.01)
    classi = {c["variazione_settimanale"]: c for c in stat["interesse_per_variazione"]}
    assert classi["oltre il 10%"]["settimane"] == 62
    assert classi["oltre il 10%"]["interesse_medio"] == pytest.approx(34.6, abs=0.05)
    # periodo senza dati: correlazioni null, nessun errore
    vuoto = client.get("/monete/btc/statistiche", params={"dal": "2030-01-01"}).json()
    assert vuoto["settimane"] == 0 and vuoto["correlazione_prezzo_interesse"] is None and vuoto["dal"] is None


def test_confronto(client):
    risposta = client.get("/confronto", params={"dal": "2021-01-01", "al": "2021-12-31"}).json()
    assert risposta["settimane"] == len(risposta["dati"]) == 52
    assert set(risposta["dati"][0]) == {"settimana", "btc_prezzo_medio", "btc_variazione_pct", "btc_interesse",
                                        "xmr_prezzo_medio", "xmr_variazione_pct", "xmr_interesse"}
    tutto = client.get("/confronto").json()
    assert tutto["correlazioni"]["variazioni_settimanali"] == pytest.approx(0.67, abs=0.01)


@pytest.mark.parametrize("percorso, params, codice", [
    ("/monete/eth/prezzi", {}, 404),
    ("/monete/btc/prezzi", {"dal": "2024-03-01", "al": "2024-01-01"}, 422),
    ("/monete/btc/prezzi", {"dal": "01/02/2024"}, 422),
    ("/confronto", {"dal": "2024-03-01", "al": "2024-01-01"}, 422),
])
def test_errori(client, percorso, params, codice):
    assert client.get(percorso, params=params).status_code == codice


def test_api_key():
    with TestClient(create_app(sorgente="", api_key="segreta")) as protetto:
        assert protetto.get("/health").status_code == 200
        assert protetto.get("/monete").status_code == 401
        assert protetto.get("/monete", headers={"X-API-Key": "errata"}).status_code == 401
        assert protetto.get("/monete", headers={"X-API-Key": "segreta"}).status_code == 200


def _parquet_della_pipeline():
    return {(m, t): cripto.a_parquet(df) for m, tabelle in tabelle_da_csv().items() for t, df in tabelle.items()}


def test_sorgente_cartella_parquet(tmp_path):
    for (codice, tabella), contenuto in _parquet_della_pipeline().items():
        percorso = tmp_path / cripto.chiave_argento(cripto.CRIPTO[codice], tabella)
        percorso.parent.mkdir(parents=True, exist_ok=True)
        percorso.write_bytes(contenuto)
    with TestClient(create_app(sorgente=str(tmp_path), api_key="")) as test_client:
        assert len(test_client.get("/monete/xmr/settimanale").json()) == 260


def test_sorgente_s3():
    s3 = FakeS3({("progettoaws-argento", cripto.chiave_argento(cripto.CRIPTO[c], t)): contenuto
                 for (c, t), contenuto in _parquet_della_pipeline().items()})
    with TestClient(create_app(sorgente="s3://progettoaws-argento", api_key="", s3=s3)) as test_client:
        monete = test_client.get("/monete").json()
        assert [m["giorni"] for m in monete] == [1887, 1898]

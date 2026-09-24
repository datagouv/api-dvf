import pytest
from aiohttp import web

import api_aio


PARIS_ARRONDISSEMENTS = (
    "75101,75102,75103,75104,75105,75106,75107,75108,75109,75110,"
    "75111,75112,75113,75114,75115,75116,75117,75118,75119,75120"
)
LYON_ARRONDISSEMENTS = "69381,69382,69383,69384,69385,69386,69387,69388,69389"
MARSEILLE_ARRONDISSEMENTS = (
    "13201,13202,13203,13204,13205,13206,13207,13208,13209,13210,"
    "13211,13212,13213,13214,13215,13216"
)
ORDER = "order=date_mutation.desc,id_mutation.asc"


class FakePgrest:
    """Stands in for PostgREST: answers empty and records the requests it received."""

    def __init__(self):
        self.received = []

    async def _handle(self, request):
        self.received.append(f"{request.path}?{request.query_string}")
        if request.method == "HEAD":
            # get_resource_data_streamed reads the row count from Content-Range
            return web.Response(headers={"Content-Range": "0-0/0"})
        return web.json_response([])

    def app(self):
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", self._handle)
        return app


@pytest.fixture
async def pgrest(aiohttp_server, monkeypatch):
    fake = FakePgrest()
    server = await aiohttp_server(fake.app())
    monkeypatch.setattr(api_aio, "PGREST_ENDPOINT", f"http://127.0.0.1:{server.port}")
    return fake


@pytest.fixture
async def client(pgrest, aiohttp_client):
    return await aiohttp_client(await api_aio.app_factory())


def test_plm_codes_match_the_insee_official_geographic_code():
    assert ",".join(api_aio.PLM_ARRONDISSEMENTS["75056"]) == PARIS_ARRONDISSEMENTS
    assert ",".join(api_aio.PLM_ARRONDISSEMENTS["69123"]) == LYON_ARRONDISSEMENTS
    assert ",".join(api_aio.PLM_ARRONDISSEMENTS["13055"]) == MARSEILLE_ARRONDISSEMENTS


async def test_sections_of_a_plm_city_query_its_arrondissements(client, pgrest):
    await client.get("/commune/69123/sections")

    assert pgrest.received == [
        "/stats_whole_period?echelle_geo=eq.section"
        f"&code_parent=in.({LYON_ARRONDISSEMENTS})"
    ]


async def test_sections_of_an_arrondissement_are_left_alone(client, pgrest):
    await client.get("/commune/69381/sections")

    assert pgrest.received == [
        "/stats_whole_period?echelle_geo=eq.section&code_parent=eq.69381"
    ]


async def test_sections_of_an_ordinary_commune_are_left_alone(client, pgrest):
    await client.get("/commune/44109/sections")

    assert pgrest.received == [
        "/stats_whole_period?echelle_geo=eq.section&code_parent=eq.44109"
    ]


async def test_mutations_table_of_a_plm_city_queries_its_arrondissements(
    client, pgrest
):
    await client.get("/dvf?com=75056&page=2")

    assert pgrest.received == [
        f"/dvf?code_commune=in.({PARIS_ARRONDISSEMENTS})&{ORDER}&limit=20&offset=20"
    ]


async def test_mutations_table_of_an_ordinary_commune_is_left_alone(client, pgrest):
    await client.get("/dvf?com=44109")

    assert pgrest.received == [f"/dvf?code_commune=eq.44109&{ORDER}&limit=20&offset=0"]


async def test_every_mutations_scope_is_ordered(client, pgrest):
    await client.get("/dvf?dep=44")
    await client.get("/dvf?section=44109000DX")
    await client.get("/dvf?parcelle=44109000DX0416")

    assert pgrest.received == [
        f"/dvf?code_departement=eq.44&{ORDER}&limit=20&offset=0",
        f"/dvf?id_parcelle=like.44109000DX*&{ORDER}&limit=20&offset=0",
        f"/dvf?id_parcelle=eq.44109000DX0416&{ORDER}&limit=20&offset=0",
    ]


async def test_the_narrowest_scope_wins_when_several_are_given(client, pgrest):
    await client.get("/dvf?dep=44&com=44109&parcelle=44109000DX0416")

    assert pgrest.received == [
        f"/dvf?id_parcelle=eq.44109000DX0416&{ORDER}&limit=20&offset=0"
    ]


async def test_csv_export_of_a_plm_city_queries_its_arrondissements(client, pgrest):
    # The export streams, so the headers land before any query is issued: reading the
    # body to the end is what waits for the requests we are asserting on.
    response = await client.get("/dvf/csv/?com=13055")
    await response.read()

    assert pgrest.received == [
        f"/dvf?code_commune=in.({MARSEILLE_ARRONDISSEMENTS})&{ORDER}&limit=1&"
    ]


async def test_csv_export_of_an_ordinary_commune_is_left_alone(client, pgrest):
    response = await client.get("/dvf/csv/?com=44109")
    await response.read()

    assert pgrest.received == [f"/dvf?code_commune=eq.44109&{ORDER}&limit=1&"]


async def test_communes_of_a_departement_are_matched_on_their_own_code(client, pgrest):
    await client.get("/departement/69/communes")

    assert pgrest.received == [
        "/stats_whole_period?echelle_geo=eq.commune&code_geo=like.69*"
    ]


async def test_communes_of_an_epci_are_matched_on_their_parent(client, pgrest):
    await client.get("/epci/200046977/communes")

    assert pgrest.received == [
        "/stats_whole_period?echelle_geo=eq.commune&code_parent=eq.200046977"
    ]

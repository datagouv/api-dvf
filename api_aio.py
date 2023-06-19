from aiohttp import web, ClientSession
from markupsafe import escape
import json
import aiohttp_cors
from ast import literal_eval
from datetime import date
import os
import requests

PGREST_ENDPOINT = f"http://{os.getenv('PGREST_ENDPOINT')}"


start_year = date.today().year - 1
start_month = '01' if date.today().month <= 6 else '06'
# start_year = 2022
# start_month = '01'
start_date = str(start_year) + "-" + start_month

threshold = 3


def get_moy_5ans(echelle_geo, code=None, case_dep_commune=False):
    payload = {
        'echelle': echelle_geo,
        'code': code,
        'is_dep_com': 'yes' if case_dep_commune else 'no'
    }

    r = requests.post(f"{PGREST_ENDPOINT}/rpc/get_moy_5ans", json=payload)
    data = r.json()[0]["get_moy_5ans"]

    return web.json_response(text=json.dumps({"data": data}, default=str))


def process_geo(echelle_geo, code):
    r = requests.get(
        f"{PGREST_ENDPOINT}/stats_dvf?echelle_geo=eq.{echelle_geo}"
        f"&code_geo=eq.{code}&order=annee_mois"
    )
    data = r.json()
    return web.json_response(text=json.dumps({"data": data}, default=str))


routes = web.RouteTableDef()


@routes.get("/")
async def get_health(request):
    return web.HTTPOk()


@routes.get('/nation/mois')
def get_nation(request):
    r = requests.get(
        f"{PGREST_ENDPOINT}/stats_dvf?echelle_geo=eq.nation"
        f"&order=annee_mois"
    )
    data = r.json()
    return web.json_response(text=json.dumps({"data": data}, default=str))


@routes.get('/nation')
def get_all_nation(request):
    return get_moy_5ans("nation")


@routes.get('/departement')
def get_all_departement(request):
    return get_moy_5ans("departement")


@routes.get('/departement/{code}')
def get_departement(request):
    code = request.match_info["code"]
    return process_geo("departement", code)


@routes.get('/epci')
def get_all_epci(request):
    return get_moy_5ans("epci")


@routes.get('/epci/{code}')
def get_epci(request):
    code = request.match_info["code"]
    return process_geo("epci", code)


@routes.get('/commune/{code}')
def get_commune(request):
    code = request.match_info["code"]
    return process_geo("commune", code)


@routes.get('/mutations/{com}/{section}')
def get_mutations(request):
    com = request.match_info["com"]
    section = request.match_info["section"]
    r = requests.get(
        f"{PGREST_ENDPOINT}/dvf?code_commune=eq.{com}&section_prefixe=eq.{section}"
    )
    data = r.json()
    return web.json_response(text=json.dumps({"data": data}, default=str))


@routes.get('/section/{code}')
def get_section(request):
    code = request.match_info["code"]
    return process_geo("section", code)


@routes.get('/departement/{code}/communes')
def get_communes_from_dep(request):
    code = request.match_info["code"]
    return get_moy_5ans("commune", code, True)


@routes.get('/epci/{code}/communes')
def get_commune_from_dep(request):
    code = request.match_info["code"]
    return get_moy_5ans("commune", code)


@routes.get('/commune/{code}/sections')
def get_section_from_commune(request):
    code = request.match_info["code"]
    return get_moy_5ans("section", code)


@routes.get('/dpe-copro/{parcelle_id}')
def get_dpe_copro_from_parcelle_id(request):
    parcelle_id = request.match_info["parcelle_id"]

    r = requests.get(
        f"{PGREST_ENDPOINT}/dpe?parcelle_id=eq.{parcelle_id}"
    )
    dpe_data = r.json()

    r = requests.get(
        f"{PGREST_ENDPOINT}/copro?or=(reference_cadastrale_1.eq.{parcelle_id},"
        f"reference_cadastrale_2.eq.{parcelle_id},reference_cadastrale_3.eq.{parcelle_id})"
    )
    copro_data = r.json()

    return web.json_response(text=json.dumps({"data": {
        "dpe": dpe_data,
        "copro": copro_data,
    }}, default=str))


@routes.get('/distribution/{code}')
def get_repartition_from_code_geo(request):
    code = request.match_info["code"]
    if code:
        r = requests.get(
            f"{PGREST_ENDPOINT}/distribution_prix?code_geo=eq.{code}"
        )
        data = r.json()
        for d in data:
            for key in d:
                if (
                    d[key] is not None and
                    d[key].startswith('[') and
                    isinstance(literal_eval(d[key]), list)
                ):
                    d[key] = literal_eval(d[key])
        res = {'code_geo': data['data'][0]['code_geo']}
        return web.json_response(text=json.dumps(res, default=str))


@routes.get('/geo')
@routes.get('/geo/{echelle_geo}')
@routes.get('/geo/{echelle_geo}/{code_geo}/')
@routes.get('/geo/{echelle_geo}/{code_geo}/from={dateminimum}&to={datemaximum}')
def get_echelle(request):
    echelle_geo = request.match_info["echelle_geo"]
    code_geo = request.match_info["code_geo"]
    dateminimum = request.match_info["dateminimum"]
    datemaximum = request.match_info["datemaximum"]
    if echelle_geo is None:
        echelle_query = ''
    else:
        echelle_query = f"echelle_geo=eq.{escape(echelle_geo)}"

    if code_geo is None:
        code_query = ''
    else:
        code_query = f"code_geo=eq.{escape(code_geo)}"

    if dateminimum is None or datemaximum is None:
        date_query = ''
    else:
        date_query = f"annee_mois=gte.{escape(dateminimum)}&annee_mois=lte.{escape(datemaximum)}"

    queries = [echelle_query, code_query, date_query]
    queries = [q for q in queries if q != '']
    
    if len(queries) == 0:
        r = requests.get(
            f"{PGREST_ENDPOINT}/stats_dvf?limit=1"
        )
        data = r.json()
    else:
        r = requests.get(
            f"{PGREST_ENDPOINT}/stats_dvf?" + "&".join(queries)
        )
        data = r.json()

    return web.json_response(text=json.dumps({"data": data}, default=str))


async def app_factory():

    async def on_startup(app):
        app["csession"] = ClientSession()

    async def on_cleanup(app):
        await app["csession"].close()

    app = web.Application()
    app.add_routes(routes)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True,
                    expose_headers="*",
                    allow_headers="*"
                )
        }
    )
    for route in list(app.router.routes()):
        cors.add(route)
    return app


def run():
    web.run_app(app_factory(), path="0.0.0.0", port="3030")


if __name__ == "__main__":
    run()

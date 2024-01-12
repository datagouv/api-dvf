from aiohttp import web, ClientSession
from markupsafe import escape
import json
from ast import literal_eval
from datetime import date
import os

PGREST_ENDPOINT = f"http://{os.getenv('PGREST_ENDPOINT')}"


start_year = date.today().year - 1
start_month = '01' if date.today().month <= 6 else '06'
# start_year = 2022
# start_month = '01'
start_date = str(start_year) + "-" + start_month

threshold = 3


def lighten_response(data):
    mapping_properties = {
        "code_geo": "c",
        "libelle_geo": "n",
        "code_parent": "p",
        "echelle_geo": "l",
        "nb_ventes_whole_appartement": "a",
        "med_prix_m2_whole_appartement": "m_a",
        "nb_ventes_whole_maison": "m",
        "med_prix_m2_whole_maison": "m_m",
        "nb_ventes_whole_apt_maison": "am",
        "med_prix_m2_whole_apt_maison": "m_am",
        "nb_ventes_whole_local": "l",
        "med_prix_m2_whole_local": "m_l",
        "nb_ventes_maison": "m",
        "med_prix_m2_maison": "m_m",
        "nb_ventes_appartement": "a",
        "med_prix_m2_appartement": "m_a",
        "nb_ventes_local": "l",
        "med_prix_m2_local": "m_l",
        "nb_ventes_apt_maison": "am",
        "med_prix_m2_apt_maison": "m_am",
        "annee_mois": "d",
    }
    arr = []
    for item in data:
        newItem = {}
        for prop in item:
            if("moy_" not in prop):
                newItem[mapping_properties[prop]] = item[prop]
        arr.append(newItem)
    return arr


async def get_med_5ans(session, echelle_geo, code=None, case_dep_commune=False):
    url = f"{PGREST_ENDPOINT}/stats_whole_period?echelle_geo=eq.{echelle_geo}"
    if code and not case_dep_commune:
        url += f"&code_parent=eq.{code}"
    if code and case_dep_commune:
        url += f"&code_geo=like.{code}*"

    async with session.get(url) as res:
        data = await res.json()
        data = lighten_response(data)
        return web.json_response(text=json.dumps({"data": data}, default=str))


async def process_geo(session, echelle_geo, code):
    async with session.get(
        f"{PGREST_ENDPOINT}/stats_dvf?echelle_geo=eq.{echelle_geo}"
        f"&code_geo=eq.{code}&order=annee_mois"
    ) as res:
        data = await res.json()
        data = lighten_response(data)
        return web.json_response(text=json.dumps({"data": data}, default=str))


def process_total(raw_total: str) -> int:
    # The raw total looks like this: '0-49/21777'
    _, str_total = raw_total.split("/")
    return int(str_total)


async def get_resource_data_streamed(
    session: ClientSession,
    url: str,
    accept_format: str = "text/csv",
):
    res = await session.head(f"{url}&limit=1&", headers={"Prefer": "count=exact"})
    total = process_total(res.headers.get("Content-Range"))
    for i in range(0, total, 50000):
        async with session.get(
            url=f"{url}&limit=50000&offset={i}", headers={"Accept": accept_format}
        ) as res:
            async for chunk in res.content.iter_chunked(1024):
                yield chunk
            yield b'\n'


routes = web.RouteTableDef()


@routes.get("/")
async def get_health(request):
    return web.HTTPOk()


@routes.get('/nation/mois')
async def get_nation(request):
    async with request.app["csession"].get(
        f"{PGREST_ENDPOINT}/stats_dvf?echelle_geo=eq.nation"
        f"&order=annee_mois"
    ) as res:
        data = await res.json()
        data = lighten_response(data)
        return web.json_response(text=json.dumps({"data": data}, default=str))


@routes.get('/nation')
async def get_all_nation(request):
    return await get_med_5ans(request.app["csession"], "nation")


@routes.get('/departement')
async def get_all_departement(request):
    return await get_med_5ans(request.app["csession"], "departement")


@routes.get('/departement/{code}')
async def get_departement(request):
    code = request.match_info["code"]
    return await process_geo(request.app["csession"], "departement", code)


@routes.get('/epci')
async def get_all_epci(request):
    return await get_med_5ans(request.app["csession"], "epci")


@routes.get('/epci/{code}')
async def get_epci(request):
    code = request.match_info["code"]
    return await process_geo(request.app["csession"], "epci", code)


@routes.get('/commune/{code}')
async def get_commune(request):
    code = request.match_info["code"]
    return await process_geo(request.app["csession"], "commune", code)


@routes.get('/mutations/{com}/{section}')
async def get_mutations(request):
    com = request.match_info["com"]
    section = request.match_info["section"]

    async with request.app["csession"].get(
        f"{PGREST_ENDPOINT}/dvf?code_commune=eq.{com}&section_prefixe=eq.{section}&order=date_mutation.desc"
    ) as res:
        data = await res.json()
        return web.json_response(text=json.dumps({"data": data}, default=str))


@routes.get('/dvf')
async def get_mutations_table(request):
    params = request.rel_url.query
    offset = 0
    query = None
    if "page" in params:
        offset = (int(params["page"]) - 1) * 20
    if "dep" in params:
        query = f"{PGREST_ENDPOINT}/dvf?code_departement=eq.{params['dep']}&limit=20&offset={offset}"
    if "com" in params:
        query = f"{PGREST_ENDPOINT}/dvf?code_commune=eq.{params['com']}&limit=20&offset={offset}"
    if "section" in params:
        query = f"{PGREST_ENDPOINT}/dvf?id_parcelle=like.{params['section']}*&limit=20&offset={offset}"
    if "parcelle" in params:
        query = f"{PGREST_ENDPOINT}/dvf?id_parcelle=eq.{params['parcelle']}&limit=20&offset={offset}"
    if query:
        async with request.app["csession"].get(query) as res:
            data = await res.json()
            return web.json_response(text=json.dumps({"data": data}, default=str))


@routes.get("/dvf/csv/", name="csv")
async def resource_data_csv(request):
    params = request.rel_url.query
    query = None
    if "dep" in params:
        query = f"{PGREST_ENDPOINT}/dvf?code_departement=eq.{params['dep']}"
    if "com" in params:
        query = f"{PGREST_ENDPOINT}/dvf?code_commune=eq.{params['com']}"
    if "section" in params:
        query = f"{PGREST_ENDPOINT}/dvf?id_parcelle=like.{params['section']}*"
    if "parcelle" in params:
        query = f"{PGREST_ENDPOINT}/dvf?id_parcelle=eq.{params['parcelle']}"
    if query:
        response_headers = {
            "Content-Disposition": f'attachment; filename="dvf.csv"',
            "Content-Type": "text/csv",
        }
        response = web.StreamResponse(headers=response_headers)
        await response.prepare(request)

        async for chunk in get_resource_data_streamed(
            request.app["csession"], query
        ):
            await response.write(chunk)

        await response.write_eof()
        return response


@routes.get('/section/{code}')
async def get_section(request):
    code = request.match_info["code"]
    return await process_geo(request.app["csession"], "section", code)


@routes.get('/departement/{code}/communes')
async def get_communes_from_dep(request):
    code = request.match_info["code"]
    return await get_med_5ans(request.app["csession"], "commune", code, True)


@routes.get('/epci/{code}/communes')
async def get_commune_from_dep(request):
    code = request.match_info["code"]
    return await get_med_5ans(request.app["csession"], "commune", code)


@routes.get('/commune/{code}/sections')
async def get_section_from_commune(request):
    code = request.match_info["code"]
    return await get_med_5ans(request.app["csession"], "section", code)


@routes.get('/dpe-copro/{parcelle_id}')
async def get_dpe_copro_from_parcelle_id(request):
    parcelle_id = request.match_info["parcelle_id"]
    async with request.app["csession"].get(f"{PGREST_ENDPOINT}/dpe?parcelle_id=eq.{parcelle_id}") as res:     
        dpe_data = await res.json()

        async with request.app["csession"].get(
            f"{PGREST_ENDPOINT}/copro?or=(reference_cadastrale_1.eq.{parcelle_id},"
            f"reference_cadastrale_2.eq.{parcelle_id},reference_cadastrale_3.eq.{parcelle_id})"
        ) as res2:
            copro_data = await res2.json()

            return web.json_response(text=json.dumps({"data": {
                "dpe": dpe_data,
                "copro": copro_data,
            }}, default=str))


@routes.get('/distribution/{code}')
async def get_repartition_from_code_geo(request):
    code = request.match_info["code"]
    if code:
        async with request.app["csession"].get(f"{PGREST_ENDPOINT}/distribution_prix?code_geo=eq.{code}") as res:    
            data = await res.json()
            for d in data:
                for key in d:
                    if (
                        d[key] is not None and
                        d[key].startswith('[') and
                        isinstance(literal_eval(d[key]), list)
                    ):
                        d[key] = literal_eval(d[key])
            res2 = {'code_geo': data[0]['code_geo']}
            for d in data:
                res2[d['type_local']] = {'xaxis': d['xaxis'], 'yaxis': d['yaxis']}
            return web.json_response(text=json.dumps(res2, default=str))


@routes.get('/geo')
@routes.get('/geo/{echelle_geo}')
@routes.get('/geo/{echelle_geo}/{code_geo}/')
@routes.get('/geo/{echelle_geo}/{code_geo}/from={dateminimum}&to={datemaximum}')
async def get_echelle(request):
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
        async with request.app["csession"].get(f"{PGREST_ENDPOINT}/stats_dvf?limit=1") as res: 
            data = await res.json()
            return web.json_response(text=json.dumps({"data": data}, default=str))
    else:
        async with request.app["csession"].get(f"{PGREST_ENDPOINT}/stats_dvf?" + "&".join(queries)) as res: 
            data = await res.json()
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

    return app


def run():
    web.run_app(app_factory(), path="0.0.0.0", port="3030")


if __name__ == "__main__":
    run()

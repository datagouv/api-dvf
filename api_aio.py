from aiohttp import web, ClientSession
from markupsafe import escape
import psycopg2
import config
import json
import aiohttp_cors
from ast import literal_eval
from datetime import date


user = config.PG_USER
pwd = config.PG_PWD
host = config.PG_HOST
db = config.PG_DB
port = config.PG_PORT
schema = config.PG_SCHEMA

start_year = date.today().year - 1
start_month = '01' if date.today().month <= 6 else '06'
# start_year = 2022
# start_month = '01'
start_date = str(start_year) + "-" + start_month

threshold = 3

conn = psycopg2.connect(
    host=host,
    database=db,
    user=user,
    password=pwd,
    port=port,
    options=f"-c search_path={schema}",
)


def get_moy_5ans(echelle_geo, code=None, case_dep_commune=False):
    where_complement = ""
    if code:
        if not case_dep_commune:
            where_complement = f" AND code_parent = '{code}'"
        else:
            where_complement = f" AND LEFT(code_geo, 2) = '{code}'"
    with conn as connection:
        sql = f"""
            SELECT
                code_geo,
                code_parent,
                libelle_geo,
                COALESCE(SUM(nb_ventes_maison) + SUM(nb_ventes_appartement), 0) AS nb_mutations_appart_maison_5ans,
                COALESCE(SUM(nb_ventes_maison), 0) AS nb_mutations_maison_5ans,
                COALESCE(SUM(nb_ventes_appartement), 0) AS nb_mutations_appart_5ans,
                COALESCE(SUM(nb_ventes_local), 0) AS nb_mutations_local_5ans,
                CASE
                    WHEN (SUM(nb_ventes_maison) + SUM(nb_ventes_appartement)) < 3 THEN NULL
                    ELSE (SUM(nb_ventes_maison * moy_prix_m2_maison) + SUM(nb_ventes_appartement * moy_prix_m2_appartement)) / (SUM(nb_ventes_maison) + SUM(nb_ventes_appartement))
                END AS moy_prix_m2_appart_maison_5ans, 
                CASE
                    WHEN SUM(nb_ventes_maison) < 3 THEN NULL
                    ELSE SUM(nb_ventes_maison * moy_prix_m2_maison) / SUM(nb_ventes_maison)
                END AS moy_prix_m2_maison_5ans,     
                CASE
                    WHEN SUM(nb_ventes_appartement) < 3 THEN NULL
                    ELSE SUM(nb_ventes_appartement * moy_prix_m2_appartement) / SUM(nb_ventes_appartement)
                END AS moy_prix_m2_appart_5ans,
                CASE
                    WHEN SUM(nb_ventes_local) < 3 THEN NULL
                    ELSE SUM(nb_ventes_local * moy_prix_m2_local) / SUM(nb_ventes_local)
                END AS moy_prix_m2_local_5ans
            FROM stats_dvf 
            WHERE echelle_geo = '{echelle_geo}' {where_complement}
            GROUP BY 
                code_geo,
                code_parent,
                libelle_geo
            ;
        """
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


def create_moy_rolling_year(echelle_geo, code=None, case_dep_commune=False):
    with conn as connection:
        sql = f"""SELECT
            tbl2.code_geo,
            code_parent,
            libelle_geo,
            moy_prix_m2_rolling_year,
            nb_mutations_appart_maison_rolling_year,
            nb_mutations_maison_5ans,
            nb_mutations_appartement_5ans,
            nb_mutations_local_5ans,
            CASE
                WHEN COALESCE(nb_mutations_appartement_5ans, 0) + COALESCE(nb_mutations_maison_5ans, 0) < {threshold} THEN NULL
                ELSE tot_appart_maison / (COALESCE(nb_mutations_appartement_5ans, 0) + COALESCE(nb_mutations_maison_5ans, 0))
            END AS moy_prix_m2_appart_maison_5ans,
            CASE
                WHEN nb_mutations_maison_5ans < {threshold} THEN NULL
                ELSE tot_maison / nb_mutations_maison_5ans
            END AS moy_prix_m2_maison_5ans,
            CASE
                WHEN nb_mutations_appartement_5ans < {threshold} THEN NULL
                ELSE tot_appart / nb_mutations_appartement_5ans
            END AS moy_prix_m2_appart_5ans,
            CASE
                WHEN nb_mutations_local_5ans < {threshold} THEN NULL
                ELSE tot_local / nb_mutations_local_5ans
            END AS moy_prix_m2_local_5ans
        FROM (
            SELECT
                code_geo,
                ROUND(SUM(tot) / NULLIF(SUM(nb), 0)) as moy_prix_m2_rolling_year,
                SUM(nb) as nb_mutations_appart_maison_rolling_year
            FROM
            (
                SELECT
                    (COALESCE(moy_prix_m2_maison * nb_ventes_maison, 0) + COALESCE(moy_prix_m2_appartement * nb_ventes_appartement, 0)) as tot,
                    COALESCE(nb_ventes_maison, 0) + COALESCE(nb_ventes_appartement, 0) as nb,
                    annee_mois,
                    code_geo
                FROM stats_dvf
                WHERE
                    echelle_geo='{echelle_geo}'
                AND
                    annee_mois > '{start_date}'
        """
        if (echelle_geo in ['departement', 'epci'] and code is not None) or echelle_geo in ['commune', 'section']:
            if not case_dep_commune:
                sql += f"AND code_parent='{code}'"
            else:
                sql += f"AND LEFT(code_geo, 2)='{code}'"
        sql += f"""
            ) temp
            GROUP BY code_geo
        ) tbl1
        RIGHT JOIN (
            SELECT
                code_geo,
                code_parent,
                libelle_geo,
                SUM(COALESCE(nb_ventes_maison, 0)) as nb_mutations_maison_5ans,
                SUM(COALESCE(nb_ventes_appartement, 0)) as nb_mutations_appartement_5ans,
                SUM(COALESCE(nb_ventes_local, 0)) as nb_mutations_local_5ans,
                SUM((COALESCE(moy_prix_m2_maison * nb_ventes_maison, 0) + COALESCE(moy_prix_m2_appartement * nb_ventes_appartement, 0))) as tot_appart_maison,
                SUM(COALESCE(moy_prix_m2_maison * nb_ventes_maison, 0)) as tot_maison,
                SUM(COALESCE(moy_prix_m2_appartement * nb_ventes_appartement, 0)) as tot_appart,
                SUM(COALESCE(moy_prix_m2_local * nb_ventes_local, 0)) as tot_local
            FROM stats_dvf
            WHERE echelle_geo='{echelle_geo}'
        """
        if (echelle_geo in ['departement', 'epci'] and code is not None) or echelle_geo in ['commune', 'section']:
            if not case_dep_commune:
                sql += f"AND code_parent='{code}'"
            else:
                sql += f"AND LEFT(code_geo, 2)='{code}'"
        sql += """
        GROUP BY code_geo, code_parent, libelle_geo
        ) tbl2
        ON tbl1.code_geo = tbl2.code_geo;"""
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


def process_geo(echelle_geo, code):
    with conn as connection:
        sql = f"SELECT * FROM stats_dvf WHERE echelle_geo='{echelle_geo}' AND code_geo = '{code}' ORDER BY annee_mois ASC;"
        with connection.cursor() as cursor:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()

    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


routes = web.RouteTableDef()


@routes.get("/")
async def get_health(request):
    with conn as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            data = cursor.fetchall()
    assert data[0][0] == 1
    return web.HTTPOk()


@routes.get('/nation/mois')
def get_nation(request):
    with conn as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT * FROM stats_dvf WHERE echelle_geo='nation' ORDER BY annee_mois ASC;""")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


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
    with conn as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"""SELECT * FROM public.dvf WHERE code_commune = '{com}' and section_prefixe = '{section}'""")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


@routes.get('/section/{code}')
def get_section(request):
    code = request.match_info["code"]
    return process_geo("section", code)


# @routes.get('/departement/{code}/epci')
# def get_epci_from_dep(request):
#     code = request.match_info["code"]
#     return get_moy_5ans("epci", code)


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


@routes.get('/dpe/{parcelle_id}')
def get_dpe_from_parcelle_id(request):
    parcelle_id = request.match_info["parcelle_id"]
    with conn as connexion:
        with connexion.cursor() as cursor:
            cursor.execute(f"""SELECT * FROM dpe WHERE parcelle_id = '{parcelle_id}'""")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


@routes.get('/distribution/{code}')
def get_repartition_from_code_geo(request):
    code = request.match_info["code"]
    if code:
        with conn as connection:
            sql = f"SELECT * FROM distribution_prix WHERE code_geo='{code}'"
            with connection.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
        data = {
            "data":
            [{k: literal_eval(v) if v is not None and (v.startswith('[') and isinstance(literal_eval(v), list)) else v
             for k, v in zip(columns, d)} for d in data]
        }
        res = {'code_geo': data['data'][0]['code_geo']}
        for d in data['data']:
            res[d['type_local']] = {'xaxis': d['xaxis'], 'yaxis': d['yaxis']}
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
        echelle_query = f"echelle_geo='{escape(echelle_geo)}'"

    if code_geo is None:
        code_query = ''
    else:
        code_query = f"code_geo='{escape(code_geo)}'"

    if dateminimum is None or datemaximum is None:
        date_query = ''
    else:
        date_query = f"annee_mois>='{escape(dateminimum)}' AND annee_mois<='{escape(datemaximum)}'"

    queries = [echelle_query, code_query, date_query]
    queries = [q for q in queries if q != '']

    with conn as connection:
        with connection.cursor() as cursor:
            if len(queries) == 0:
                cursor.execute("""SELECT * FROM stats_dvf""")
            else:
                cursor.execute(f"""SELECT * FROM stats_dvf WHERE """ + ' AND '.join(queries))
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
    return web.json_response(text=json.dumps({"data": [{k: v for k, v in zip(columns, d)} for d in data]}, default=str))


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

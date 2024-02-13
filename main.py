from fastapi import FastAPI, HTTPException
import sqlite3
import uvicorn
import logging

def handle_sql_error(e: Exception, detail_message: str, status_code: int = 500):
    logging.error(f"SQL Error: {str(e)}")

    if isinstance(e, sqlite3.Error):
        return HTTPException(status_code=status_code, detail=f"{detail_message}: {str(e)}")
    else:
        return HTTPException(status_code=status_code, detail=f"{detail_message}: Unexpected error")

logging.basicConfig(level=logging.ERROR)

con = sqlite3.connect("Chinook.db")
cur = con.cursor()

app = FastAPI()

@app.get("/revenu_fiscal/", description="Retourne le revenu fiscal moyen pour une ville et année données")
async def revenu_fiscal_moyen(year: str, city: str = ""):
    try:
        req = f"SELECT revenu_fiscal_moyen, date, ville FROM foyers_fiscaux WHERE date = {year} AND ville = '{city}'"
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0][0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du revenu fiscal moyen.")

@app.get("/transactions_sample/", description="Consulter les 10 dernières transactions dans ma ville")
async def transactions_sample(city: str = ""):
    try:
        req = f"SELECT * FROM transactions_sample ts WHERE ville ='{city}' ORDER BY date_transaction DESC LIMIT 10 "
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération des 10 dernières transactions.")

@app.get("/acquisitions/", description="connaitre le nombre d'acquisitions dans ma ville ")
async def acquisitions(city: str = ""):
    try:
        req = f"SELECT COUNT(id_transaction) FROM transactions_sample ts WHERE ville = '{city}' AND date_transaction LIKE '2022%';"
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du nombre d'acquisitions.")

@app.get("/prix_au_metre_carre/", description="connaitre le prix au m2 moyen pour les maisons vendues l'année 2022 ")
async def prix_au_metre_carre(city: str = ""):
    try:
        req = f"SELECT AVG(prix / surface_habitable) FROM transactions_sample ts WHERE ville = '{city}' AND date_transaction LIKE '2022%';"
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du prix au m2 moyen.")

@app.get("/nombre_acquisitions/", description="connaitre le nombre d'acquisitions de studios dans ma ville")
async def nombre_acquisitions(city: str = ""):
    try:
        req = f"SELECT COUNT(id_transaction) FROM transactions_sample ts WHERE ville = '{city}' AND date_transaction LIKE '2022%';"
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du nombre d'acquisitions de studios.")

@app.get("/count_appartments_rooms/", description="répartition des appartements vendus durant l'année 2022 en fonction du nombre de pièces")
async def count_appartments_rooms(city: str = ""):
    try:
        req = f"SELECT n_pieces, COUNT(id_transaction) FROM transactions_sample ts WHERE type_batiment = 'Appartement' AND ville = '{city}' GROUP BY n_pieces ORDER BY n_pieces LIKE '2022%' ;"
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération de la répartition des appartements par nombre de pièces.")

@app.get("/avg_prix_par_m2_avignon/", description=" le prix au m2 moyen pour les maisons vendues à Avignon l'année 2022 ")
async def avg_prix_par_m2_avignon():
    try:
        req = "SELECT AVG(prix / surface_habitable) FROM transactions_sample ts WHERE type_batiment = 'Maison' AND ville ='Avignon' LIKE '2022%';"
        cur.execute(req)
        result = cur.fetchall()
        if result[0][0] is None:
            raise HTTPException(status_code=404, detail="Aucun résultat trouvé pour la moyenne du prix par mètre carré à Avignon en 2022.")
        return result[0]
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du prix au m2 moyen à Avignon.")

@app.get("/transactions_count_by_department/", description=" consulter le nombre de transactions (tout type confondu) par département")
async def transactions_count_by_department(department: str = ""):
    try:
        req = f"SELECT departement, COUNT(*) AS count FROM transactions_sample WHERE departement = '{department}' GROUP BY departement;"
        cur.execute(req)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail=f"Aucun résultat trouvé pour le nombre de transactions dans le département {department}.")
        return result
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du nombre de transactions par département.")

@app.get("/vente_appart_2k22_foyer_70k/", description=" connaitre le nombre total de vente d'appartements en 2022 dans toutes les villes où le revenu fiscal moyen en 2018 est supérieur à 70k")
async def vente_appart_2k22_foyer_70k(city: str = ""):
    try:
        req = f"""
            SELECT COUNT(*) AS total_sales
            FROM transactions_sample ts
            JOIN foyers_fiscaux ff ON ts.id_ville = UPPER(ff.id_ville)
            WHERE ts.type_batiment = 'Appartement' AND ts.date_transaction LIKE '2022%' AND ts.ville = '{city}'
            AND ff.revenu_fiscal_moyen > 70000;
        """
        cur.execute(req)
        result = cur.fetchall()
        if not result or len(result) == 0:
            raise HTTPException(status_code=404, detail="Veuillez spécifier une valeur valide")
        return result
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du nombre total de ventes d'appartements en 2022 dans les villes avec un revenu fiscal moyen supérieur à 70k.")

@app.get("/top_10_ville_dynamic/", description=" consulter le top 10 des villes les plus dynamiques en termes de transactions immobilières")
async def top_10_ville_dynamic():
    try:
        req = """
            SELECT ville, COUNT(id_transaction) AS n_transaction, SUM(prix) AS tot_prix
            FROM transactions_sample ts
            GROUP BY ville
            ORDER BY tot_prix DESC
            LIMIT 10;
        """
        cur.execute(req)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Aucun résultat trouvé pour le top 10 des villes les plus dynamiques en termes de transactions immobilières.")
        return result
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération du top 10 des villes les plus dynamiques en termes de transactions immobilières.")

@app.get("/top_10_prix_plus_bas_par_appart/", description=" accéder aux 10 villes avec un prix au m2 moyen le plus bas pour les appartements")
async def top_10_prix_plus_bas_par_appart():
    try:
        req = """
            SELECT ville, AVG(ROUND(prix / surface_habitable)) as prix_au_m2
            FROM transactions_sample ts
            WHERE type_batiment = 'Appartement'
            GROUP BY ville
            ORDER BY prix_au_m2
            LIMIT 10;
        """
        cur.execute(req)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Aucun résultat trouvé pour les 10 villes avec un prix au m2 moyen le plus bas pour les appartements.")
        return result
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération des 10 villes avec un prix au m2 moyen le plus bas pour les appartements.")

@app.get("/top_10_prix_plus_haut_par_maison/", description="  je veux accéder aux 10 villes avec un prix au m2 moyen le plus haut pour les maisons")
async def top_10_prix_plus_haut_par_maison():
    try:
        req = """
            SELECT ville, AVG(ROUND(prix / surface_habitable)) as prix_au_m2
            FROM transactions_sample ts
            WHERE type_batiment = 'Maison'
            GROUP BY ville
            ORDER BY prix_au_m2 DESC
            LIMIT 10;
        """
        cur.execute(req)
        result = cur.fetchall()
        if not result:
            raise HTTPException(status_code=404, detail="Aucun résultat trouvé pour les 10 villes avec un prix au m2 moyen le plus haut pour les maisons.")
        return result
    except Exception as e:
        return handle_sql_error(e, "Erreur lors de la récupération des 10 villes avec un prix au m2 moyen le plus haut pour les maisons.")

if __name__ == "__main__":
    uvicorn.run(app)

import re
import pandas as pd

# Patrones para identificar segmentos importantes
PATRONES = [
    r'(ZN|ZONA|ZO|FRANCA|FRANCADE)',  # Zona/Zona Franca
    r'(P|PARQUE|PARK|PRK|PRQ|PQUE|PQE|PQ|PORQUE|PORQ|PERQUE|PA|P\.Q\.|P\.Q)',  # Parque
    r'(IND|INDUSTRIAL|INDUS|I|IN|INDT|INDTRIAL)',  # Industrial
    r'(OF|OFICINA|OFC|OFIC|OFPPAL|OFICPPAL)',  # Oficina
    r'(VRD|VEREDA|VE|VDA|V)',  # Vereda
    r'(URBANIZACION|URB|URBANIZ|UR)',  # Urbanización
    r'(UN|UNIDAD|UND|U)',  # Unidad
    r'(PREDIO|PREVIO)',  # Predio
    r'(POTRERO|PORTOS|PORTO|PORT)',  # Potrero
    r'(PORTAL|PRTL|PORTL)',  # Portal
    r'(BODEGA|BG|BOD|BDG|BGA|B)',  # Bodega
    r'(VIA|VÍA|V|VI)',  # Vía
    r'(AUTOPISTA|AUT|AUTOP|AUTP|AUT\.MEDELLIN)',  # Autopista
    r'(KILOMETRO|KM|KILOMETRAJE)',  # Kilómetro
    r'(GRAN SABANA|GRANSABANA|SABANA|SBANA)',  # Gran Sabana
    r'(TOCANCIPA|TOC|TOXEMENT)',  # Tocancipá
    r'(CANAVITA|CANABITA|CANATIVA)',  # Canavita
    r'(FUNZA|FZA)',  # Funza
    r'(SIBERIA|SIB|SBR)',  # Siberia
    r'(MOSQUERA|MOSQ|MQRA)'  # Mosquera
]

def extraer_informacion_relevante(direccion):
    """
    Extrae la información relevante de una dirección basada en patrones predefinidos.
    """
    direccion = direccion.upper()  # Estandarizar a mayúsculas
    informacion_relevante = []

    # Aplicar cada patrón
    for patron in PATRONES:
        coincidencias = re.findall(patron, direccion)
        for coincidencia in coincidencias:
            # Unir las partes relevantes de la coincidencia
            if isinstance(coincidencia, tuple):
                informacion_relevante.append(" ".join(coincidencia).strip())
            else:
                informacion_relevante.append(coincidencia.strip())

    # Eliminar duplicados manteniendo el orden
    informacion_relevante = list(dict.fromkeys(informacion_relevante))

    return informacion_relevante
    
if __name__ == "__main__":
    df = pd.read_csv("data/output_chunks/direcciones_1.csv")
    
    df["informacion_relevante"] = df["DIRECCIONES"].apply(extraer_informacion_relevante)
    
    df.to_csv("data/output_chunks/direcciones_1_1.csv", index=False)
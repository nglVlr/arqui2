# Reporte ciudadano manual e interactivo (la "entrada manual" del enunciado).
# Pregunta sector, tipo de reporte y mensaje, arma el evento con generadores.py
# y lo envía con envio.py a NGINX, igual que haría la app del vecino.
# Uso: docker compose run --rm simulador python reportar.py

from envio import enviar
from generadores import SECTORES, evento_reporte

TIPOS = ["ACCIDENTE", "INCENDIO", "ALTERCADO"]


def elegir(titulo, opciones):
    """Muestra una lista numerada y devuelve la opción elegida."""
    print(f"\n{titulo}")
    for numero, opcion in enumerate(opciones, start=1):
        print(f"  {numero}. {opcion}")
    while True:
        respuesta = input("Número: ").strip()
        if respuesta.isdigit() and 1 <= int(respuesta) <= len(opciones):
            return int(respuesta) - 1
        print("Opción no válida, intenta de nuevo.")


def main():
    print("=== Reporte ciudadano: San Juan Sacatepéquez ===")
    zonas = list(SECTORES)
    zona = zonas[elegir("¿En qué sector ocurre?", [f"{z} {SECTORES[z]['zoneName']}" for z in zonas])]
    tipo = TIPOS[elegir("¿Qué está pasando?", TIPOS)]
    mensaje = input("\nDescribe lo que ves (máximo 280 caracteres, Enter para omitir): ").strip()

    evento = evento_reporte(zona, tipo, origen="USUARIO")
    evento["data"]["message"] = mensaje or None   # el mensaje es opcional
    respuesta = enviar([evento])
    if respuesta is None:
        print("\nNo se pudo enviar el reporte. Intenta de nuevo más tarde.")
    elif respuesta["aceptados"] == 1:
        print(f"\nReporte enviado. Id: {evento['eventId']} (atendido por {respuesta['ingestorId']})")
    else:
        print(f"\nEl reporte fue rechazado: {respuesta['motivos'][0]}")


if __name__ == "__main__":
    main()

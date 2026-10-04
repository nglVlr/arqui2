# Evalúa los escenarios: compara la alerta esperada de cada uno (escenarios.ESPERADAS)
# con las alertas reales que el correlador guardó en PostgreSQL (por scenarioId).
# Muestra una tabla de aciertos, faltantes y alertas adicionales, y guarda el
# resultado en la tabla evaluation_results.
# Uso: docker compose run --rm simulador evaluar.py [--minutos 15]

import argparse
import os

import psycopg2

from escenarios import ESPERADAS


def main():
    parser = argparse.ArgumentParser(description="Evalúa las alertas de los escenarios")
    parser.add_argument("--minutos", type=int, default=15, help="Solo alertas de los últimos N minutos")
    args = parser.parse_args()

    conexion = psycopg2.connect(host=os.getenv("POSTGRES_HOST", "postgres"), dbname=os.getenv("POSTGRES_DB"),
                                user=os.getenv("APP_DB_USER"), password=os.getenv("APP_DB_PASSWORD"))
    cursor = conexion.cursor()
    cursor.execute(
        """SELECT scenario_id, alert_type, count(*), max(latency_ms)
           FROM alerts
           WHERE scenario_id IS NOT NULL AND created_at > now() - make_interval(mins => %s)
           GROUP BY scenario_id, alert_type""",
        (args.minutos,),
    )
    obtenidas = {}   # escenario -> {tipo: (cantidad, latencia)}
    for escenario, tipo, cantidad, latencia in cursor.fetchall():
        obtenidas.setdefault(escenario, {})[tipo] = (cantidad, latencia)

    aciertos = 0
    print(f"\nEvaluación de escenarios (alertas de los últimos {args.minutos} minutos)\n")
    print(f"{'Escenario':<18} {'Esperada':<25} {'Resultado':<10} {'Latencia':>9}  Adicionales")
    print("-" * 95)
    for escenario, esperada in ESPERADAS.items():
        reales = obtenidas.get(escenario, {})
        if esperada in reales:
            resultado = "ACIERTO"
            aciertos += 1
            latencia = f"{reales[esperada][1]} ms"
        else:
            resultado = "FALTANTE"
            latencia = "-"
        # Otras alertas que generó el mismo escenario (posibles falsos positivos)
        extras = [f"{tipo} x{c}" for tipo, (c, _) in reales.items() if tipo != esperada]
        print(f"{escenario:<18} {esperada:<25} {resultado:<10} {latencia:>9}  {', '.join(extras) or '-'}")
        cursor.execute(
            "INSERT INTO evaluation_results (scenario_id, alerta_esperada, alertas_obtenidas, resultado) "
            "VALUES (%s, %s, %s, %s)",
            (escenario, esperada, ", ".join(f"{t} x{c}" for t, (c, _) in reales.items()) or None, resultado),
        )
    conexion.commit()
    print("-" * 95)
    print(f"Aciertos: {aciertos} de {len(ESPERADAS)}  ({100 * aciertos / len(ESPERADAS):.0f} %)")


if __name__ == "__main__":
    main()

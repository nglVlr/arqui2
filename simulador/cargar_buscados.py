# Carga en Redis la lista de placas reportadas como robadas (set placas_buscadas).
# El correlador consulta ese set en la regla R07 (VEHICULO_REPORTADO).
# Igual que la cámara, solo guarda el HMAC de cada placa, nunca la placa en claro.
# Es el único script del simulador que habla directamente con Redis.
# Uso: python cargar_buscados.py

import os

# Placas sintéticas "robadas". La primera la usa el escenario ESC-BUSCADO-01.
PLACAS_BUSCADAS = ["P777BCD", "M120XYZ", "C455KLM", "P903RST", "P318GHJ",
                   "M666NPQ", "C101DFG", "P250VWX", "P614LMN", "M889TRK"]


def main():
    import redis   # solo se necesita al ejecutar este script, no al importarlo
    from generadores import hash_placa, placa_parcial

    cliente = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379)
    hashes = [hash_placa(placa) for placa in PLACAS_BUSCADAS]
    cliente.sadd("placas_buscadas", *hashes)
    for placa in PLACAS_BUSCADAS:
        print(f"Cargada placa buscada {placa_parcial(placa)}")
    print(f"Total en placas_buscadas: {cliente.scard('placas_buscadas')}")


if __name__ == "__main__":
    main()

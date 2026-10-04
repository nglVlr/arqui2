-- Carga de los 7 sectores de San Juan Sacatepéquez.
-- Debe coincidir con config/sectores.json (que usan los servicios en Python).
-- Grafana une estas filas con eventos y alertas para mostrar nombres y límites.

INSERT INTO zones (zone, zone_name, tipo, latitude, longitude, peso, radio_m, zona_escolar, limite_kmh) VALUES
    ('SJS-01', 'Casco urbano',       'comercial',    14.71840, -90.64340, 0.20,  600, true,  40),
    ('SJS-02', 'Sajcavillá',         'mixto',        14.70645, -90.62770, 0.14,  700, false, 50),
    ('SJS-03', 'Cruz Blanca',        'residencial',  14.73330, -90.66670, 0.20,  800, true,  50),
    ('SJS-04', 'Lo de Mejía',        'corredor',     14.69256, -90.60027, 0.12,  900, false, 60),
    ('SJS-05', 'Ciudad Quetzal',     'urbano denso', 14.70313, -90.58035, 0.20,  800, true,  50),
    ('SJS-06', 'Villas del Quetzal', 'residencial',  14.70831, -90.55144, 0.09,  600, false, 40),
    ('SJS-07', 'Montúfar',           'rural',        14.79956, -90.63965, 0.05, 1500, false, 60);
